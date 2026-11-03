defmodule DiscordyGateway.Domain.Fanout do
  @moduledoc """
  Decides which subscribed sessions of a guild receive an event, and in which variant.

  Filters, in order: guild membership, intents, then the permission the event needs (VIEW_CHANNEL
  on the channel for channel-scoped events, plus event-specific gates). Bots lacking
  MESSAGE_CONTENT get a `:stripped` variant of message events unless they authored or are
  mentioned in the message.
  """

  alias DiscordyGateway.Domain.{GuildState, Intents, Permissions, Subscriber}

  @channel_object_events ~w(CHANNEL_CREATE CHANNEL_UPDATE CHANNEL_DELETE THREAD_CREATE THREAD_UPDATE THREAD_DELETE)
  @content_events ~w(MESSAGE_CREATE MESSAGE_UPDATE)

  @type variant :: :full | :stripped

  @spec plan(GuildState.t(), [Subscriber.t()], String.t(), map(), String.t() | nil, integer()) ::
          [{term(), variant()}]
  def plan(state, subscribers, event, data, channel_id, now_ms) do
    gate = gate(event, data, channel_id)

    for %Subscriber{} = sub <- subscribers,
        GuildState.member?(state, sub.user_id),
        Intents.allows?(sub.intents, event, :guild),
        permitted?(gate, state, sub, data, now_ms) do
      {sub.pid, variant(event, sub, data)}
    end
  end

  @doc """
  True for a MESSAGE_CREATE whose only broadcast tag is `@here` (messages sets `mention_here`).
  Such a message pings online members only; `@everyone` messages never carry the flag.
  """
  @spec here_only?(String.t(), map()) :: boolean()
  def here_only?("MESSAGE_CREATE", %{"mention_here" => true}), do: true
  def here_only?(_event, _data), do: false

  @doc """
  The copy of a `mention_here` message for members who must not be pinged by it: the broadcast
  flags are cleared, explicit user mentions stay.
  """
  @spec quiet_here(map()) :: map()
  def quiet_here(data),
    do: data |> Map.put("mention_everyone", false) |> Map.delete("mention_here")

  @doc "Whether the aggregated presence (nil when unknown) counts as online for `@here`."
  @spec here_audience?(map() | nil) :: boolean()
  def here_audience?(nil), do: false
  def here_audience?(%{status: status}), do: status != "offline"

  @doc "Removes message content fields for bots without the MESSAGE_CONTENT intent."
  @spec strip_content(map()) :: map()
  def strip_content(data) do
    data
    |> Map.put("content", "")
    |> Map.put("embeds", [])
    |> Map.put("attachments", [])
    |> Map.put("components", [])
    |> Map.delete("poll")
  end

  defp gate(event, _data, _channel_id) when event in ~w(GUILD_BAN_ADD GUILD_BAN_REMOVE),
    do: {:guild, :ban_members}

  defp gate("AUDIT_LOG_ENTRY_CREATE", _data, _channel_id), do: {:guild, :view_audit_log}

  defp gate("APPLICATION_COMMAND_PERMISSIONS_UPDATE", _data, _channel_id),
    do: :command_permissions

  defp gate(event, data, channel_id) when event in ~w(INVITE_CREATE INVITE_DELETE),
    do: {:channel, channel_id || data["channel_id"], [:view_channel, :manage_channels]}

  defp gate(event, data, _channel_id) when event in @channel_object_events,
    do: {:channel, data["id"], [:view_channel]}

  defp gate(_event, data, channel_id) do
    case channel_id || data["channel_id"] do
      nil -> :guild_wide
      id -> {:channel, id, [:view_channel]}
    end
  end

  defp permitted?(:guild_wide, _state, _sub, _data, _now), do: true

  defp permitted?({:guild, permission}, state, sub, _data, _now),
    do: Permissions.has?(GuildState.guild_permissions(state, sub.user_id), permission)

  defp permitted?(:command_permissions, state, sub, data, _now) do
    (sub.bot and sub.application_id != nil and sub.application_id == data["application_id"]) or
      Permissions.has?(GuildState.guild_permissions(state, sub.user_id), :manage_guild)
  end

  defp permitted?({:channel, channel_id, permissions}, state, sub, _data, now) do
    perms = GuildState.channel_permissions(state, sub.user_id, channel_id, now)
    Enum.all?(permissions, &Permissions.has?(perms, &1))
  end

  defp variant(event, %Subscriber{bot: true} = sub, data) when event in @content_events do
    cond do
      Intents.has?(sub.intents, :message_content) -> :full
      get_in(data, ["author", "id"]) == sub.user_id -> :full
      Enum.any?(List.wrap(data["mentions"]), &(&1["id"] == sub.user_id)) -> :full
      true -> :stripped
    end
  end

  defp variant(_event, _sub, _data), do: :full
end
