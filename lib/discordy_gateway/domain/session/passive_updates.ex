defmodule DiscordyGateway.Domain.Session.PassiveUpdates do
  @moduledoc "Coalesces changes for one passive session before permission revalidation."

  defstruct channels: %{}, read_states: %{}, voice_states: %{}, voice_gates: %{}

  @type t :: %__MODULE__{
          channels: map(),
          read_states: map(),
          voice_states: map(),
          voice_gates: map()
        }

  @doc "Records a visible guild message marker, retaining the greatest message ID."
  @spec message(t(), String.t(), map()) :: t()
  def message(state, guild_id, %{"channel_id" => channel_id, "id" => id})
      when is_binary(guild_id) and is_binary(channel_id) and is_binary(id) do
    key = {guild_id, channel_id}
    previous = Map.get(state.channels, key)

    if previous == nil or snowflake_greater?(id, previous["last_message_id"]) do
      marker = %{"guild_id" => guild_id, "channel_id" => channel_id, "last_message_id" => id}
      %{state | channels: Map.put(state.channels, key, marker)}
    else
      state
    end
  end

  def message(state, _guild_id, _payload), do: state

  @doc "Keeps the newest authoritative read snapshot for each channel."
  @spec read_state(t(), map()) :: t()
  def read_state(state, %{"channel_id" => channel_id, "read_state_version" => version} = payload)
      when is_binary(channel_id) and is_binary(version) do
    previous = Map.get(state.read_states, channel_id)

    if previous == nil or snowflake_greater?(version, previous["read_state_version"]) do
      %{state | read_states: Map.put(state.read_states, channel_id, payload)}
    else
      state
    end
  end

  def read_state(state, _payload), do: state

  @doc "Keeps the latest voice state for each guild, user and voice session."
  @spec voice_state(t(), map(), String.t() | nil) :: t()
  def voice_state(
        state,
        %{"guild_id" => guild, "user_id" => user, "session_id" => session} = payload,
        gate
      )
      when is_binary(guild) and is_binary(user) and is_binary(session) do
    key = {guild, user, session}

    %{
      state
      | voice_states: Map.put(state.voice_states, key, Map.delete(payload, "member")),
        voice_gates: Map.put(state.voice_gates, key, gate || payload["channel_id"])
    }
  end

  def voice_state(state, _payload, _gate), do: state

  @doc "Rechecks every guild-scoped channel against current permissions."
  @spec filter_visible(t(), (String.t(), [String.t()] -> [String.t()])) :: t()
  def filter_visible(state, visible) do
    gates =
      Enum.map(state.channels, fn {{guild, channel}, _} -> {guild, channel} end) ++
        Enum.map(state.voice_gates, fn {{guild, _user, _session}, channel} -> {guild, channel} end)

    allowed =
      gates
      |> Enum.filter(fn {_guild, channel} -> is_binary(channel) end)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.flat_map(fn {guild, channels} ->
        Enum.map(visible.(guild, Enum.uniq(channels)), &{guild, &1})
      end)
      |> MapSet.new()

    channels =
      Map.filter(state.channels, fn {{guild, channel}, _} -> {guild, channel} in allowed end)

    voice =
      Map.filter(state.voice_states, fn {{guild, _, _} = key, _} ->
        {guild, Map.get(state.voice_gates, key)} in allowed
      end)

    %{state | channels: channels, voice_states: voice}
  end

  @doc "Restricts read-state updates to known private channels or currently visible guild channels."
  @spec filter_read_states(t(), map(), MapSet.t(), (String.t(), [String.t()] -> [String.t()])) ::
          t()
  def filter_read_states(state, guilds, private_channels, visible) do
    grouped =
      state.read_states
      |> Map.keys()
      |> Enum.filter(&Map.has_key?(guilds, &1))
      |> Enum.group_by(&Map.fetch!(guilds, &1))

    allowed =
      grouped
      |> Enum.flat_map(fn {guild, channels} -> visible.(guild, channels) end)
      |> MapSet.new()
      |> MapSet.union(private_channels)

    %{state | read_states: Map.filter(state.read_states, fn {id, _} -> id in allowed end)}
  end

  @doc "Produces a compact payload; an empty map means no changes."
  @spec payload(t()) :: map()
  def payload(state) do
    %{}
    |> put_values("channels", state.channels)
    |> put_values("read_states", state.read_states)
    |> put_values("voice_states", state.voice_states)
  end

  defp put_values(payload, _key, values) when map_size(values) == 0, do: payload
  defp put_values(payload, key, values), do: Map.put(payload, key, Map.values(values))

  defp snowflake_greater?(candidate, previous) do
    with {candidate, ""} <- Integer.parse(candidate),
         {previous, ""} <- Integer.parse(previous) do
      candidate > previous
    else
      _ -> false
    end
  end
end
