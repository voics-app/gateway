defmodule DiscordyGateway.Domain.GuildState do
  @moduledoc """
  In-memory cache of one guild (roles, channels, overwrites, members) kept current from
  `discordy.guild.v1` events, plus the permission queries fan-out needs.
  """

  alias DiscordyGateway.Domain.Permissions

  @voice_types [2, 13]
  @thread_types [10, 11, 12]

  defstruct id: nil,
            owner_id: nil,
            voice_region: nil,
            guild: %{},
            roles: %{},
            role_permissions: %{},
            channels: %{},
            overwrites: %{},
            members: %{}

  @type t :: %__MODULE__{}

  @doc "Builds the state from a GuildFull payload, flat (Discord guild object) or nested (`guild` key)."
  @spec from_payload(map()) :: t()
  def from_payload(%{"guild" => guild} = payload) when is_map(guild) do
    guild
    |> Map.merge(Map.take(payload, ["roles", "channels", "members"]))
    |> Map.put("voice_region", Map.get(payload, "voice_region", guild["voice_region"]))
    |> from_payload()
  end

  def from_payload(payload) do
    base = %__MODULE__{
      id: payload["id"],
      owner_id: payload["owner_id"],
      voice_region: payload["voice_region"],
      guild: Map.drop(payload, ["roles", "channels", "members"])
    }

    base = Enum.reduce(List.wrap(payload["roles"]), base, &put_role(&2, &1))
    base = Enum.reduce(List.wrap(payload["channels"]), base, &put_channel(&2, &1))
    Enum.reduce(List.wrap(payload["members"]), base, &put_member(&2, &1))
  end

  @doc "Applies a guild-topic event to the cache. Unrelated events leave it unchanged."
  @spec apply_event(t(), String.t(), map()) :: t()
  def apply_event(state, "GUILD_UPDATE", data) do
    guild = Map.merge(state.guild, Map.drop(data, ["roles", "channels", "members"]))

    %{
      state
      | guild: guild,
        owner_id: guild["owner_id"] || state.owner_id,
        voice_region: Map.get(data, "voice_region", state.voice_region)
    }
  end

  def apply_event(state, type, data) when type in ~w(CHANNEL_CREATE CHANNEL_UPDATE THREAD_CREATE THREAD_UPDATE),
    do: put_channel(state, data)

  def apply_event(state, type, %{"id" => id}) when type in ~w(CHANNEL_DELETE THREAD_DELETE),
    do: %{state | channels: Map.delete(state.channels, id), overwrites: Map.delete(state.overwrites, id)}

  def apply_event(state, type, %{"role" => role}) when type in ~w(GUILD_ROLE_CREATE GUILD_ROLE_UPDATE),
    do: put_role(state, role)

  def apply_event(state, "GUILD_ROLE_DELETE", %{"role_id" => id}),
    do: %{state | roles: Map.delete(state.roles, id), role_permissions: Map.delete(state.role_permissions, id)}

  def apply_event(state, "GUILD_MEMBER_ADD", data), do: put_member(state, Map.delete(data, "guild_id"))

  def apply_event(state, "GUILD_MEMBER_UPDATE", data) do
    user_id = get_in(data, ["user", "id"])
    existing = Map.get(state.members, user_id, %{})
    put_member(state, Map.merge(existing, Map.delete(data, "guild_id")))
  end

  def apply_event(state, "GUILD_MEMBER_REMOVE", data),
    do: %{state | members: Map.delete(state.members, get_in(data, ["user", "id"]))}

  def apply_event(state, _type, _data), do: state

  @doc "Whether `user_id` is a member."
  @spec member?(t(), String.t()) :: boolean()
  def member?(state, user_id), do: Map.has_key?(state.members, user_id)

  @doc "IDs of every member."
  @spec member_ids(t()) :: [String.t()]
  def member_ids(state), do: Map.keys(state.members)

  @doc "Guild-level permissions of a member (0 for non-members)."
  @spec guild_permissions(t(), String.t()) :: non_neg_integer()
  def guild_permissions(state, user_id) do
    case Map.fetch(state.members, user_id) do
      {:ok, member} -> Permissions.compute_base(state.owner_id, state.id, state.role_permissions, member_ref(member))
      :error -> 0
    end
  end

  @doc "Effective permissions of a member in a channel (threads use their parent)."
  @spec channel_permissions(t(), String.t(), String.t(), integer()) :: non_neg_integer()
  def channel_permissions(state, user_id, channel_id, now_ms) do
    with {:ok, member} <- Map.fetch(state.members, user_id),
         {:ok, channel} <- Map.fetch(state.channels, channel_id) do
      Permissions.compute_channel(%{
        owner_id: state.owner_id,
        guild_id: state.id,
        role_permissions: state.role_permissions,
        member: member_ref(member),
        overwrites: Map.get(state.overwrites, permission_source(state, channel), []),
        channel_type: channel["type"] || 0,
        timed_out: timed_out?(member, now_ms)
      })
    else
      :error -> 0
    end
  end

  @doc "Whether the channel is a voice or stage channel."
  @spec voice_channel?(t(), String.t()) :: boolean()
  def voice_channel?(state, channel_id), do: get_in(state.channels, [channel_id, "type"]) in @voice_types

  @doc "Channel by id."
  @spec channel(t(), String.t()) :: map() | nil
  def channel(state, channel_id), do: Map.get(state.channels, channel_id)

  @doc "Voice region: per-channel override, else the guild's region, else `default`."
  @spec voice_region_for(t(), String.t(), String.t()) :: String.t()
  def voice_region_for(state, channel_id, default) do
    channel = Map.get(state.channels, channel_id, %{})
    channel["voice_region"] || channel["rtc_region"] || state.voice_region || default
  end

  @doc "Flat Discord guild object used for READY and GUILD_CREATE."
  @spec snapshot(t()) :: map()
  def snapshot(state) do
    Map.merge(state.guild, %{
      "id" => state.id,
      "owner_id" => state.owner_id,
      "voice_region" => state.voice_region,
      "roles" => Map.values(state.roles),
      "channels" => Map.values(state.channels),
      "members" => Map.values(state.members),
      "member_count" => map_size(state.members)
    })
  end

  @doc "Members whose username, global name or nick starts with `query`; `limit` 0 means no limit."
  @spec search_members(t(), String.t(), non_neg_integer()) :: [map()]
  def search_members(state, query, limit) do
    needle = String.downcase(query)

    state.members
    |> Map.values()
    |> Enum.filter(&matches?(&1, needle))
    |> Enum.sort_by(&get_in(&1, ["user", "id"]))
    |> take_limit(limit)
  end

  @doc "Members for the given ids, plus the ids that are not members."
  @spec members_by_ids(t(), [String.t()]) :: {[map()], [String.t()]}
  def members_by_ids(state, ids) do
    {found, missing} = Enum.split_with(ids, &Map.has_key?(state.members, &1))
    {Enum.map(found, &Map.fetch!(state.members, &1)), missing}
  end

  defp put_role(state, %{"id" => id} = role) do
    %{
      state
      | roles: Map.put(state.roles, id, role),
        role_permissions: Map.put(state.role_permissions, id, Permissions.parse(role["permissions"]))
    }
  end

  defp put_channel(state, %{"id" => id} = channel) do
    overwrites = Enum.map(List.wrap(channel["permission_overwrites"]), &parse_overwrite/1)
    %{state | channels: Map.put(state.channels, id, channel), overwrites: Map.put(state.overwrites, id, overwrites)}
  end

  defp put_member(state, member) do
    case get_in(member, ["user", "id"]) do
      nil -> state
      user_id -> %{state | members: Map.put(state.members, user_id, member)}
    end
  end

  defp parse_overwrite(ow) do
    %{
      id: ow["id"],
      type: if(ow["type"] in [1, "1", "member"], do: 1, else: 0),
      allow: Permissions.parse(ow["allow"]),
      deny: Permissions.parse(ow["deny"])
    }
  end

  defp member_ref(member), do: %{user_id: get_in(member, ["user", "id"]), role_ids: List.wrap(member["roles"])}

  defp permission_source(_state, %{"type" => type, "parent_id" => parent}) when type in @thread_types and is_binary(parent),
    do: parent

  defp permission_source(_state, channel), do: channel["id"]

  defp timed_out?(member, now_ms) do
    with until when is_binary(until) <- member["communication_disabled_until"],
         {:ok, dt, _offset} <- DateTime.from_iso8601(until) do
      DateTime.to_unix(dt, :millisecond) > now_ms
    else
      _ -> false
    end
  end

  defp matches?(_member, ""), do: true

  defp matches?(member, needle) do
    [member["nick"], get_in(member, ["user", "username"]), get_in(member, ["user", "global_name"])]
    |> Enum.any?(&(is_binary(&1) and String.starts_with?(String.downcase(&1), needle)))
  end

  defp take_limit(list, 0), do: list
  defp take_limit(list, limit), do: Enum.take(list, limit)
end
