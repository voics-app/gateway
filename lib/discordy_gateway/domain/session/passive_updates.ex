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
      | voice_states:
          Map.put(
            state.voice_states,
            key,
            payload |> Map.delete("member") |> Map.put_new("request_to_speak_timestamp", nil)
          ),
        voice_gates: Map.put(state.voice_gates, key, gate || payload["channel_id"])
    }
  end

  def voice_state(state, _payload, _gate), do: state

  @doc "Lists guild/channel pairs whose current permissions are needed."
  @spec permission_pairs(t(), map()) :: [{String.t(), String.t()}]
  def permission_pairs(state, channel_guilds) do
    channels = Enum.map(state.channels, fn {{guild, channel}, _} -> {guild, channel} end)
    voice = Enum.map(state.voice_gates, fn {{guild, _, _}, channel} -> {guild, channel} end)

    reads =
      for channel <- Map.keys(state.read_states),
          guild = Map.get(channel_guilds, channel),
          is_binary(guild),
          do: {guild, channel}

    Enum.filter(channels ++ voice ++ reads, fn {_guild, channel} -> is_binary(channel) end)
  end

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

    %{
      state
      | channels: channels,
        voice_states: voice,
        voice_gates: Map.take(state.voice_gates, Map.keys(voice))
    }
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

  @doc "Removes values already delivered or superseded in previous intervals."
  @spec changed(t(), t()) :: t()
  def changed(state, delivered) do
    channels =
      Map.filter(state.channels, fn {key, value} ->
        newer_value?(value["last_message_id"], delivered.channels[key], "last_message_id")
      end)

    reads =
      Map.filter(state.read_states, fn {key, value} ->
        newer_value?(
          value["read_state_version"],
          delivered.read_states[key],
          "read_state_version"
        )
      end)

    voice =
      Map.filter(state.voice_states, fn {key, value} ->
        previous = delivered.voice_states[key]
        if is_nil(value["channel_id"]), do: previous != nil, else: previous != value
      end)

    %{state | channels: channels, read_states: reads, voice_states: voice}
  end

  @doc "Remembers live values sent in a delta and discards completed voice sessions."
  @spec merge(t(), t()) :: t()
  def merge(delivered, changes) do
    {left, live} =
      Enum.split_with(changes.voice_states, fn {_key, value} -> is_nil(value["channel_id"]) end)

    left_keys = Enum.map(left, &elem(&1, 0))
    live = Map.new(live)

    %{
      delivered
      | channels: Map.merge(delivered.channels, changes.channels),
        read_states: Map.merge(delivered.read_states, changes.read_states),
        voice_states: delivered.voice_states |> Map.drop(left_keys) |> Map.merge(live),
        voice_gates:
          delivered.voice_gates
          |> Map.drop(left_keys)
          |> Map.merge(Map.take(changes.voice_gates, Map.keys(live)))
    }
  end

  @doc "Forgets buffered and delivered data for a channel and its voice sessions."
  @spec forget_channel(t(), String.t(), String.t()) :: t()
  def forget_channel(state, guild_id, channel_id) do
    voice_keys =
      for {{guild, _, _} = key, gate} <- state.voice_gates,
          guild == guild_id and gate == channel_id,
          do: key

    %{
      state
      | channels: Map.delete(state.channels, {guild_id, channel_id}),
        read_states: Map.delete(state.read_states, channel_id),
        voice_states: Map.drop(state.voice_states, voice_keys),
        voice_gates: Map.drop(state.voice_gates, voice_keys)
    }
  end

  @doc "Forgets all guild data and associated user read states."
  @spec forget_guild(t(), String.t(), [String.t()]) :: t()
  def forget_guild(state, guild_id, channel_ids) do
    voice_keys = for {{guild, _, _} = key, _} <- state.voice_states, guild == guild_id, do: key

    %{
      state
      | channels: Map.reject(state.channels, fn {{guild, _}, _} -> guild == guild_id end),
        read_states: Map.drop(state.read_states, channel_ids),
        voice_states: Map.drop(state.voice_states, voice_keys),
        voice_gates: Map.drop(state.voice_gates, voice_keys)
    }
  end

  defp newer_value?(_candidate, nil, _field), do: true

  defp newer_value?(candidate, previous, field),
    do: snowflake_greater?(candidate, previous[field])

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
