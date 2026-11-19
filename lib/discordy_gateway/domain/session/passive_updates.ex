defmodule DiscordyGateway.Domain.Session.PassiveUpdates do
  @moduledoc "Coalesces changes for one passive session before permission revalidation."

  defstruct channels: %{}, read_states: %{}, voice_states: %{}

  @type t :: %__MODULE__{channels: map(), read_states: map(), voice_states: map()}

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
  @spec voice_state(t(), map()) :: t()
  def voice_state(state, %{"guild_id" => guild, "user_id" => user, "session_id" => session} = payload)
      when is_binary(guild) and is_binary(user) and is_binary(session) do
    key = {guild, user, session}
    %{state | voice_states: Map.put(state.voice_states, key, Map.delete(payload, "member"))}
  end

  def voice_state(state, _payload), do: state

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
