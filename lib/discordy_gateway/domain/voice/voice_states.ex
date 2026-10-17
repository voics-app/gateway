defmodule DiscordyGateway.Domain.Voice.VoiceStates do
  @moduledoc "Voice states of one voice server scope (a guild, or a DM call): one per user."

  defstruct states: %{}

  @type state :: %{
          user_id: String.t(),
          session_id: String.t(),
          channel_id: String.t(),
          self_mute: boolean(),
          self_deaf: boolean(),
          self_video: boolean(),
          self_stream: boolean()
        }
  @type t :: %__MODULE__{states: %{String.t() => state()}}
  @type change :: :joined | :moved | :updated | :left | :noop

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Applies a VOICE_STATE_UPDATE request from `session_id` and reports what changed."
  @spec update(t(), String.t(), String.t(), map()) :: {change(), t()}
  def update(%__MODULE__{states: states} = vs, user_id, _session_id, %{channel_id: nil}) do
    if Map.has_key?(states, user_id),
      do: {:left, %{vs | states: Map.delete(states, user_id)}},
      else: {:noop, vs}
  end

  def update(%__MODULE__{states: states} = vs, user_id, session_id, request) do
    state = %{
      user_id: user_id,
      session_id: session_id,
      channel_id: request.channel_id,
      self_mute: request.self_mute,
      self_deaf: request.self_deaf,
      self_video: request.self_video,
      self_stream: request.self_stream
    }

    change =
      case Map.fetch(states, user_id) do
        :error -> :joined
        {:ok, %{session_id: other}} when other != session_id -> :joined
        {:ok, %{channel_id: channel}} when channel != request.channel_id -> :moved
        {:ok, _same_channel} -> :updated
      end

    {change, %{vs | states: Map.put(states, user_id, state)}}
  end

  @doc "Removes every state owned by `session_id` (session ended)."
  @spec remove_session(t(), String.t()) :: {[state()], t()}
  def remove_session(%__MODULE__{states: states} = vs, session_id) do
    {removed, kept} = Enum.split_with(states, fn {_user, s} -> s.session_id == session_id end)
    {Enum.map(removed, &elem(&1, 1)), %{vs | states: Map.new(kept)}}
  end

  @spec get(t(), String.t()) :: state() | nil
  def get(%__MODULE__{states: states}, user_id), do: Map.get(states, user_id)

  @spec all(t()) :: [state()]
  def all(%__MODULE__{states: states}), do: Map.values(states)

  @spec in_channel(t(), String.t()) :: [state()]
  def in_channel(%__MODULE__{states: states}, channel_id),
    do: states |> Map.values() |> Enum.filter(&(&1.channel_id == channel_id))

  @spec user_ids(t()) :: [String.t()]
  def user_ids(%__MODULE__{states: states}), do: Map.keys(states)

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{states: states}), do: map_size(states) == 0

  @doc "Discord VoiceState object."
  @spec to_payload(state(), String.t() | nil) :: map()
  def to_payload(state, guild_id) do
    %{
      "guild_id" => guild_id,
      "channel_id" => state.channel_id,
      "user_id" => state.user_id,
      "session_id" => state.session_id,
      "deaf" => false,
      "mute" => false,
      "self_deaf" => state.self_deaf,
      "self_mute" => state.self_mute,
      "self_video" => state.self_video,
      "self_stream" => state.self_stream,
      "suppress" => false
    }
  end

  @doc "VoiceState announcing that the user left (channel_id null)."
  @spec left_payload(state(), String.t() | nil) :: map()
  def left_payload(state, guild_id), do: state |> to_payload(guild_id) |> Map.put("channel_id", nil)
end
