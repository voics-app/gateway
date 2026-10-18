defmodule DiscordyGateway.Application.Session.VoiceRouting do
  @moduledoc """
  Routes a session's VOICE_STATE_UPDATE (op 4) to the process that owns the voice scope: the
  guild process for guild voice, a per-channel DM call process for DM calls. A session is in at
  most one scope; switching scope first leaves the previous one.
  """

  alias DiscordyGateway.Application.{Dispatch, DmCall, Guild, Ports}
  alias DiscordyGateway.Application.Session.Identity
  alias DiscordyGateway.Domain.Voice.VoiceStates

  @type scope :: {:guild, String.t()} | {:dm_call, String.t()} | nil
  @type session :: %{pid: pid(), id: String.t(), identity: Identity.t()}

  @doc "Applies `request` and returns the session's new voice scope."
  @spec route(session(), scope(), map()) :: scope()
  def route(session, current, %{channel_id: nil, guild_id: guild_id} = request) do
    scope = if is_binary(guild_id), do: {:guild, guild_id}, else: current
    leave(session, scope, request)
    if scope == current, do: nil, else: current
  end

  def route(session, current, request) do
    target = target_scope(request)
    if current != nil and current != target, do: leave(session, current, request)
    join(session, target, request)
    target
  end

  defp target_scope(%{guild_id: guild_id}) when is_binary(guild_id), do: {:guild, guild_id}
  defp target_scope(%{channel_id: channel_id}), do: {:dm_call, channel_id}

  defp leave(_session, nil, _request), do: :ok

  defp leave(session, {:guild, guild_id}, request),
    do: Guild.voice_state_update(guild_id, session.pid, %{request | channel_id: nil})

  defp leave(session, {:dm_call, channel_id}, request),
    do: DmCall.leave(channel_id, member(session), %{request | channel_id: nil})

  defp join(session, {:guild, guild_id}, request),
    do: Guild.voice_state_update(guild_id, session.pid, request)

  defp join(session, {:dm_call, channel_id}, request) do
    Task.Supervisor.start_child(DiscordyGateway.TaskSupervisor, fn ->
      case Ports.api_client().fetch_channel_access(channel_id, session.identity.user_id) do
        {:ok, %{"guild_id" => nil} = access} ->
          DmCall.voice_state_update(channel_id, member(session), request, access)

        _refused ->
          refuse(session, request)
      end
    end)
  end

  defp refuse(session, request) do
    state = %{
      user_id: session.identity.user_id,
      session_id: session.id,
      channel_id: request.channel_id,
      self_mute: request.self_mute,
      self_deaf: request.self_deaf,
      self_video: request.self_video,
      self_stream: request.self_stream
    }

    Dispatch.to_sessions(
      [session.pid],
      "VOICE_STATE_UPDATE",
      VoiceStates.left_payload(state, nil)
    )
  end

  defp member(session),
    do: %{pid: session.pid, user_id: session.identity.user_id, session_id: session.id}
end
