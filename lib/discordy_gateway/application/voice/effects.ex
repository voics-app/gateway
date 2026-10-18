defmodule DiscordyGateway.Application.Voice.Effects do
  @moduledoc "Delivers the effects produced by `Voice.Room`; state broadcasts are scope-specific."

  alias DiscordyGateway.Application.{Dispatch, Ports}

  @type broadcast :: (map(), String.t() | nil -> :ok)

  @spec run([tuple()], broadcast()) :: :ok
  def run(effects, broadcast), do: Enum.each(effects, &run_one(&1, broadcast))

  defp run_one({:broadcast_state, payload, gate_channel_id}, broadcast),
    do: broadcast.(payload, gate_channel_id)

  defp run_one({:server_update, pid, payload}, _broadcast),
    do: Dispatch.to_sessions([pid], "VOICE_SERVER_UPDATE", payload)

  defp run_one({:rejected, pid, payload}, _broadcast),
    do: Dispatch.to_sessions([pid], "VOICE_STATE_UPDATE", payload)

  defp run_one({:relay_watch, session_ids, payload}, _broadcast) do
    for session_id <- session_ids,
        pid = Ports.session_directory().whereis(session_id),
        is_pid(pid) do
      Dispatch.relay(pid, :watch_together, payload)
    end

    :ok
  end

  defp run_one({:relay_watch_to_pid, pid, payload}, _broadcast),
    do: Dispatch.relay(pid, :watch_together, payload)
end
