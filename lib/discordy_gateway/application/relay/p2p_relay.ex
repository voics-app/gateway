defmodule DiscordyGateway.Application.Relay.P2pRelay do
  @moduledoc """
  P2P signalling relay (op 22). Signals are handed to the target device only while it is online;
  they are never stored. When the peer is offline or the signal is refused, the sender gets a
  `P2P_SIGNAL_FAILED` dispatch carrying `{"error": "peer_offline"}`.
  """

  alias DiscordyGateway.Application.{Dispatch, Ports}
  alias DiscordyGateway.Application.Session.Identity
  alias DiscordyGateway.Domain.Relay.P2pSignal

  @call_timeout_ms 1_000

  @spec relay(Identity.t(), pid(), map()) :: :ok
  def relay(%Identity{} = identity, session_pid, signal) do
    case authorize(identity, signal) do
      :ok -> deliver_or_fail(session_pid, signal)
      {:error, _reason} -> fail(session_pid, signal)
    end
  end

  defp authorize(identity, signal) do
    with {:ok, access} <-
           Ports.api_client().fetch_channel_access(signal.channel_id, identity.user_id) do
      sender = %{user_id: identity.user_id, device_id: identity.device_id}

      P2pSignal.authorize(
        signal,
        sender,
        access["delivery_mode"],
        List.wrap(access["recipient_ids"])
      )
    end
  end

  defp deliver_or_fail(session_pid, signal) do
    payload = P2pSignal.to_payload(signal)

    delivered? =
      signal.to_device_id
      |> Ports.session_directory().sessions_for_device()
      |> Enum.any?(&accepted?(&1, signal.to_user_id, payload))

    if delivered?, do: :ok, else: fail(session_pid, signal)
  end

  defp accepted?(pid, user_id, payload) do
    GenServer.call(pid, {:deliver_p2p, user_id, payload}, @call_timeout_ms) == :ok
  catch
    :exit, _reason -> false
  end

  defp fail(session_pid, signal),
    do:
      Dispatch.to_sessions([session_pid], "P2P_SIGNAL_FAILED", P2pSignal.failure_payload(signal))
end
