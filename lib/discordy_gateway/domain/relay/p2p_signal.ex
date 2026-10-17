defmodule DiscordyGateway.Domain.Relay.P2pSignal do
  @moduledoc "P2P signalling relay rules (op 22). Signals are relayed only, never persisted."

  @spec authorize(map(), %{user_id: String.t(), device_id: String.t()}, String.t(), [String.t()]) ::
          :ok | {:error, :sender_mismatch | :not_p2p | :recipient_not_allowed}
  def authorize(signal, sender, delivery_mode, recipient_ids) do
    cond do
      signal.from_user_id != sender.user_id or signal.from_device_id != sender.device_id -> {:error, :sender_mismatch}
      delivery_mode != "p2p" -> {:error, :not_p2p}
      signal.to_user_id not in recipient_ids -> {:error, :recipient_not_allowed}
      true -> :ok
    end
  end

  @spec to_payload(map()) :: map()
  def to_payload(signal) do
    %{
      "channel_id" => signal.channel_id,
      "to_user_id" => signal.to_user_id,
      "to_device_id" => signal.to_device_id,
      "from_user_id" => signal.from_user_id,
      "from_device_id" => signal.from_device_id,
      "kind" => signal.kind,
      "payload" => signal.payload,
      "identity_sig_b64" => signal.identity_sig_b64
    }
  end

  @doc "P2P_SIGNAL_FAILED dispatch payload when the target device is offline."
  @spec failure_payload(map()) :: map()
  def failure_payload(signal) do
    %{
      "error" => "peer_offline",
      "channel_id" => signal.channel_id,
      "to_user_id" => signal.to_user_id,
      "to_device_id" => signal.to_device_id,
      "kind" => signal.kind
    }
  end
end
