defmodule DiscordyGateway.Domain.Relay.E2ee do
  @moduledoc """
  E2EE ciphertext relay rules (op 20). The gateway never inspects ciphertext: it checks that the
  sender is who it claims to be, that the channel is not in standard delivery, and that every
  recipient belongs to the channel, then splits the envelope into one payload per device.
  """

  @type sender :: %{user_id: String.t(), device_id: String.t()}

  @spec authorize(map(), sender(), String.t(), MapSet.t()) ::
          :ok | {:error, :sender_device_mismatch | :not_ephemeral | :recipient_not_allowed}
  def authorize(envelope, sender, delivery_mode, allowed_user_ids) do
    cond do
      envelope.sender_device_id != sender.device_id ->
        {:error, :sender_device_mismatch}

      delivery_mode not in ["ephemeral", "p2p"] ->
        {:error, :not_ephemeral}

      not Enum.all?(envelope.recipients, &MapSet.member?(allowed_user_ids, &1.user_id)) ->
        {:error, :recipient_not_allowed}

      true ->
        :ok
    end
  end

  @doc "One `{device_id, user_id, payload}` per recipient; each payload carries only that device's entry."
  @spec per_recipient(map()) :: [{String.t(), String.t(), map()}]
  def per_recipient(envelope) do
    base = %{
      "channel_id" => envelope.channel_id,
      "message_id" => envelope.message_id,
      "sender_device_id" => envelope.sender_device_id,
      "mls_epoch" => envelope.mls_epoch,
      "content_type" => envelope.content_type
    }

    for r <- envelope.recipients do
      entry = %{
        "user_id" => r.user_id,
        "device_id" => r.device_id,
        "ciphertext_b64" => r.ciphertext_b64
      }

      {r.device_id, r.user_id, Map.put(base, "recipients", [entry])}
    end
  end
end
