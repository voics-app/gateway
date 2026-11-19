defmodule DiscordyGateway.Application.Relay.E2eeRelay do
  @moduledoc """
  E2EE ciphertext relay (op 20/21). The ciphertext is stored once per recipient device in the
  ephemeral inbox, pushed live to the devices that are online, and deleted when the device
  acknowledges it. Nothing is persisted outside the inbox TTL (P7).
  """

  alias DiscordyGateway.Application.{Dispatch, Ports}
  alias DiscordyGateway.Application.Session.Identity
  alias DiscordyGateway.Application.Relay.SendAccess
  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Relay.E2ee

  require Logger

  @doc """
  Relays an envelope sent by `session_pid`. Rejections are reported back to the sender as an
  `E2EE_ENVELOPE_REJECTED` dispatch.
  """
  @spec relay(Identity.t(), pid(), map()) :: :ok
  def relay(%Identity{} = identity, session_pid, envelope) do
    case authorize(identity, envelope) do
      :ok -> deliver(envelope)
      {:error, reason} -> reject(session_pid, envelope, reason)
    end
  end

  @doc "Deletes acknowledged entries from the device's inbox."
  @spec ack(Identity.t(), [String.t()]) :: :ok
  def ack(%Identity{device_id: nil}, _message_ids), do: :ok

  def ack(%Identity{device_id: device_id}, message_ids),
    do: log_error(Ports.inbox_store().ack(device_id, message_ids))

  @doc "Frames for the entries still waiting in the device's inbox, oldest first."
  @spec pending(Identity.t()) :: [map()]
  def pending(%Identity{device_id: nil}), do: []

  def pending(%Identity{device_id: device_id}) do
    case Ports.inbox_store().list(device_id) do
      {:ok, entries} -> Enum.flat_map(entries, fn {_message_id, json} -> decode(json) end)
      {:error, _reason} -> []
    end
  end

  defp authorize(identity, envelope) do
    with {:ok, access} <-
           Ports.api_client().fetch_channel_access(envelope.channel_id, identity.user_id),
         :ok <- SendAccess.authorize(access) do
      allowed = MapSet.new([identity.user_id | List.wrap(access["recipient_ids"])])
      sender = %{user_id: identity.user_id, device_id: identity.device_id}
      E2ee.authorize(envelope, sender, access["delivery_mode"], allowed)
    end
  end

  defp deliver(envelope) do
    expires_at = System.os_time(:millisecond) + Contracts.e2ee_ttl_ms()

    for {device_id, user_id, payload} <- E2ee.per_recipient(envelope) do
      log_error(
        Ports.inbox_store().store(
          device_id,
          envelope.message_id,
          Jason.encode!(payload),
          expires_at
        )
      )

      for pid <- Ports.session_directory().sessions_for_device(device_id) do
        send(pid, {:relay_to_user, user_id, :e2ee_envelope, payload})
      end
    end

    :ok
  end

  defp reject(session_pid, envelope, reason) do
    payload = %{
      "channel_id" => envelope.channel_id,
      "message_id" => envelope.message_id,
      "reason" => reason_name(reason)
    }

    Dispatch.to_sessions([session_pid], "E2EE_ENVELOPE_REJECTED", payload)
  end

  defp reason_name(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_name(_reason), do: "unavailable"

  defp decode(json) do
    case Jason.decode(json) do
      {:ok, payload} -> [payload]
      {:error, _reason} -> []
    end
  end

  defp log_error(:ok), do: :ok

  defp log_error({:error, reason}) do
    Logger.warning("e2ee inbox operation failed: #{inspect(reason)}")
    :ok
  end
end
