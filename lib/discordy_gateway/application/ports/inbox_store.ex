defmodule DiscordyGateway.Application.Ports.InboxStore do
  @moduledoc "Per-device E2EE ciphertext inbox (P7 `e2ee:inbox:<device_id>`, `e2ee:ttl`)."

  @callback store(
              device_id :: String.t(),
              message_id :: String.t(),
              json :: binary(),
              expires_at_ms :: integer()
            ) :: :ok | {:error, term()}
  @callback list(device_id :: String.t()) :: {:ok, [{String.t(), binary()}]} | {:error, term()}
  @callback ack(device_id :: String.t(), message_ids :: [String.t()]) :: :ok | {:error, term()}
  @callback sweep_expired(now_ms :: integer(), limit :: pos_integer()) ::
              {:ok, non_neg_integer()} | {:error, term()}
end
