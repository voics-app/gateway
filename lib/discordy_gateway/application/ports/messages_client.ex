defmodule DiscordyGateway.Application.Ports.MessagesClient do
  @moduledoc "Internal HTTP API of the `messages` service: the call system message (type 3)."

  @doc "Posts the type 3 message of a call that just started; returns the new message id."
  @callback create_call_message(
              channel_id :: String.t(),
              author_id :: String.t(),
              participants :: [String.t()]
            ) :: {:ok, String.t()} | {:error, term()}

  @doc "Records the participants and, once the call is over, its end (RFC 3339) on the message."
  @callback update_call_message(
              channel_id :: String.t(),
              message_id :: String.t(),
              update :: %{
                optional(:participants) => [String.t()],
                optional(:ended_timestamp) => String.t()
              }
            ) :: :ok | {:error, term()}
end
