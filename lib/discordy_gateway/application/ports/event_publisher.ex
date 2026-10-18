defmodule DiscordyGateway.Application.Ports.EventPublisher do
  @moduledoc "Publishes P6 envelopes to the event bus."

  @callback publish(topic :: String.t(), key :: String.t(), envelope :: map()) ::
              :ok | {:error, term()}
end
