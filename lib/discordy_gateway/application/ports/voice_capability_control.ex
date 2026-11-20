defmodule DiscordyGateway.Application.Ports.VoiceCapabilityControl do
  @moduledoc """
  Node-specific capability control. Implementations must authenticate the node identity and lease
  and return only after the SFU has applied the entire command. This port is not yet wired.
  """

  @callback apply(node :: map(), session_id :: String.t(), update :: map()) ::
              {:ok, map()} | {:error, term()}
end
