defmodule DiscordyGateway.Application.Ports.VoiceCapabilityControl do
  @moduledoc """
  Node-specific capability control. Implementations must authenticate the node identity and lease
  and return only after the SFU has applied the entire command. This port is not yet wired.
  """

  @callback apply(node_id :: String.t(), registration_lease :: pos_integer(), command :: map()) ::
              {:ok, map()} | {:error, term()}
end
