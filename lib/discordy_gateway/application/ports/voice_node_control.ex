defmodule DiscordyGateway.Application.Ports.VoiceNodeControl do
  @moduledoc "Signals drain/undrain to the voice node itself (etcd `/discordy/voice/drain/<node_id>`)."

  @callback drain(node_id :: String.t()) :: :ok | {:error, term()}
  @callback undrain(node_id :: String.t()) :: :ok | {:error, term()}
end
