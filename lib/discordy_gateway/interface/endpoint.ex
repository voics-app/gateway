defmodule DiscordyGateway.Interface.Endpoint do
  @moduledoc "HTTP endpoint: the gateway WebSocket, health probes, metrics and the internal API."

  use Phoenix.Endpoint, otp_app: :discordy_gateway

  plug Plug.RequestId
  plug Plug.Telemetry, event_prefix: [:discordy_gateway, :endpoint]
  plug DiscordyGateway.Interface.Router
end
