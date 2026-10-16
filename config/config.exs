import Config

config :discordy_gateway, DiscordyGateway.Interface.Endpoint,
  adapter: Bandit.PhoenixAdapter,
  url: [host: "localhost"],
  render_errors: [formats: [json: DiscordyGateway.Interface.ErrorJSON], layout: false],
  pubsub_server: DiscordyGateway.PubSub,
  server: true

config :phoenix, :json_library, Jason

config :logger, :default_formatter, format: "$time $metadata[$level] $message\n"

import_config "#{config_env()}.exs"
