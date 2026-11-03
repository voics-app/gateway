import Config

config :discordy_gateway, DiscordyGateway.Interface.Endpoint,
  adapter: Bandit.PhoenixAdapter,
  url: [host: "localhost"],
  render_errors: [formats: [json: DiscordyGateway.Interface.ErrorJSON], layout: false],
  server: true

config :discordy_gateway, :ports,
  token_verifier: DiscordyGateway.Infrastructure.Auth.JwtVerifier,
  api_client: DiscordyGateway.Infrastructure.Http.ApiClient,
  messages_client: DiscordyGateway.Infrastructure.Http.MessagesClient,
  resume_store: DiscordyGateway.Infrastructure.Valkey.ResumeStore,
  presence_store: DiscordyGateway.Infrastructure.Valkey.PresenceStore,
  call_defaults: DiscordyGateway.Infrastructure.Valkey.CallDefaultsStore,
  inbox_store: DiscordyGateway.Infrastructure.Valkey.InboxStore,
  event_publisher: DiscordyGateway.Infrastructure.Kafka.Publisher,
  voice_nodes: DiscordyGateway.Infrastructure.Etcd.VoiceNodeDirectory,
  voice_allocations: DiscordyGateway.Infrastructure.Valkey.VoiceAllocationStore,
  voice_node_control: DiscordyGateway.Infrastructure.Etcd.VoiceNodeControl,
  voice_token_signer: DiscordyGateway.Infrastructure.Auth.Hs256VoiceTokenSigner,
  turn_credentials: DiscordyGateway.Infrastructure.Auth.CoturnCredentials,
  session_directory: DiscordyGateway.Infrastructure.Cluster.PgSessionDirectory,
  server_directory: DiscordyGateway.Infrastructure.Cluster.HordeServerDirectory

config :phoenix, :json_library, Jason

config :logger, :default_handler, formatter: {LoggerJSON.Formatters.Basic, metadata: :all}

import_config "#{config_env()}.exs"
