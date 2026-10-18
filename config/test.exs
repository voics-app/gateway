import Config

config :logger, :default_handler, formatter: {Logger.Formatter, []}
config :logger, level: :warning
config :opentelemetry, traces_exporter: :none

config :discordy_gateway, :infrastructure, false
config :discordy_gateway, DiscordyGateway.Interface.Endpoint, server: false, http: [port: 0]

config :discordy_gateway, :ports,
  token_verifier: DiscordyGateway.Fakes.TokenVerifier,
  api_client: DiscordyGateway.Fakes.ApiClient,
  resume_store: DiscordyGateway.Fakes.ResumeStore,
  presence_store: DiscordyGateway.Fakes.PresenceStore,
  inbox_store: DiscordyGateway.Fakes.InboxStore,
  event_publisher: DiscordyGateway.Fakes.EventPublisher,
  voice_nodes: DiscordyGateway.Fakes.VoiceNodes,
  voice_allocations: DiscordyGateway.Fakes.VoiceAllocations,
  voice_node_control: DiscordyGateway.Fakes.VoiceNodeControl,
  voice_token_signer: DiscordyGateway.Fakes.VoiceTokenSigner,
  turn_credentials: DiscordyGateway.Fakes.TurnCredentials,
  session_directory: DiscordyGateway.Infrastructure.Cluster.PgSessionDirectory,
  server_directory: DiscordyGateway.Infrastructure.Cluster.HordeServerDirectory

config :discordy_gateway, :settings,
  default_voice_region: "nl-home",
  public_gateway_url: "ws://localhost:4000/gateway",
  heartbeat_interval_ms: 200,
  resume_window_ms: 300,
  identify_timeout_ms: 300,
  guild_idle_ms: 200

config :discordy_gateway, :http,
  api_url: "http://api.test",
  internal_token: "test-internal-token",
  messages_url: "http://messages.test"
