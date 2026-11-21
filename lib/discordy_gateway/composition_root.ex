defmodule DiscordyGateway.CompositionRoot do
  @moduledoc """
  Composition root. Validates the environment (P2), binds the runtime settings, and starts the
  supervision tree. Application ports are bound to adapters in `config/config.exs`; tests rebind
  them to fakes and start only the core tree (`:infrastructure` false).
  """

  use Application

  alias DiscordyGateway.Application.Admission
  alias DiscordyGateway.Application.Events.Deduplicator
  alias DiscordyGateway.Application.Relay.InboxSweeper
  alias DiscordyGateway.Application.UsageTicker
  alias DiscordyGateway.Application.Voice.NodeMonitor
  alias DiscordyGateway.Infrastructure.Cluster.HordeServerDirectory
  alias DiscordyGateway.Infrastructure.Cluster.RoutingMembership
  alias DiscordyGateway.Infrastructure.Config
  alias DiscordyGateway.Infrastructure.Etcd.VoiceNodeDirectory
  alias DiscordyGateway.Infrastructure.Http.ApiClient
  alias DiscordyGateway.Infrastructure.Kafka
  alias DiscordyGateway.Infrastructure.Valkey
  alias DiscordyGateway.Interface.{Endpoint, Metrics}

  @impl true
  def start(_type, _args) do
    infrastructure? = Application.get_env(:discordy_gateway, :infrastructure, true)
    config = if infrastructure?, do: bind(Config.load!())

    children =
      core_children() ++ if(infrastructure?, do: infrastructure_children(config), else: [])

    Supervisor.start_link(children ++ [Endpoint],
      strategy: :one_for_one,
      name: DiscordyGateway.Supervisor
    )
  end

  @impl true
  def config_change(changed, _new, removed) do
    Endpoint.config_change(changed, removed)
    :ok
  end

  @doc "Publishes the validated configuration to the application environment."
  @spec bind(Config.t()) :: Config.t()
  def bind(config) do
    put(:settings,
      default_voice_region: config.default_voice_region,
      public_gateway_url: config.public_gateway_url,
      e2ee_settings_unavailable_policy: config.e2ee_settings_unavailable_policy,
      voice_detach_grace_ms: config.voice_detach_grace_ms,
      mobile_background_presence_ms: config.mobile_background_presence_ms,
      admission_memory_fraction: config.admission_memory_fraction,
      memory_limit_bytes: config.memory_limit_bytes,
      transport_backlog_limit: config.transport_backlog_limit
    )

    put(:auth,
      jwt_public_key: config.jwt_public_key,
      voice_token_secret: config.voice_token_secret
    )

    put(:turn, secret: config.turn_secret, urls: config.turn_urls)

    put(:http,
      api_url: config.api_url,
      messages_url: config.messages_url,
      internal_token: config.internal_token
    )

    put(:readiness, valkey: {Valkey.Pool, :ready?})
    put(:readiness_soft, etcd: {VoiceNodeDirectory, :ready?})

    put(
      :routing_readiness,
      if(config.cluster_dns_query, do: [cluster: {RoutingMembership, :ready?}], else: [])
    )

    endpoint = Application.get_env(:discordy_gateway, Endpoint, [])
    http = [ip: {0, 0, 0, 0}, port: config.port]
    Application.put_env(:discordy_gateway, Endpoint, Keyword.put(endpoint, :http, http))
    Logger.configure(level: String.to_existing_atom(config.log_level))
    config
  end

  defp put(key, value), do: Application.put_env(:discordy_gateway, key, value)

  defp core_children do
    [
      %{id: :pg, start: {:pg, :start_link, [DiscordyGateway.Pg]}},
      {Task.Supervisor, name: DiscordyGateway.TaskSupervisor},
      {DynamicSupervisor, name: DiscordyGateway.SessionSupervisor, strategy: :one_for_one},
      Deduplicator,
      Admission
    ] ++ HordeServerDirectory.children() ++ [Metrics.reporter()]
  end

  defp infrastructure_children(config) do
    :ok = ApiClient.init_cache()

    cluster =
      case config.cluster_dns_query do
        nil ->
          []

        query ->
          [
            {Cluster.Supervisor,
             [[gateway: dns_topology(query)], [name: DiscordyGateway.ClusterSupervisor]]},
            {RoutingMembership, query: query}
          ]
      end

    cluster ++
      [
        {Valkey.Pool, url: config.valkey_url},
        {VoiceNodeDirectory, endpoints: config.etcd_endpoints},
        {Kafka.Supervisor, brokers: config.kafka_brokers},
        Metrics.poller(),
        NodeMonitor,
        InboxSweeper,
        UsageTicker,
        DiscordyGateway.Application.Voice.ActivityReporter
      ]
  end

  defp dns_topology(query) do
    [
      strategy: Cluster.Strategy.DNSPoll,
      config: [polling_interval: 5_000, query: query, node_basename: "gateway"]
    ]
  end
end
