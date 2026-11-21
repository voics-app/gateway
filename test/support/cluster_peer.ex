defmodule DiscordyGateway.ClusterPeer do
  @moduledoc "Owns a remote test node's fake data and real HTTP listener."
  use GenServer

  alias DiscordyGateway.Application.Ids
  alias DiscordyGateway.Fakes
  alias DiscordyGateway.Infrastructure.Cluster.RoutingMembership
  alias DiscordyGateway.Interface.Router

  def start(config), do: GenServer.start(__MODULE__, config, name: __MODULE__, timeout: 20_000)
  def port, do: GenServer.call(__MODULE__, :port)

  def start_membership(expected) do
    Supervisor.start_child(
      DiscordyGateway.Supervisor,
      {RoutingMembership, query: expected, resolver: &__MODULE__.expected_names/1}
    )
  end

  def expected_names(names), do: names

  def start_kafka(brokers) do
    Supervisor.start_child(
      DiscordyGateway.Supervisor,
      {DiscordyGateway.Infrastructure.Kafka.Supervisor, brokers: brokers}
    )
  end

  def event_ids(time, count),
    do: Enum.map(1..count, fn _ -> Ids.uuid_v7(time) end)

  @impl true
  def init(config) do
    Application.put_all_env(config)
    Fakes.start()
    {:ok, _apps} = Application.ensure_all_started(:discordy_gateway)

    {:ok, server} =
      Bandit.start_link(plug: Router, port: 0, ip: :loopback)

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, port}
  end

  @impl true
  def handle_call(:port, _from, port), do: {:reply, port, port}
end
