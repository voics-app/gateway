defmodule DiscordyGateway.Infrastructure.ClusterSafetyTest do
  use ExUnit.Case, async: false

  alias DiscordyGateway.Infrastructure.Kafka.TopicConsumer
  alias DiscordyGateway.Interface.{Health, Socket}

  require Record
  Record.defrecordp(:message, Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl"))

  defmodule DisconnectedCluster do
    def ready?, do: Agent.get(__MODULE__, & &1)
  end

  setup do
    start_supervised!(%{
      id: Agent,
      start: {Agent, :start_link, [fn -> false end, [name: DisconnectedCluster]]}
    })

    previous = Application.get_env(:discordy_gateway, :routing_readiness)

    Application.put_env(:discordy_gateway, :routing_readiness,
      cluster: {DisconnectedCluster, :ready?}
    )

    on_exit(fn ->
      if previous,
        do: Application.put_env(:discordy_gateway, :routing_readiness, previous),
        else: Application.delete_env(:discordy_gateway, :routing_readiness)
    end)
  end

  test "a disconnected replica is unready and refuses new sessions" do
    assert {:error, %{"cluster" => false}} = Health.readiness()
    assert {:stop, :normal, {4015, ""}, %{session: nil}} = Socket.init([])
  end

  test "a disconnected Kafka consumer must not acknowledge an event it cannot route" do
    event =
      Jason.encode!(%{
        "id" => "partition-event",
        "type" => "USER_UPDATE",
        "v" => 1,
        "occurred_at" => "2026-11-18T00:00:00Z",
        "target_user_ids" => ["1"],
        "data" => %{"id" => "1"}
      })

    pending = Task.async(fn -> TopicConsumer.handle_message(message(value: event), :user) end)
    assert Task.yield(pending, 150) == nil
    Agent.update(DisconnectedCluster, fn _ -> true end)
    assert Task.await(pending, 1_000) == {:ok, :commit, :user}
  end
end
