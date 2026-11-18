defmodule DiscordyGateway.Integration.EtcdTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias DiscordyGateway.Containers
  alias DiscordyGateway.Domain.Voice.VoiceNode
  alias DiscordyGateway.Infrastructure.Etcd.{VoiceNodeControl, VoiceNodeDirectory}

  @prefix "/discordy/voice/nodes/"

  setup_all do
    args = [
      "etcd",
      "--advertise-client-urls",
      "http://0.0.0.0:2379",
      "--listen-client-urls",
      "http://0.0.0.0:2379"
    ]

    endpoint =
      case System.get_env("ETCD_TEST_ENDPOINT") do
        nil ->
          {_name, port} =
            Containers.start(
              "quay.io/coreos/etcd:v3.6.5",
              2379,
              args,
              "ready to serve client requests"
            )

          ~c"127.0.0.1:#{port}"

        value ->
          String.to_charlist(value)
      end

    start_supervised!({VoiceNodeDirectory, endpoints: [endpoint]})
    await(&VoiceNodeDirectory.ready?/0)
    :ok
  end

  defp await(fun, attempts \\ 50) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("condition never became true")
      true -> Process.sleep(100) && await(fun, attempts - 1)
    end
  end

  defp put_node(id, region, extra \\ %{}) do
    value =
      Map.merge(
        %{
          "node_id" => id,
          "region" => region,
          "ws_url" => "ws://#{id}",
          "users" => 3,
          "cpu_pct" => 1.5,
          "draining" => false
        },
        extra
      )

    {:ok, _} =
      :eetcd_kv.put(
        VoiceNodeDirectory.connection(),
        @prefix <> region <> "/" <> id,
        Jason.encode!(value)
      )
  end

  test "follows node registrations, updates and lease expiry" do
    put_node("n1", "nl-home")

    await(fn ->
      match?({:ok, %{region: "nl-home", users: 3}}, VoiceNodeDirectory.get_node("n1"))
    end)

    put_node("n1", "nl-home", %{"users" => 8, "draining" => true})
    await(fn -> match?({:ok, %{users: 8, draining: true}}, VoiceNodeDirectory.get_node("n1")) end)

    {:ok, _} = :eetcd_kv.delete(VoiceNodeDirectory.connection(), @prefix <> "nl-home/n1")
    await(fn -> VoiceNodeDirectory.get_node("n1") == :error end)
  end

  test "ignores malformed entries" do
    {:ok, _} = :eetcd_kv.put(VoiceNodeDirectory.connection(), @prefix <> "x/bad", "not json")
    put_node("n2", "eu-west")
    await(fn -> match?({:ok, _}, VoiceNodeDirectory.get_node("n2")) end)
    assert "n2" in Enum.map(VoiceNodeDirectory.list_nodes(), & &1.node_id)
    refute Enum.any?(VoiceNodeDirectory.list_nodes(), &(&1.node_id == "bad"))
  end

  test "drain control writes and clears the drain key" do
    assert :ok = VoiceNodeControl.drain("n7")

    {:ok, %{kvs: [%{value: "1"}]}} =
      :eetcd_kv.get(VoiceNodeDirectory.connection(), "/discordy/voice/drain/n7")

    assert :ok = VoiceNodeControl.undrain("n7")
    {:ok, %{kvs: []}} = :eetcd_kv.get(VoiceNodeDirectory.connection(), "/discordy/voice/drain/n7")
  end

  test "health watches are lease-fenced, fresh and independent from node membership" do
    conn = VoiceNodeDirectory.connection()
    {:ok, %{ID: registration_lease}} = :eetcd_lease.grant(conn, 30)
    {:ok, %{ID: observer_lease}} = :eetcd_lease.grant(conn, 30)
    key = @prefix <> "health/health-node"
    health_key = "/discordy/voice/health/health-node"
    blacklist_key = "/discordy/voice/blacklist/regions/health"

    on_exit(fn ->
      :eetcd_kv.delete(conn, blacklist_key)
      :eetcd_lease.revoke(conn, registration_lease)
      :eetcd_lease.revoke(conn, observer_lease)
    end)

    put_leased(
      key,
      %{
        node_id: "health-node",
        region: "health",
        ws_url: "ws://health-node",
        probe_required: true
      },
      registration_lease
    )

    await(fn -> match?({:ok, _}, VoiceNodeDirectory.get_node("health-node")) end)
    refute healthy?("health-node")

    health = %{
      node_id: "health-node",
      registration_lease: Integer.to_string(registration_lease),
      prober_id: "test",
      status: "healthy",
      consecutive_failures: 0,
      consecutive_passes: 2,
      checked_at_ms: 0
    }

    put_leased(health_key, %{health | registration_lease: "1"}, observer_lease)
    Process.sleep(100)
    refute healthy?("health-node")
    put_leased(health_key, health, observer_lease)
    await(fn -> healthy?("health-node") end)

    :sys.suspend(VoiceNodeDirectory)

    try do
      Process.sleep(2_050)
      refute healthy?("health-node")
      assert "health-node" in Enum.map(VoiceNodeDirectory.list_nodes(), & &1.node_id)
    after
      :sys.resume(VoiceNodeDirectory)
    end

    await(fn -> healthy?("health-node") end)

    {:ok, _} = :eetcd_kv.put(conn, blacklist_key, "malformed marker still excludes")
    await(fn -> not healthy?("health-node") end)
    {:ok, _} = :eetcd_kv.delete(conn, blacklist_key)
    await(fn -> healthy?("health-node") end)
    {:ok, _} = :eetcd_lease.revoke(conn, observer_lease)
    await(fn -> not healthy?("health-node") end)
    assert {:ok, _node} = VoiceNodeDirectory.get_node("health-node")
  end

  defp put_leased(key, value, lease) do
    ctx =
      VoiceNodeDirectory.connection()
      |> :eetcd_kv.new()
      |> :eetcd_kv.with_key(key)
      |> :eetcd_kv.with_value(Jason.encode!(value))
      |> :eetcd_kv.with_lease(lease)

    {:ok, _} = :eetcd_kv.put(ctx)
  end

  defp healthy?(id) do
    {:ok, node} = VoiceNodeDirectory.get_node(id)
    VoiceNode.healthy?(node)
  end

  test "recovers when the directory restarts while the eetcd connection survives" do
    old = Process.whereis(VoiceNodeDirectory)
    Process.exit(old, :kill)

    await(fn ->
      new = Process.whereis(VoiceNodeDirectory)
      new != nil and new != old and VoiceNodeDirectory.ready?()
    end)

    put_node("n9", "nl-home")
    await(fn -> match?({:ok, _}, VoiceNodeDirectory.get_node("n9")) end)
  end
end
