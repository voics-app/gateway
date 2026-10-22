defmodule DiscordyGateway.Integration.EtcdTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias DiscordyGateway.Containers
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

    {_name, port} =
      Containers.start("quay.io/coreos/etcd:v3.6.5", 2379, args, "ready to serve client requests")

    start_supervised!({VoiceNodeDirectory, endpoints: [~c"127.0.0.1:#{port}"]})
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
