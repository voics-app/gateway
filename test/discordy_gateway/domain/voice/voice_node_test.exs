defmodule DiscordyGateway.Domain.Voice.VoiceNodeTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Voice.VoiceNode

  defp node(id, region, users, cpu, draining \\ false) do
    %VoiceNode{
      node_id: id,
      region: region,
      ws_url: "wss://#{id}",
      users: users,
      cpu_pct: cpu,
      draining: draining
    }
  end

  test "parse/1 reads the etcd value (P8)" do
    raw = %{
      "node_id" => "n1",
      "region" => "nl-home",
      "ws_url" => "wss://discordy-rtc-nl-home.voics.nl",
      "public_ip" => "1.2.3.4",
      "udp_port" => 50_000,
      "users" => 3,
      "cpu_pct" => 12.5,
      "egress_bps" => 1000,
      "draining" => false,
      "version" => "0.1.0"
    }

    assert {:ok, %VoiceNode{node_id: "n1", users: 3, cpu_pct: 12.5, draining: false}} =
             VoiceNode.parse(raw)

    assert VoiceNode.parse(%{"region" => "x"}) == {:error, :invalid_node}
  end

  test "control endpoint is private and bound to a live registration identity" do
    raw = %{"node_id" => "n", "region" => "r", "ws_url" => "wss://public.test", "control_ip" => "10.42.0.8", "control_port" => 4100, "registration_lease" => "123"}
    assert {:ok, %{control_endpoint: %{ip: "10.42.0.8", port: 4100, lease: "123"}}} = VoiceNode.parse(raw)
    assert {:ok, %{control_endpoint: nil}} = VoiceNode.parse(Map.put(raw, "control_ip", "203.0.113.1"))
    assert {:ok, %{control_endpoint: nil}} = VoiceNode.parse(Map.put(raw, "registration_lease", "0"))
    assert {:ok, %{control_endpoint: nil}} = VoiceNode.parse(Map.delete(raw, "registration_lease"))
  end

  describe "pick_least_loaded/3" do
    test "picks the non-draining node with the fewest users in the region" do
      nodes = [
        node("a", "eu", 5, 10),
        node("b", "eu", 2, 90),
        node("c", "us", 0, 0),
        node("d", "eu", 1, 5, true)
      ]

      assert {:ok, %VoiceNode{node_id: "b"}} =
               VoiceNode.pick_least_loaded(nodes, "eu", MapSet.new())
    end

    test "breaks ties by cpu" do
      nodes = [node("a", "eu", 2, 50), node("b", "eu", 2, 10)]

      assert {:ok, %VoiceNode{node_id: "b"}} =
               VoiceNode.pick_least_loaded(nodes, "eu", MapSet.new())
    end

    test "skips nodes drained through the gateway" do
      nodes = [node("a", "eu", 0, 0), node("b", "eu", 9, 9)]

      assert {:ok, %VoiceNode{node_id: "b"}} =
               VoiceNode.pick_least_loaded(nodes, "eu", MapSet.new(["a"]))
    end

    test "reports when no node is available" do
      assert VoiceNode.pick_least_loaded([node("a", "eu", 0, 0, true)], "eu", MapSet.new()) ==
               {:error, :no_voice_nodes}
    end
  end

  test "available?/2 accounts for both drain sources" do
    refute VoiceNode.available?(node("a", "eu", 0, 0, true), MapSet.new())
    refute VoiceNode.available?(node("a", "eu", 0, 0), MapSet.new(["a"]))
    assert VoiceNode.available?(node("a", "eu", 0, 0), MapSet.new())
  end
end
