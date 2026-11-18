defmodule DiscordyGateway.Domain.Voice.DiscoveryTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Voice.{Discovery, VoiceNode}

  @node_key "/discordy/voice/nodes/test/node-1"
  @health_key "/discordy/voice/health/node-1"

  test "required probes fail closed until a leased healthy observation matches this registration" do
    nodes = records(true)
    assert [node] = Discovery.nodes(nodes, true)
    refute VoiceNode.healthy?(node)

    for health <- [health("99", "healthy"), health("1", "unknown"), health("1", "unhealthy")] do
      assert [node] = Discovery.nodes(Map.put(nodes, @health_key, health), true)
      refute VoiceNode.healthy?(node)
    end

    ready = Map.put(nodes, @health_key, health("1", "healthy"))
    assert [node] = Discovery.nodes(ready, true)
    assert VoiceNode.healthy?(node)
    assert [node] = Discovery.nodes(ready, false)
    refute VoiceNode.healthy?(node)

    persistent = put_in(ready, [@health_key, :lease], 0)
    assert [node] = Discovery.nodes(persistent, true)
    refute VoiceNode.healthy?(node)
  end

  test "legacy nodes tolerate absent health but honor explicit unhealthy results" do
    assert [node] = Discovery.nodes(records(false), true)
    assert VoiceNode.healthy?(node)

    assert [node] =
             Discovery.nodes(Map.put(records(false), @health_key, health("1", "unhealthy")), true)

    refute VoiceNode.healthy?(node)
  end

  test "node and region blacklist presence wins even if the value is malformed" do
    ready = Map.put(records(true), @health_key, health("1", "healthy"))

    for key <- [
          "/discordy/voice/blacklist/nodes/node-1",
          "/discordy/voice/blacklist/regions/test"
        ] do
      assert [node] = Discovery.nodes(Map.put(ready, key, %{value: "broken", lease: 0}), true)
      refute VoiceNode.healthy?(node)
    end

    assert [node] =
             Discovery.nodes(Map.put(ready, "/discordy/voice/blacklist/nodes/other", %{}), true)

    assert VoiceNode.healthy?(node)
  end

  test "malformed enforcement flags and mismatched registration identities are not legacy nodes" do
    raw = Jason.decode!(records(true)[@node_key].value)
    assert {:error, :invalid_node} = VoiceNode.parse(Map.put(raw, "probe_required", "true"))

    mismatch =
      put_in(records(true), [@node_key, :value], Jason.encode!(Map.put(raw, "node_id", "other")))

    assert [] = Discovery.nodes(mismatch, true)
  end

  defp records(required) do
    %{
      @node_key => %{
        lease: 1,
        value:
          Jason.encode!(%{
            node_id: "node-1",
            region: "test",
            ws_url: "ws://node-1",
            probe_required: required
          })
      }
    }
  end

  defp health(lease, status) do
    %{
      lease: 2,
      value:
        Jason.encode!(%{
          node_id: "node-1",
          registration_lease: lease,
          prober_id: "observer",
          status: status,
          consecutive_failures: 0,
          consecutive_passes: 2,
          checked_at_ms: 1
        })
    }
  end
end
