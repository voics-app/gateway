defmodule DiscordyGateway.Application.VoiceFlowTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Application.Voice.{Allocator, DrainNode, NodeMonitor}
  alias DiscordyGateway.Fixtures

  defp setup_guild(gid) do
    payload = Fixtures.guild_payload(gid)

    ready = fn user ->
      {:ok,
       %{
         "user" => %{"id" => user},
         "guilds" => [payload],
         "private_channels" => [],
         "relationships" => []
       }}
    end

    for user <- ~w(1 2 3), do: Fakes.put({:ready, user}, ready.(user))
  end

  defp online_in(gid, user) do
    client = online(user)
    assert %{"id" => ^gid} = await(client, "GUILD_CREATE")
    client
  end

  defp join(client, gid, channel) do
    send_frame(client.session, 4, %{
      "guild_id" => gid,
      "channel_id" => channel,
      "self_mute" => false,
      "self_deaf" => false
    })
  end

  defp leave(client, gid), do: join(client, gid, nil)

  test "joining voice allocates the least-loaded node and sends VOICE_SERVER_UPDATE" do
    setup_guild("v1")

    Fakes.put(:voice_nodes, [
      voice_node("busy", region: "us-east", users: 9),
      voice_node("idle", region: "us-east", users: 1),
      voice_node("elsewhere", region: "nl-home")
    ])

    alice = online_in("v1", "2")
    bob = online_in("v1", "3")
    join(alice, "v1", "12")

    assert %{"channel_id" => "12", "user_id" => "2"} = await(alice, "VOICE_STATE_UPDATE")
    assert %{"user_id" => "2"} = await(bob, "VOICE_STATE_UPDATE")

    assert %{
             "token" => "voice-token:2:idle:false",
             "endpoint" => "ws://idle.test",
             "region" => "us-east",
             "channel_id" => "12",
             "guild_id" => "v1"
           } =
             await(alice, "VOICE_SERVER_UPDATE")

    assert {:ok, "idle"} = Ports.voice_allocations().get("v1")
    assert Ports.voice_allocations().count() == 1
  end

  test "everyone in the guild shares the node, even when load shifts" do
    setup_guild("v2")

    Fakes.put(:voice_nodes, [
      voice_node("a", region: "us-east"),
      voice_node("b", region: "us-east")
    ])

    alice = online_in("v2", "2")
    bob = online_in("v2", "3")

    join(alice, "v2", "12")
    assert %{"token" => "voice-token:2:a:false"} = await(alice, "VOICE_SERVER_UPDATE")

    Fakes.put(:voice_nodes, [
      voice_node("a", region: "us-east", users: 50),
      voice_node("b", region: "us-east")
    ])

    join(bob, "v2", "12")
    assert %{"token" => "voice-token:3:a:false"} = await(bob, "VOICE_SERVER_UPDATE")
  end

  test "leaving the last voice channel releases the allocation and announces channel null" do
    setup_guild("v3")
    Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
    alice = online_in("v3", "2")
    bob = online_in("v3", "3")
    join(alice, "v3", "12")
    await(alice, "VOICE_SERVER_UPDATE")

    leave(alice, "v3")
    assert %{"channel_id" => nil, "user_id" => "2"} = await_left(bob)
    Process.sleep(30)
    assert :none = Ports.voice_allocations().get("v3")
  end

  defp await_left(client) do
    case await(client, "VOICE_STATE_UPDATE") do
      %{"channel_id" => nil} = payload -> payload
      :none -> :none
      _joined -> await_left(client)
    end
  end

  test "a join is refused when no node serves the region" do
    setup_guild("v4")
    alice = online_in("v4", "2")
    join(alice, "v4", "12")
    assert %{"channel_id" => nil} = await(alice, "VOICE_STATE_UPDATE")
    assert :none = await(alice, "VOICE_SERVER_UPDATE", 100)
  end

  test "joining a text channel, or a voice channel without CONNECT, is refused" do
    setup_guild("v5")

    Fakes.put(:voice_nodes, [
      voice_node("a", region: "eu-west"),
      voice_node("b", region: "us-east")
    ])

    alice = online_in("v5", "2")
    join(alice, "v5", "10")
    assert %{"channel_id" => nil} = await(alice, "VOICE_STATE_UPDATE")
  end

  test "draining a node stops new allocations but keeps existing ones" do
    setup_guild("v6")

    Fakes.put(:voice_nodes, [
      voice_node("a", region: "us-east"),
      voice_node("b", region: "us-east", users: 5)
    ])

    assert {:ok, %{node_id: "a"}} = Allocator.allocate("srv-x", "us-east")

    :ok = DrainNode.drain("a")
    assert [{:drain, "a"}] = Fakes.calls(:control)
    assert {:ok, %{node_id: "a"}} = Allocator.allocate("srv-x", "us-east")
    assert {:ok, %{node_id: "b"}} = Allocator.allocate("srv-y", "us-east")

    :ok = DrainNode.undrain("a")
    assert [{:drain, "a"}, {:undrain, "a"}] = Fakes.calls(:control)
    assert {:ok, %{node_id: "a"}} = Allocator.allocate("srv-z", "us-east")
  end

  test "allocation fails when every node is draining" do
    Fakes.put(:voice_nodes, [voice_node("a", draining: true)])
    assert {:error, :no_voice_nodes} = Allocator.allocate("srv-1", "nl-home")
  end

  test "when the node disappears the guild reallocates and re-sends VOICE_SERVER_UPDATE" do
    setup_guild("v7")
    Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])

    {:ok, monitor} =
      start_supervised({NodeMonitor, name: :test_node_monitor, interval_ms: 60_000})

    alice = online_in("v7", "2")
    join(alice, "v7", "12")
    assert %{"token" => "voice-token:2:a:false"} = await(alice, "VOICE_SERVER_UPDATE")

    Fakes.put(:voice_nodes, [voice_node("b", region: "us-east")])
    :ok = NodeMonitor.check_now(monitor)

    assert %{"token" => "voice-token:2:b:false", "endpoint" => "ws://b.test"} =
             await(alice, "VOICE_SERVER_UPDATE")

    assert {:ok, "b"} = Ports.voice_allocations().get("v7")
  end

  test "Watch Together is stamped with the server clock and relayed to the voice channel only" do
    setup_guild("v8")
    Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
    alice = online_in("v8", "2")
    bob = online_in("v8", "3")
    carol = online_in("v8", "1")
    join(alice, "v8", "12")
    join(bob, "v8", "12")
    await(bob, "VOICE_SERVER_UPDATE")

    send_frame(alice.session, 24, %{
      "channel_id" => "12",
      "action" => "load",
      "url" => "https://v.test/x",
      "position_ms" => 0
    })

    assert %{
             "op" => 24,
             "d" => %{
               "action" => "load",
               "url" => "https://v.test/x",
               "user_id" => "2",
               "at_server_ms" => at
             }
           } =
             await_op(bob, 24)

    assert is_integer(at)
    assert :none = await_op(carol, 24, 100)

    late = online_in("v8", "1")
    join(late, "v8", "12")
    assert %{"d" => %{"url" => "https://v.test/x"}} = await_op(late, 24)
  end

  test "ending a session removes it from voice" do
    setup_guild("v9")
    Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
    alice = online_in("v9", "2")
    bob = online_in("v9", "3")
    join(alice, "v9", "12")
    await(alice, "VOICE_SERVER_UPDATE")
    GenServer.stop(alice.session)
    assert %{"channel_id" => nil, "user_id" => "2"} = await_left(bob)
  end

  describe "DM calls" do
    defp dm_access(channel, mode \\ "standard") do
      for user <- ~w(2 3) do
        Fakes.put(
          {:access, channel, user},
          {:ok,
           %{
             "guild_id" => nil,
             "delivery_mode" => mode,
             "recipient_ids" => ["2", "3"],
             "channel_type" => 1
           }}
        )
      end
    end

    test "a DM call shares one node and notifies the other recipient" do
      dm_access("dmv1", "ephemeral")
      Fakes.put(:voice_nodes, [voice_node("a")])
      alice = online("2")
      bob = online("3")

      send_frame(alice.session, 4, %{
        "guild_id" => nil,
        "channel_id" => "dmv1",
        "self_mute" => false,
        "self_deaf" => false
      })

      assert %{"channel_id" => "dmv1", "user_id" => "2"} = await(bob, "VOICE_STATE_UPDATE")

      assert %{"token" => "voice-token:2:a:true", "guild_id" => nil, "channel_id" => "dmv1"} =
               await(alice, "VOICE_SERVER_UPDATE")

      assert {:ok, "a"} = Ports.voice_allocations().get("dmv1")

      send_frame(alice.session, 4, %{
        "guild_id" => nil,
        "channel_id" => nil,
        "self_mute" => false,
        "self_deaf" => false
      })

      assert %{"channel_id" => nil} = await_left(bob)
      Process.sleep(30)
      assert :none = Ports.voice_allocations().get("dmv1")
    end

    test "a non-participant is refused" do
      dm_access("dmv2")
      outsider = online("9")

      send_frame(outsider.session, 4, %{
        "guild_id" => nil,
        "channel_id" => "dmv2",
        "self_mute" => false,
        "self_deaf" => false
      })

      assert %{"channel_id" => nil} = await(outsider, "VOICE_STATE_UPDATE")
    end
  end
end
