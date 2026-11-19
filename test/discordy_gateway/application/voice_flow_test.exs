defmodule DiscordyGateway.Application.VoiceFlowTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Events.Ingest
  alias DiscordyGateway.Application.Guild
  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Application.Voice.{Allocator, DrainNode, NodeMonitor}
  alias DiscordyGateway.Fixtures

  defp publish(topic, type, data, fields) do
    envelope =
      Map.merge(
        %{
          "id" => unique_id(),
          "type" => type,
          "v" => 1,
          "data" => data,
          "occurred_at" => "2026-10-18T00:00:00Z"
        },
        fields
      )

    Ingest.handle(topic, Jason.encode!(envelope))
  end

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

  test "allocation excludes replicas that advertise the same socket or media endpoint" do
    Fakes.put(:voice_nodes, [
      voice_node("socket-a", ws_url: "wss://rtc.test", users: 0),
      voice_node("socket-b", ws_url: "wss://rtc.test:443/", users: 0),
      voice_node("media-a", public_ip: "203.0.113.2", udp_port: 50_000),
      voice_node("media-b", public_ip: "203.0.113.2", udp_port: 50_000),
      voice_node("safe", public_ip: "203.0.113.2", udp_port: 50_001, users: 10)
    ])

    assert {:ok, %{node_id: "safe"}} = Allocator.allocate("unique-routes", "nl-home")

    Fakes.put(:voice_nodes, [
      voice_node("one", ws_url: "wss://shared.test"),
      voice_node("two", ws_url: "wss://shared.test")
    ])

    assert {:error, :no_voice_nodes} = Allocator.allocate("ambiguous", "nl-home")
  end

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
    assert {:ok, true} = Guild.user_in_voice?("v1", "12", "2")
    assert {:ok, false} = Guild.user_in_voice?("v1", "13", "2")
    assert {:ok, false} = Guild.user_in_voice?("v1", "12", "3")
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

  # Skips earlier voice states (a join can be broadcast more than once) until one for `channel_id` arrives.
  defp await_in(client, channel_id) do
    case await(client, "VOICE_STATE_UPDATE") do
      %{"channel_id" => ^channel_id} = payload -> payload
      :none -> :none
      _other -> await_in(client, channel_id)
    end
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

  test "new joins recheck health and blacklist for cached allocations without disconnecting callers" do
    setup_guild("health-guild")
    a = voice_node("a", region: "us-east")
    b = voice_node("b", region: "us-east", users: 5)
    Fakes.put(:voice_nodes, [a, b])
    assert {:ok, %{node_id: "a"}} = Allocator.allocate("health-guild", "us-east")

    {:ok, monitor} =
      start_supervised({NodeMonitor, name: :health_node_monitor, interval_ms: 60_000})

    alice = online_in("health-guild", "2")
    join(alice, "health-guild", "12")
    assert %{"token" => "voice-token:2:a:false"} = await(alice, "VOICE_SERVER_UPDATE")

    Fakes.put(:voice_nodes, [Map.put(a, :health, :unhealthy), b])
    :ok = NodeMonitor.check_now(monitor)
    assert :none = await(alice, "VOICE_SERVER_UPDATE", 100)
    assert {:ok, %{node_id: "b"}} = Allocator.allocate("health-guild", "us-east")

    Fakes.put(:voice_nodes, [Map.put(a, :blacklisted, true), Map.put(b, :blacklisted, true)])
    assert {:error, :no_voice_nodes} = Allocator.allocate("health-guild", "us-east")
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

  describe "moderator move and disconnect (api#24)" do
    defp moderate(gid, user, channel) do
      publish(
        :guild,
        "VOICE_MEMBER_MOVE",
        %{"guild_id" => gid, "user_id" => user, "channel_id" => channel},
        %{"guild_id" => gid}
      )
    end

    defp add_voice_channel(gid, id) do
      publish(
        :guild,
        "CHANNEL_CREATE",
        %{"id" => id, "type" => 2, "name" => "second", "guild_id" => gid},
        %{"guild_id" => gid}
      )

      Process.sleep(30)
    end

    test "a move re-homes the member to the target channel with a fresh token" do
      setup_guild("m1")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
      alice = online_in("m1", "2")
      bob = online_in("m1", "3")
      add_voice_channel("m1", "14")
      join(alice, "m1", "12")
      await(alice, "VOICE_SERVER_UPDATE")
      await(bob, "VOICE_STATE_UPDATE")

      moderate("m1", "2", "14")

      assert %{"channel_id" => "14", "user_id" => "2"} = await_in(bob, "14")

      assert %{"channel_id" => "14", "guild_id" => "m1", "token" => "voice-token:2:a:false"} =
               await(alice, "VOICE_SERVER_UPDATE")

      assert await(bob, "VOICE_MEMBER_MOVE", 100) == :none
    end

    test "BAN_MESSAGE_PURGE is a bus-only event and never reaches clients" do
      setup_guild("m3")
      bob = online_in("m3", "3")

      publish(
        :guild,
        "BAN_MESSAGE_PURGE",
        %{
          "guild_id" => "m3",
          "user_id" => "9",
          "channel_ids" => ["1"],
          "since_ms" => 1,
          "delete_message_seconds" => 60
        },
        %{"guild_id" => "m3"}
      )

      assert await(bob, "BAN_MESSAGE_PURGE", 100) == :none
    end

    test "a disconnect announces channel null and releases the node" do
      setup_guild("m2")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
      alice = online_in("m2", "2")
      bob = online_in("m2", "3")
      join(alice, "m2", "12")
      await(alice, "VOICE_SERVER_UPDATE")

      moderate("m2", "2", nil)

      assert %{"channel_id" => nil, "user_id" => "2"} = await_left(alice)
      assert %{"channel_id" => nil, "user_id" => "2"} = await_left(bob)
      Process.sleep(30)
      assert :none = Ports.voice_allocations().get("m2")
    end

    test "moving a member who is not in voice, or to a non-voice channel, does nothing" do
      setup_guild("m3")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
      alice = online_in("m3", "2")
      bob = online_in("m3", "3")

      moderate("m3", "2", "12")
      assert await(alice, "VOICE_SERVER_UPDATE", 100) == :none

      join(alice, "m3", "12")
      await(alice, "VOICE_SERVER_UPDATE")
      await(bob, "VOICE_STATE_UPDATE")

      moderate("m3", "2", "10")
      # A late echo of the join may still arrive; what must not arrive is a move to channel 10.
      refute_receive {:client, "3",
                      %{"t" => "VOICE_STATE_UPDATE", "d" => %{"channel_id" => "10"}}},
                     150
    end
  end

  describe "account e2ee_calls_default (voice#8)" do
    defp ready_with_setting(gid, user, enabled) do
      {:ok,
       %{
         "user" => %{"id" => user},
         "guilds" => [Fixtures.guild_payload(gid)],
         "private_channels" => [],
         "relationships" => [],
         "user_settings" => %{"e2ee_calls_default" => enabled}
       }}
    end

    test "a participant's setting makes the whole call E2EE, re-issuing tokens to those already in" do
      setup_guild("e1")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])

      bob = online_in("e1", "3")
      alice = online_in("e1", "2")
      Fakes.put({:call_default, "2"}, true)

      join(bob, "e1", "12")
      assert %{"token" => "voice-token:3:a:false"} = await(bob, "VOICE_SERVER_UPDATE")

      join(alice, "e1", "12")
      assert %{"token" => "voice-token:2:a:true"} = await(alice, "VOICE_SERVER_UPDATE")
      assert %{"token" => "voice-token:3:a:true"} = await(bob, "VOICE_SERVER_UPDATE")
    end

    test "a joiner without the setting is still encrypted when the call already is" do
      setup_guild("e2")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])

      alice = online_in("e2", "2")
      carol = online_in("e2", "1")
      Fakes.put({:call_default, "2"}, true)

      join(alice, "e2", "12")
      assert %{"token" => "voice-token:2:a:true"} = await(alice, "VOICE_SERVER_UPDATE")

      join(carol, "e2", "12")
      assert %{"token" => "voice-token:1:a:true"} = await(carol, "VOICE_SERVER_UPDATE")
    end

    test "READY with the setting on encrypts the user's own calls" do
      Fakes.put({:ready, "2"}, ready_with_setting("e3", "2", true))
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])

      alice = online_in("e3", "2")
      join(alice, "e3", "12")

      assert %{"token" => "voice-token:2:a:true"} = await(alice, "VOICE_SERVER_UPDATE")
    end

    test "calls stay standard when nobody in them has the setting" do
      setup_guild("e4")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])

      alice = online_in("e4", "2")
      join(alice, "e4", "12")

      assert %{"token" => "voice-token:2:a:false"} = await(alice, "VOICE_SERVER_UPDATE")
    end
  end

  describe "E2EE_SETTINGS_UNAVAILABLE_POLICY when the call settings cannot be read" do
    setup do
      previous = Application.get_env(:discordy_gateway, :settings)
      on_exit(fn -> Application.put_env(:discordy_gateway, :settings, previous) end)
      :ok
    end

    defp set_policy(policy) do
      settings =
        :discordy_gateway
        |> Application.get_env(:settings)
        |> Keyword.put(:e2ee_settings_unavailable_policy, policy)

      Application.put_env(:discordy_gateway, :settings, settings)
    end

    test "open lets the join proceed unencrypted" do
      setup_guild("p1")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
      set_policy(:open)
      Fakes.put(:call_defaults_failures, 99)

      alice = online_in("p1", "2")
      join(alice, "p1", "12")

      assert %{"token" => "voice-token:2:a:false"} = await(alice, "VOICE_SERVER_UPDATE")
    end

    test "closed encrypts the call" do
      setup_guild("p2")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
      set_policy(:closed)
      Fakes.put(:call_defaults_failures, 99)

      alice = online_in("p2", "2")
      join(alice, "p2", "12")

      assert %{"token" => "voice-token:2:a:true"} = await(alice, "VOICE_SERVER_UPDATE")
    end

    test "wait retries and joins once the settings are readable again" do
      setup_guild("p3")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
      set_policy(:wait)
      alice = online_in("p3", "2")
      Fakes.put({:call_default, "2"}, true)
      Fakes.put(:call_defaults_failures, 2)
      join(alice, "p3", "12")

      assert %{"token" => "voice-token:2:a:true"} = await(alice, "VOICE_SERVER_UPDATE")
    end

    test "wait refuses the join with a VOICE_JOIN_FAILED error when the settings stay unreadable" do
      setup_guild("p4")
      Fakes.put(:voice_nodes, [voice_node("a", region: "us-east")])
      set_policy(:wait)
      Fakes.put(:call_defaults_failures, 99)

      alice = online_in("p4", "2")
      join(alice, "p4", "12")

      assert %{"error" => "e2ee_settings_unavailable", "channel_id" => "12"} =
               await(alice, "VOICE_JOIN_FAILED")

      assert %{"channel_id" => nil, "user_id" => "2"} = await_left(alice)
      assert :none = await(alice, "VOICE_SERVER_UPDATE", 100)
      assert length(Fakes.calls(:call_defaults)) == 3
    end
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

    defp dm_voice(client, channel_id) do
      send_frame(client.session, 4, %{
        "guild_id" => nil,
        "channel_id" => channel_id,
        "self_mute" => false,
        "self_deaf" => false
      })
    end

    defp dm_ring_setup(channel) do
      dm_access(channel)
      Fakes.put(:voice_nodes, [voice_node("a")])
      {online("2"), online("3")}
    end

    # Client names are user ids, so a session left over from an earlier test can deliver a call for
    # another channel; only the call of `channel_id` counts.
    defp await_call(client, channel_id, event, timeout \\ 500) do
      case await(client, event, timeout) do
        %{"channel_id" => ^channel_id} = call -> call
        :none -> :none
        _other -> await_call(client, channel_id, event, timeout)
      end
    end

    test "the first join creates the call and rings the other recipient" do
      {alice, bob} = dm_ring_setup("dmr1")
      dm_voice(alice, "dmr1")

      assert %{
               "channel_id" => "dmr1",
               "ringing" => ["3"],
               "voice_states" => [%{"user_id" => "2"}]
             } = await_call(bob, "dmr1", "CALL_CREATE")

      assert %{"channel_id" => "dmr1", "ringing" => ["3"]} =
               await_call(alice, "dmr1", "CALL_CREATE")
    end

    test "the ringing user joining stops the ring with CALL_UPDATE" do
      {alice, bob} = dm_ring_setup("dmr2")
      dm_voice(alice, "dmr2")
      assert %{"ringing" => ["3"]} = await_call(bob, "dmr2", "CALL_CREATE")

      dm_voice(bob, "dmr2")

      assert %{"ringing" => [], "voice_states" => [%{"user_id" => "2"}, %{"user_id" => "3"}]} =
               await_call(alice, "dmr2", "CALL_UPDATE")
    end

    test "declining stops the ring, and the call is deleted when the last participant leaves" do
      {alice, bob} = dm_ring_setup("dmr3")
      dm_voice(alice, "dmr3")
      assert %{"ringing" => ["3"]} = await_call(bob, "dmr3", "CALL_CREATE")

      send_frame(bob.session, 25, %{"channel_id" => "dmr3"})
      assert %{"ringing" => [], "voice_states" => [_]} = await_call(alice, "dmr3", "CALL_UPDATE")

      dm_voice(alice, nil)
      assert %{"channel_id" => "dmr3"} = await_call(bob, "dmr3", "CALL_DELETE")
    end

    test "a participant rings another recipient of the running call" do
      {alice, bob} = dm_ring_setup("dmr7")
      dm_voice(alice, "dmr7")
      assert %{"ringing" => ["3"]} = await_call(bob, "dmr7", "CALL_CREATE")
      send_frame(bob.session, 25, %{"channel_id" => "dmr7"})
      assert %{"ringing" => []} = await_call(alice, "dmr7", "CALL_UPDATE")
      assert %{"ringing" => []} = await_call(bob, "dmr7", "CALL_UPDATE")

      send_frame(alice.session, 26, %{"channel_id" => "dmr7", "user_ids" => ["3", "2", "9"]})

      assert %{"ringing" => ["3"], "voice_states" => [%{"user_id" => "2"}]} =
               await_call(bob, "dmr7", "CALL_UPDATE")
    end

    test "only a participant of the running call can ring, and not users already in it" do
      {alice, bob} = dm_ring_setup("dmr8")
      send_frame(alice.session, 26, %{"channel_id" => "dmr8", "user_ids" => ["3"]})
      assert :none = await_call(bob, "dmr8", "CALL_CREATE")

      dm_voice(alice, "dmr8")
      assert %{"ringing" => ["3"]} = await_call(bob, "dmr8", "CALL_CREATE")
      send_frame(bob.session, 25, %{"channel_id" => "dmr8"})
      assert %{"ringing" => []} = await_call(alice, "dmr8", "CALL_UPDATE")

      send_frame(bob.session, 26, %{"channel_id" => "dmr8", "user_ids" => ["2"]})
      assert :none = await_call(alice, "dmr8", "CALL_UPDATE")
    end

    test "a dropped connection leaves the call after the voice grace, before the resume window" do
      settings = Application.get_env(:discordy_gateway, :settings)
      on_exit(fn -> Application.put_env(:discordy_gateway, :settings, settings) end)

      Application.put_env(
        :discordy_gateway,
        :settings,
        Keyword.merge(settings, resume_window_ms: 60_000, voice_detach_grace_ms: 50)
      )

      {alice, bob} = dm_ring_setup("dmr6")
      dm_voice(alice, "dmr6")
      assert %{"ringing" => ["3"]} = await_call(bob, "dmr6", "CALL_CREATE")

      Process.unlink(alice.transport)
      Process.exit(alice.transport, :kill)
      assert %{"channel_id" => "dmr6"} = await_call(bob, "dmr6", "CALL_DELETE", 1_000)
      assert Process.alive?(alice.session)
    end

    test "a ring stops after the ring timeout" do
      Application.put_env(:discordy_gateway, :ring_timeout_ms, 50)
      on_exit(fn -> Application.delete_env(:discordy_gateway, :ring_timeout_ms) end)

      {alice, bob} = dm_ring_setup("dmr4")
      dm_voice(alice, "dmr4")
      assert %{"ringing" => ["3"]} = await_call(bob, "dmr4", "CALL_CREATE")

      assert %{"ringing" => []} = await_call(bob, "dmr4", "CALL_UPDATE", 1_000)
    end

    test "a recipient connecting during a call gets CALL_CREATE after READY" do
      dm_access("dmr5")
      Fakes.put(:voice_nodes, [voice_node("a")])
      alice = online("2")
      dm_voice(alice, "dmr5")
      assert %{"channel_id" => "dmr5"} = await(alice, "VOICE_SERVER_UPDATE")

      bob = online("3")

      assert %{"channel_id" => "dmr5", "ringing" => ["3"]} =
               await_call(bob, "dmr5", "CALL_CREATE")
    end
  end
end
