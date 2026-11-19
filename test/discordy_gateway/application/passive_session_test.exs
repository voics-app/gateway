defmodule DiscordyGateway.Application.PassiveSessionTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.{Events.Ingest, Ports}
  alias DiscordyGateway.Fixtures

  test "passive batches changed markers, then activation sends a fresh guild snapshot" do
    guild = Fixtures.guild_payload("100")

    ready = %{
      "user" => %{"id" => "2"},
      "guilds" => [guild],
      "private_channels" => [],
      "relationships" => [],
      "user_settings" => %{}
    }

    Fakes.put({:guild, "100"}, {:ok, guild})
    Fakes.put({:ready, "2"}, {:ok, ready})
    Fakes.put({:read_states, "2"}, {:ok, %{entries: [], versioned: true}})

    active = client("active")
    passive = client("passive")
    identify(active.session, "2", %{"device_id" => "active"})
    identify(passive.session, "2", %{"device_id" => "passive", "passive" => true})
    assert await(active, "READY")
    assert await(passive, "READY")
    assert await(active, "GUILD_CREATE")
    assert await(passive, "GUILD_CREATE", 100) == :none
    send_frame(passive.session, 8, %{"guild_id" => "100", "query" => "", "limit" => 0})
    assert await(passive, "GUILD_MEMBERS_CHUNK", 100) == :none

    active_bytes =
      Enum.reduce(1..10, 0, fn n, bytes ->
        payload = %{
          "id" => Integer.to_string(1000 + n),
          "channel_id" => "10",
          "content" => String.duplicate("x", 512)
        }

        publish(n, payload)
        assert frame = receive_frame("active", "MESSAGE_CREATE")
        bytes + byte_size(Jason.encode!(frame))
      end)

    assert await(passive, "MESSAGE_CREATE", 100) == :none
    Fakes.put(:voice_nodes, [voice_node("voice-node", region: "us-east")])

    send_frame(active.session, 4, %{
      "guild_id" => "100",
      "channel_id" => "12",
      "self_mute" => false,
      "self_deaf" => false
    })

    assert %{"channel_id" => "12"} = await(active, "VOICE_STATE_UPDATE")

    read = %{
      "channel_id" => "10",
      "last_message_id" => "1000",
      "mention_count" => 1,
      "read_state_version" => "1"
    }

    Ingest.handle(
      :message,
      Jason.encode!(%{
        "id" => "read-positive",
        "type" => "READ_STATE_UPDATE",
        "target_user_ids" => ["2"],
        "data" => read
      })
    )

    token = :sys.get_state(passive.session).passive_token
    send(passive.session, {:passive_flush, token})
    assert delta = receive_frame("passive", "PASSIVE_UPDATE_V2")

    assert delta["d"]["channels"] == [
             %{"guild_id" => "100", "channel_id" => "10", "last_message_id" => "1010"}
           ]

    assert delta["d"]["read_states"] == [read]
    assert [%{"user_id" => "2", "channel_id" => "12"}] = delta["d"]["voice_states"]
    passive_bytes = byte_size(Jason.encode!(delta))

    IO.puts(
      "Passive traffic comparison: active=#{active_bytes} bytes, passive=#{passive_bytes} bytes"
    )

    assert passive_bytes < active_bytes
    send_frame(passive.session, 27, %{"passive" => false})
    assert %{"channels" => channels} = await(passive, "GUILD_CREATE")
    assert Enum.find(channels, &(&1["id"] == "10"))["last_message_id"] == "1010"

    publish(11, %{"id" => "1011", "channel_id" => "10", "content" => "after activation"})
    assert %{"id" => "1011"} = await(passive, "MESSAGE_CREATE")
  end

  test "permission loss removes buffered markers and read states before a flush" do
    guild = Fixtures.guild_payload("100")

    ready = %{
      "user" => %{"id" => "3"},
      "guilds" => [guild],
      "private_channels" => [],
      "relationships" => [],
      "user_settings" => %{}
    }

    Fakes.put({:guild, "100"}, {:ok, guild})
    Fakes.put({:ready, "3"}, {:ok, ready})
    Fakes.put({:read_states, "3"}, {:ok, %{entries: [], versioned: true}})
    passive = client("restricted")
    identify(passive.session, "3", %{"passive" => true})
    assert await(passive, "READY")
    :sys.get_state(passive.session)

    {:ok, guild_pid} =
      Ports.server_directory().lookup({:guild, "100"})

    :sys.get_state(guild_pid)
    :sys.get_state(passive.session)

    publish(21, %{"id" => "1021", "channel_id" => "10"})
    :sys.get_state(guild_pid)
    :sys.get_state(passive.session)

    read = %{
      "channel_id" => "10",
      "last_message_id" => "1020",
      "mention_count" => 1,
      "read_state_version" => "1"
    }

    envelope = %{
      "id" => "read-21",
      "type" => "READ_STATE_UPDATE",
      "data" => read,
      "target_user_ids" => ["3"]
    }

    Ingest.handle(:message, Jason.encode!(envelope))
    :sys.get_state(passive.session)

    channel = %{
      "id" => "10",
      "type" => 0,
      "permission_overwrites" => [%{"id" => "100", "type" => 0, "allow" => "0", "deny" => "1024"}]
    }

    envelope = %{
      "id" => "revoke-21",
      "type" => "CHANNEL_UPDATE",
      "data" => channel,
      "guild_id" => "100",
      "channel_id" => "10"
    }

    Ingest.handle(:guild, Jason.encode!(envelope))
    :sys.get_state(guild_pid)
    token = :sys.get_state(passive.session).passive_token
    send(passive.session, {:passive_flush, token})
    assert await(passive, "PASSIVE_UPDATE_V2", 100) == :none
  end

  test "private thread membership is rechecked at flush and wake" do
    thread = %{
      "id" => "21",
      "type" => 12,
      "parent_id" => "10",
      "owner_id" => "2",
      "last_message_id" => "1025"
    }

    guild =
      Fixtures.guild_payload("100")
      |> Map.put("threads", [thread])
      |> Map.put("thread_members", %{"21" => %{"version" => 2, "user_ids" => ["2", "3"]}})

    ready = %{
      "user" => %{"id" => "3"},
      "guilds" => [guild],
      "private_channels" => [],
      "relationships" => [],
      "user_settings" => %{}
    }

    Fakes.put({:guild, "100"}, {:ok, guild})
    Fakes.put({:ready, "3"}, {:ok, ready})
    Fakes.put({:read_states, "3"}, {:ok, %{entries: [], versioned: true}})
    client = client("thread-passive")
    identify(client.session, "3", %{"passive" => true})
    assert await(client, "READY")
    :sys.get_state(client.session)
    {:ok, guild_pid} = Ports.server_directory().lookup({:guild, "100"})
    :sys.get_state(guild_pid)
    token = :sys.get_state(client.session).passive_token
    send(client.session, {:passive_flush, token})

    assert %{"channels" => [%{"channel_id" => "21", "last_message_id" => "1025"}]} =
             await(client, "PASSIVE_UPDATE_V2")

    publish_thread_message("1026")
    :sys.get_state(guild_pid)
    :sys.get_state(client.session)
    publish_membership(3, ["2"])
    publish_membership(2, ["2", "3"])
    :sys.get_state(guild_pid)
    token = :sys.get_state(client.session).passive_token
    send(client.session, {:passive_flush, token})
    assert await(client, "PASSIVE_UPDATE_V2", 100) == :none
    assert await(client, "THREAD_DELETE", 100) == :none

    send_frame(client.session, 27, %{"passive" => false})
    assert %{"threads" => threads} = await(client, "GUILD_CREATE")
    refute Enum.any?(threads, &(&1["id"] == "21"))
    publish_membership(4, ["2", "3"])
    assert %{"id" => "21"} = await(client, "THREAD_CREATE")
    publish_thread_message("1027")
    assert %{"id" => "1027"} = await(client, "MESSAGE_CREATE")
  end

  defp publish_thread_message(id) do
    data = %{"id" => id, "channel_id" => "21"}

    envelope = %{
      "id" => "thread-message-#{id}",
      "type" => "MESSAGE_CREATE",
      "guild_id" => "100",
      "channel_id" => "21",
      "data" => data
    }

    Ingest.handle(:message, Jason.encode!(envelope))
  end

  defp publish_membership(version, users) do
    data = %{
      "id" => "21",
      "guild_id" => "100",
      "added_members" => [],
      "removed_member_ids" => [],
      "_membership_version" => version,
      "_member_ids" => users
    }

    envelope = %{
      "id" => "thread-members-#{version}",
      "type" => "THREAD_MEMBERS_UPDATE",
      "guild_id" => "100",
      "channel_id" => "21",
      "data" => data
    }

    Ingest.handle(:guild, Jason.encode!(envelope))
  end

  test "a private thread with missing parent is hidden even from its owner" do
    thread = %{
      "id" => "21",
      "type" => 12,
      "parent_id" => "999",
      "owner_id" => "3",
      "last_message_id" => "1025"
    }

    guild =
      Fixtures.guild_payload("100")
      |> Map.put("threads", [thread])
      |> Map.put("thread_members", %{"21" => %{"version" => 1, "user_ids" => ["3"]}})

    ready = %{
      "user" => %{"id" => "3"},
      "guilds" => [guild],
      "private_channels" => [],
      "relationships" => [],
      "user_settings" => %{}
    }

    Fakes.put({:guild, "100"}, {:ok, guild})
    Fakes.put({:ready, "3"}, {:ok, ready})
    Fakes.put({:read_states, "3"}, {:ok, %{entries: [], versioned: true}})
    client = client("missing-parent")
    identify(client.session, "3", %{"passive" => true})
    assert await(client, "READY")
    :sys.get_state(client.session)
    {:ok, guild_pid} = Ports.server_directory().lookup({:guild, "100"})
    :sys.get_state(guild_pid)
    token = :sys.get_state(client.session).passive_token
    send(client.session, {:passive_flush, token})
    assert await(client, "PASSIVE_UPDATE_V2", 100) == :none
    send_frame(client.session, 27, %{"passive" => false})
    assert %{"threads" => threads} = await(client, "GUILD_CREATE")
    refute Enum.any?(threads, &(&1["id"] == "21"))
  end

  test "guild removal is sent exactly once to a passive session" do
    guild = Fixtures.guild_payload("100")

    ready = %{
      "user" => %{"id" => "3"},
      "guilds" => [guild],
      "private_channels" => [],
      "relationships" => [],
      "user_settings" => %{}
    }

    Fakes.put({:guild, "100"}, {:ok, guild})
    Fakes.put({:ready, "3"}, {:ok, ready})
    Fakes.put({:read_states, "3"}, {:ok, %{entries: [], versioned: true}})
    client = client("removed")
    identify(client.session, "3", %{"passive" => true})
    assert await(client, "READY")
    :sys.get_state(client.session)
    {:ok, guild_pid} = Ports.server_directory().lookup({:guild, "100"})
    :sys.get_state(guild_pid)

    event = %{
      "id" => "remove-guild-member",
      "type" => "GUILD_MEMBER_REMOVE",
      "guild_id" => "100",
      "data" => %{"guild_id" => "100", "user" => %{"id" => "3"}}
    }

    Ingest.handle(:guild, Jason.encode!(event))
    assert %{"id" => "100"} = await(client, "GUILD_DELETE")
    assert await(client, "GUILD_DELETE", 100) == :none
  end

  test "passive sessions require certified reads and cannot replay after reconnect" do
    {legacy, _} = connect()
    identify(legacy, "1", %{"passive" => true})
    assert_receive {:gw_close, 4000}

    Fakes.put({:read_states, "1"}, {:ok, %{entries: [], versioned: true}})
    {session, _} = connect()
    identify(session, "1", %{"passive" => true})
    %{"d" => %{"session_id" => session_id}} = next_dispatch("READY")
    {stub, _} = connect()
    send_frame(stub, 6, %{"token" => "user-1", "session_id" => session_id, "seq" => 1})
    assert %{"op" => 9, "d" => false} = next_frame()
    refute Process.alive?(session)
  end

  defp receive_frame(name, event) do
    receive do
      {:client, ^name, %{"t" => ^event} = frame} -> frame
    after
      1_000 -> nil
    end
  end

  defp publish(id, payload) do
    envelope = %{
      "id" => "passive-#{id}",
      "type" => "MESSAGE_CREATE",
      "v" => 1,
      "data" => payload,
      "guild_id" => "100",
      "channel_id" => "10"
    }

    Ingest.handle(:message, Jason.encode!(envelope))
  end
end
