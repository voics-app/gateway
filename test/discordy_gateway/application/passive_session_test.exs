defmodule DiscordyGateway.Application.PassiveSessionTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Events.Ingest
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
    token = :sys.get_state(passive.session).passive_token
    send(passive.session, {:passive_flush, token})
    assert delta = receive_frame("passive", "PASSIVE_UPDATE_V2")

    assert delta["d"]["channels"] == [
             %{"guild_id" => "100", "channel_id" => "10", "last_message_id" => "1010"}
           ]

    assert byte_size(Jason.encode!(delta)) < active_bytes
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

    {:ok, guild_pid} =
      DiscordyGateway.Application.Ports.server_directory().lookup({:guild, "100"})

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
