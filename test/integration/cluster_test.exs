defmodule DiscordyGateway.Integration.ClusterTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Events.Ingest
  alias DiscordyGateway.Application.{Ids, Session}
  alias DiscordyGateway.ClusterPeer
  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Fixtures
  alias DiscordyGateway.Infrastructure.Cluster.{HordeServerDirectory, PgSessionDirectory}
  alias DiscordyGateway.Infrastructure.Cluster.RoutingMembership
  alias DiscordyGateway.Infrastructure.Kafka
  alias DiscordyGateway.Interface.Router

  @moduletag :cluster

  setup do
    assert Node.alive?(), "run with elixir --sname gateway_cluster -S mix test --include cluster"
    args = [~c"+S", ~c"2", ~c"-pa" | :code.get_path()]

    {:ok, peer, peer_node} =
      :peer.start(%{name: :peer.random_name(~c"gateway"), args: args, connection: :standard_io})

    config =
      for app <- [:discordy_gateway, :logger, :opentelemetry],
          do: {app, Application.get_all_env(app)}

    {:ok, _} = :peer.call(peer, ClusterPeer, :start, [config], 25_000)
    assert Node.connect(peer_node)
    settings = Application.get_env(:discordy_gateway, :settings)

    on_exit(fn ->
      Application.put_env(:discordy_gateway, :settings, settings)
      :peer.stop(peer)
    end)

    for target <- [node(), peer_node] do
      :erpc.call(target, Application, :put_env, [
        :discordy_gateway,
        :settings,
        Keyword.merge(Application.get_env(:discordy_gateway, :settings),
          heartbeat_interval_ms: 60_000,
          identify_timeout_ms: 5_000,
          resume_window_ms: 5_000
        )
      ])
    end

    %{peer: peer, peer_node: peer_node}
  end

  test "one consumed guild event reaches sessions on both replicas exactly once", %{
    peer_node: remote
  } do
    guild = Fixtures.guild_payload("cluster-guild")
    ready = {:ok, %{"user" => %{"id" => "1"}, "guilds" => [guild], "relationships" => []}}
    for target <- [node(), remote], do: :erpc.call(target, Fakes, :put, [{:ready, "1"}, ready])
    local = online("1")
    assert %{"id" => "cluster-guild"} = await(local, "GUILD_CREATE", 5_000)
    {:ok, remote_session} = :erpc.call(remote, Session, :start, [self()])
    identify(remote_session, "1")
    assert %{"t" => "READY"} = next_dispatch("READY", 5_000)
    assert %{"t" => "GUILD_CREATE"} = next_dispatch("GUILD_CREATE", 5_000)
    eventually(fn -> length(PgSessionDirectory.sessions_for_user("1")) == 2 end)

    eventually(fn ->
      case HordeServerDirectory.lookup({:guild, "cluster-guild"}) do
        {:ok, owner} ->
          :erpc.call(remote, HordeServerDirectory, :lookup, [{:guild, "cluster-guild"}]) ==
            {:ok, owner} and
            subscribed?(local.session, owner) and subscribed?(remote_session, owner)

        :error ->
          false
      end
    end)

    envelope = %{
      "id" => Ids.uuid_v7(),
      "v" => 1,
      "type" => "MESSAGE_CREATE",
      "guild_id" => "cluster-guild",
      "channel_id" => "10",
      "occurred_at" => "2026-11-18T00:00:00Z",
      "data" => %{
        "id" => "message-1",
        "channel_id" => "10",
        "author" => %{"id" => "1"},
        "content" => "cross node"
      }
    }

    assert :ok = publish(remote, envelope)
    assert %{"content" => "cross node"} = await(local, "MESSAGE_CREATE", 5_000)
    assert %{"d" => %{"content" => "cross node"}} = next_dispatch("MESSAGE_CREATE", 5_000)
    assert await(local, "MESSAGE_CREATE", 100) == :none
    assert next_dispatch("MESSAGE_CREATE", 100) == :none
  end

  test "real WebSockets reconnect through another replica and replay the same session", %{
    peer: peer,
    peer_node: remote
  } do
    server = start_supervised!({Bandit, plug: Router, port: 0, ip: :loopback})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {first, stream} = websocket(port)
    ws_send(first, stream, 2, %{"token" => "user-resume-user", "intents" => 0})
    assert %{"t" => "READY", "d" => %{"session_id" => session_id}} = ws_receive(first)
    session = PgSessionDirectory.whereis(session_id)

    eventually(fn ->
      :erpc.call(remote, PgSessionDirectory, :whereis, [session_id]) == session
    end)

    :gun.close(first)
    send(session, {:dispatch, "TYPING_START", ~s({"channel_id":"10"})})
    {second, second_stream} = websocket(:peer.call(peer, ClusterPeer, :port, []))

    ws_send(second, second_stream, 6, %{
      "token" => "user-resume-user",
      "session_id" => session_id,
      "seq" => 1
    })

    assert %{"s" => 2, "t" => "TYPING_START"} = ws_receive(second)
    assert %{"s" => 3, "t" => "RESUMED"} = ws_receive(second)
    ws_send(second, second_stream, 1, 3)
    assert %{"op" => 11} = ws_receive(second)
    :gun.close(second)
  end

  test "readiness detects a partition and recovers after routing membership reconverges", %{
    peer: peer,
    peer_node: remote
  } do
    expected = Enum.map([node(), remote], &Atom.to_string/1)

    start_supervised!(
      {RoutingMembership, query: expected, resolver: &ClusterPeer.expected_names/1}
    )

    # The remote node has no monitor yet, like the old version during a rolling upgrade.
    eventually(&RoutingMembership.ready?/0)
    {:ok, _} = :peer.call(peer, ClusterPeer, :start_membership, [expected])
    eventually(&RoutingMembership.ready?/0)
    eventually(fn -> :peer.call(peer, RoutingMembership, :ready?, []) end)
    cookie = Node.get_cookie()
    Node.set_cookie(remote, :partition_test)
    on_exit(fn -> Node.set_cookie(remote, cookie) end)
    assert Node.disconnect(remote)
    eventually(fn -> not RoutingMembership.ready?() end)
    Node.set_cookie(remote, cookie)
    assert Node.connect(remote)
    eventually(&RoutingMembership.ready?/0)
  end

  test "simultaneous replicas generate distinct UUIDs in the same millisecond", %{peer: peer} do
    time = System.os_time(:millisecond)
    pending = Task.async(fn -> :peer.call(peer, ClusterPeer, :event_ids, [time, 10_000]) end)
    ids = ClusterPeer.event_ids(time, 10_000) ++ Task.await(pending)
    assert MapSet.size(MapSet.new(ids)) == 20_000
  end

  defp websocket(port) do
    {:ok, conn} = :gun.open(~c"127.0.0.1", port, %{protocols: [:http]})
    {:ok, :http} = :gun.await_up(conn)
    stream = :gun.ws_upgrade(conn, "/gateway?v=1&encoding=json")
    assert_receive {:gun_upgrade, ^conn, ^stream, ["websocket"], _}, 5_000
    assert %{"op" => 10} = ws_receive(conn)
    {conn, stream}
  end

  defp publish(remote, envelope) do
    case System.get_env("KAFKA_TEST_HOST") do
      nil ->
        :erpc.call(remote, Ingest, :handle, [:message, Jason.encode!(envelope)])

      host ->
        port = System.get_env("KAFKA_TEST_PORT", "9092") |> String.to_integer()
        brokers = [{String.to_charlist(host), port}]
        start_supervised!({Kafka.Supervisor, brokers: brokers})
        {:ok, _} = :erpc.call(remote, ClusterPeer, :start_kafka, [brokers])
        Process.sleep(8_000)
        Kafka.Publisher.publish(Contracts.topic(:message), "cluster-guild", envelope)
    end
  end

  defp ws_send(conn, stream, op, data),
    do: :gun.ws_send(conn, stream, {:text, Jason.encode!(%{op: op, d: data})})

  defp ws_receive(conn) do
    receive do
      {:gun_ws, ^conn, _stream, {:text, text}} -> Jason.decode!(text)
    after
      5_000 -> flunk("gateway did not send the expected WebSocket frame")
    end
  end

  defp subscribed?(session, owner) do
    {:monitors, monitors} = :erpc.call(node(session), Process, :info, [session, :monitors])
    {:process, owner} in monitors
  end

  defp eventually(fun, attempts \\ 500) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("cluster did not converge")
      true -> Process.sleep(10) && eventually(fun, attempts - 1)
    end
  end
end
