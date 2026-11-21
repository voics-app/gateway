defmodule DiscordyGateway.Interface.SocketTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Admission
  alias DiscordyGateway.Fakes.MemoryProbe
  alias DiscordyGateway.Interface.{Router, Socket}

  setup do
    {:ok, server} = Bandit.start_link(plug: Router, port: 0, ip: :loopback)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, conn} = :gun.open(~c"127.0.0.1", port, %{protocols: [:http]})
    {:ok, :http} = :gun.await_up(conn)
    on_exit(fn -> :gun.close(conn) end)
    %{conn: conn}
  end

  defp upgrade(conn, query \\ "v=1&encoding=json") do
    stream = :gun.ws_upgrade(conn, "/gateway?" <> query)

    receive do
      {:gun_upgrade, ^conn, ^stream, ["websocket"], _headers} -> {:ok, stream}
      {:gun_response, ^conn, ^stream, _fin, status, _headers} -> {:rejected, status}
    after
      2_000 -> :timeout
    end
  end

  defp recv(conn) do
    receive do
      {:gun_ws, ^conn, _stream, {:text, text}} -> {:text, Jason.decode!(text)}
      {:gun_ws, ^conn, _stream, {:close, code, _reason}} -> {:close, code}
    after
      2_000 -> :timeout
    end
  end

  test "completes HELLO, IDENTIFY and READY over a real WebSocket", %{conn: conn} do
    assert {:ok, stream} = upgrade(conn)
    assert {:text, %{"op" => 10, "d" => %{"heartbeat_interval" => 1000}}} = recv(conn)

    :gun.ws_send(
      conn,
      stream,
      {:text, Jason.encode!(%{"op" => 2, "d" => %{"token" => "user-5", "intents" => 0}})}
    )

    assert {:text, %{"op" => 0, "t" => "READY", "s" => 1, "d" => %{"user" => %{"id" => "5"}}}} =
             recv(conn)

    :gun.ws_send(conn, stream, {:text, Jason.encode!(%{"op" => 1, "d" => 1})})
    assert {:text, %{"op" => 11}} = recv(conn)
  end

  test "negotiates a continuous compressed stream on a real WebSocket", %{conn: conn} do
    assert {:ok, stream} = upgrade(conn, "v=1&encoding=json&compress=zstd-stream")
    {:ok, decoder} = :zstd.context(:decompress)
    assert %{"op" => 10} = recv_compressed(conn, decoder)

    :gun.ws_send(
      conn,
      stream,
      {:text, Jason.encode!(%{"op" => 2, "d" => %{"token" => "user-5", "intents" => 0}})}
    )

    assert %{"t" => "READY", "s" => 1} = recv_compressed(conn, decoder)
    :gun.ws_send(conn, stream, {:text, Jason.encode!(%{"op" => 1, "d" => 1})})
    assert %{"op" => 11} = recv_compressed(conn, decoder)
    :zstd.close(decoder)
  end

  defp recv_compressed(conn, decoder) do
    receive do
      {:gun_ws, ^conn, _stream, {:binary, bytes}} ->
        {:continue, json} = :zstd.stream(decoder, bytes)
        Jason.decode!(json)
    after
      2_000 -> :timeout
    end
  end

  test "stream retains history and round-trips event payloads" do
    {:ok, encoder} = :zstd.context(:compress)
    {:ok, decoder} = :zstd.context(:decompress)
    state = %{session: self(), compression: encoder}

    frames = [
      %{"op" => 0, "t" => "READY", "d" => %{"session_id" => "abc", "user" => %{"id" => "5"}}},
      %{"op" => 0, "t" => "MESSAGE_CREATE", "d" => %{"content" => "hello", "id" => "123"}},
      %{"op" => 0, "t" => "RESUMED", "d" => %{}}
    ]

    for frame <- frames do
      original = Jason.encode!(frame)
      assert {:push, {:binary, bytes}, ^state} = Socket.handle_info({:gw_frame, original}, state)

      IO.puts(
        "compression sample #{frame["t"]}: #{byte_size(original)} plain / #{byte_size(bytes)} compressed bytes"
      )

      assert {:continue, json} = :zstd.stream(decoder, bytes)
      assert Jason.decode!(json) == frame
    end

    :zstd.close(encoder)
    :zstd.close(decoder)
  end

  test "rejects unknown compression", %{conn: conn} do
    assert {:rejected, 400} = upgrade(conn, "v=1&encoding=json&compress=invalid")
  end

  test "invalid JSON closes with 4002", %{conn: conn} do
    assert {:ok, stream} = upgrade(conn)
    assert {:text, %{"op" => 10}} = recv(conn)
    :gun.ws_send(conn, stream, {:text, "{not json"})
    assert {:close, 4002} = recv(conn)
  end

  test "an unauthenticated command closes with 4003", %{conn: conn} do
    assert {:ok, stream} = upgrade(conn)
    assert {:text, %{"op" => 10}} = recv(conn)

    :gun.ws_send(
      conn,
      stream,
      {:text,
       Jason.encode!(%{
         "op" => 4,
         "d" => %{
           "guild_id" => nil,
           "channel_id" => nil,
           "self_mute" => false,
           "self_deaf" => false
         }
       })}
    )

    assert {:close, 4003} = recv(conn)
  end

  test "refuses a connection with the retryable close code 4015 while memory is short", %{
    conn: conn
  } do
    MemoryProbe.put(900, 1_000)
    Admission.sample()

    on_exit(fn ->
      MemoryProbe.reset()
      Admission.sample()
    end)

    assert {:ok, _stream} = upgrade(conn)
    assert {:close, 4015} = recv(conn)
  end

  test "rejects unsupported versions", %{conn: conn} do
    assert {:rejected, 400} = upgrade(conn, "v=9&encoding=json")
  end
end
