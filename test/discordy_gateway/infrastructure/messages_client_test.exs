defmodule DiscordyGateway.Infrastructure.MessagesClientTest do
  use ExUnit.Case, async: false

  alias DiscordyGateway.Infrastructure.Http.MessagesClient

  setup do
    previous = Application.fetch_env!(:discordy_gateway, :http)

    Application.put_env(
      :discordy_gateway,
      :http,
      Keyword.put(previous, :req_options, plug: {Req.Test, __MODULE__})
    )

    on_exit(fn -> Application.put_env(:discordy_gateway, :http, previous) end)
  end

  test "posts a type 3 message with the internal token and returns its id" do
    Req.Test.stub(__MODULE__, fn conn ->
      assert conn.method == "POST"
      assert conn.request_path == "/internal/v1/channels/10/messages"
      assert Plug.Conn.get_req_header(conn, "x-internal-token") == ["test-internal-token"]
      {:ok, raw, conn} = Plug.Conn.read_body(conn)

      assert Jason.decode!(raw) == %{
               "type" => 3,
               "author_id" => "2",
               "call" => %{"participants" => ["2"], "ended_timestamp" => nil}
             }

      conn |> Plug.Conn.put_status(201) |> Req.Test.json(%{"id" => "m1"})
    end)

    assert {:ok, "m1"} = MessagesClient.create_call_message("10", "2", ["2"])
  end

  test "patches the call of a message" do
    Req.Test.stub(__MODULE__, fn conn ->
      assert conn.method == "PATCH"
      assert conn.request_path == "/internal/v1/channels/10/messages/m1/call"
      {:ok, raw, conn} = Plug.Conn.read_body(conn)

      assert Jason.decode!(raw) == %{
               "participants" => ["2", "3"],
               "ended_timestamp" => "2026-11-03T10:00:00Z"
             }

      Req.Test.json(conn, %{"id" => "m1"})
    end)

    update = %{participants: ["2", "3"], ended_timestamp: "2026-11-03T10:00:00Z"}
    assert :ok = MessagesClient.update_call_message("10", "m1", update)
  end

  test "reports failing statuses" do
    Req.Test.stub(__MODULE__, &Plug.Conn.send_resp(&1, 503, "{}"))
    assert {:error, {:http_status, 503}} = MessagesClient.create_call_message("10", "2", ["2"])
    assert {:error, {:http_status, 503}} = MessagesClient.update_call_message("10", "m1", %{})
    assert {:error, {:http_status, 503}} = MessagesClient.fetch_read_states("1")
  end

  test "fetches versioned read state with internal auth and preserves precise versions" do
    entry = %{
      "id" => "200",
      "last_message_id" => nil,
      "mention_count" => 1,
      "read_state_version" => "9007199254740993"
    }

    Req.Test.stub(__MODULE__, fn conn ->
      assert conn.method == "GET"
      assert conn.request_path == "/internal/v1/users/1/read-states"
      assert Plug.Conn.get_req_header(conn, "x-internal-token") == ["test-internal-token"]
      conn |> Plug.Conn.put_resp_header("x-read-state-versioned", "1") |> Req.Test.json([entry])
    end)

    assert {:ok, %{entries: [^entry], versioned: true}} = MessagesClient.fetch_read_states("1")
  end

  test "empty certified accounts and legacy arrays are distinct" do
    Req.Test.stub(__MODULE__, &Req.Test.json(&1, []))
    assert {:ok, %{entries: [], versioned: false}} = MessagesClient.fetch_read_states("1")

    Req.Test.stub(__MODULE__, fn conn ->
      conn |> Plug.Conn.put_resp_header("x-read-state-versioned", "1") |> Req.Test.json([])
    end)

    assert {:ok, %{entries: [], versioned: true}} = MessagesClient.fetch_read_states("1")
  end

  test "rejects malformed certified snapshots and duplicate channel entries" do
    valid = %{
      "id" => "200",
      "last_message_id" => "500",
      "mention_count" => 1,
      "read_state_version" => "1"
    }

    invalid = [
      %{},
      [valid, valid],
      [Map.delete(valid, "read_state_version")],
      [Map.put(valid, "read_state_version", "01")],
      [Map.put(valid, "read_state_version", "9223372036854775808")],
      [Map.put(valid, "read_state_version", 1)],
      [Map.put(valid, "mention_count", -1)],
      [Map.put(valid, "mention_count", 4_294_967_296)],
      [Map.put(valid, "last_message_id", "not-an-id")]
    ]

    for body <- invalid do
      Req.Test.stub(__MODULE__, fn conn ->
        conn |> Plug.Conn.put_resp_header("x-read-state-versioned", "1") |> Req.Test.json(body)
      end)

      assert {:error, :invalid_read_states} = MessagesClient.fetch_read_states("1")
    end

    Req.Test.stub(__MODULE__, fn conn ->
      conn |> Plug.Conn.put_resp_header("x-read-state-versioned", "2") |> Req.Test.json([])
    end)

    assert {:error, :invalid_read_states} = MessagesClient.fetch_read_states("1")
  end
end
