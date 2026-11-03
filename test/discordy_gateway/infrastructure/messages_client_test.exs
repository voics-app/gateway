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
  end
end
