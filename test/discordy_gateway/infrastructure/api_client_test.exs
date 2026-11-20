defmodule DiscordyGateway.Infrastructure.ApiClientTest do
  use ExUnit.Case, async: false

  alias DiscordyGateway.Infrastructure.Http.ApiClient

  setup do
    previous = Application.fetch_env!(:discordy_gateway, :http)

    Application.put_env(
      :discordy_gateway,
      :http,
      Keyword.put(previous, :req_options, plug: {Req.Test, __MODULE__})
    )

    if :ets.whereis(:discordy_introspect_cache) == :undefined, do: ApiClient.init_cache()
    :ets.delete_all_objects(:discordy_introspect_cache)
    on_exit(fn -> Application.put_env(:discordy_gateway, :http, previous) end)
  end

  test "sends the internal token and decodes the ready payload" do
    Req.Test.stub(__MODULE__, fn conn ->
      assert conn.request_path == "/internal/v1/users/42/ready"
      assert Plug.Conn.get_req_header(conn, "x-internal-token") == ["test-internal-token"]
      Req.Test.json(conn, %{"user" => %{"id" => "42"}})
    end)

    assert {:ok, %{"user" => %{"id" => "42"}}} = ApiClient.fetch_ready("42")
  end

  test "sends an authenticated checkpoint and accepts an empty success" do
    checkpoint = %{"guild_id" => "123", "connected" => true}

    Req.Test.stub(__MODULE__, fn conn ->
      assert conn.request_path == "/internal/v1/voice/activity-checkpoints"
      assert Plug.Conn.get_req_header(conn, "x-internal-token") == ["test-internal-token"]
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      assert Jason.decode!(body) == checkpoint
      Plug.Conn.send_resp(conn, 204, "")
    end)

    assert :ok = ApiClient.voice_activity_checkpoint(checkpoint)
  end

  test "maps HTTP statuses to errors" do
    for {status, expected} <- [
          {401, :unauthorized},
          {403, :forbidden},
          {404, :not_found},
          {500, {:http_status, 500}}
        ] do
      Req.Test.stub(__MODULE__, &Plug.Conn.send_resp(&1, status, "{}"))
      assert {:error, ^expected} = ApiClient.fetch_channel_access("c", "u")
    end
  end

  test "caches bot token introspection for 60 seconds" do
    counter = :counters.new(1, [])

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(counter, 1, 1)
      Req.Test.json(conn, %{"user_id" => "900", "kind" => "bot", "application_id" => "900"})
    end)

    assert {:ok, %{"user_id" => "900"}} = ApiClient.introspect("tok")
    assert {:ok, %{"user_id" => "900"}} = ApiClient.introspect("tok")
    assert :counters.get(counter, 1) == 1
  end

  test "url-encodes path segments" do
    Req.Test.stub(__MODULE__, fn conn ->
      assert conn.request_path == "/internal/v1/guilds/a%2Fb/state"
      Req.Test.json(conn, %{})
    end)

    assert {:ok, %{}} = ApiClient.fetch_guild_state("a/b")
  end
end
