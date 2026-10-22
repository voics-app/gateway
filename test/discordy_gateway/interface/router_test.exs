defmodule DiscordyGateway.Interface.RouterTest do
  use DiscordyGateway.Case, async: false

  import Plug.Test

  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Interface.Router

  defp call(method, path, headers \\ []) do
    conn =
      Enum.reduce(headers, conn(method, path), fn {k, v}, c ->
        Plug.Conn.put_req_header(c, k, v)
      end)

    Router.call(conn, Router.init([]))
  end

  defp internal, do: [{"x-internal-token", "test-internal-token"}]

  test "healthz is always ok and readyz reflects the dependency checks" do
    assert %{status: 200} = call(:get, "/healthz")
    assert %{status: 200} = call(:get, "/readyz")

    previous = Application.get_env(:discordy_gateway, :readiness)
    Application.put_env(:discordy_gateway, :readiness, down: {__MODULE__, :down?})
    on_exit(fn -> Application.put_env(:discordy_gateway, :readiness, previous || []) end)

    conn = call(:get, "/readyz")
    assert conn.status == 503

    assert %{"status" => "unavailable", "checks" => %{"down" => false}} =
             Jason.decode!(conn.resp_body)
  end

  def down?, do: false

  test "metrics exposes Prometheus text" do
    :telemetry.execute([:discordy_gateway, :session, :started], %{count: 1}, %{code: nil})
    conn = call(:get, "/metrics")
    assert conn.status == 200
    assert conn.resp_body =~ "discordy_gateway_session_started_count"
  end

  test "requests are counted and timed by route template and status class" do
    call(:get, "/healthz")
    call(:post, "/internal/v1/voice/nodes/abc/drain")
    call(:get, "/nope")
    body = call(:get, "/metrics").resp_body

    assert body =~ ~s(http_requests_total{method="GET",route="/healthz",status="2xx"})
    assert body =~ ~s(route="/internal/v1/voice/nodes/:node_id/drain",status="4xx")
    assert body =~ ~s(route="/*_path",status="4xx")
    assert body =~ "http_request_duration_seconds_bucket"
  end

  test "the gateway endpoint requires v=1 and encoding=json" do
    assert %{status: 400} = call(:get, "/gateway")
    assert %{status: 400} = call(:get, "/gateway?v=2&encoding=json")
    assert %{status: 400} = call(:get, "/gateway?v=1&encoding=etf")
  end

  test "internal endpoints reject missing or wrong tokens" do
    assert %{status: 401} = call(:get, "/internal/v1/admin/stats")
    assert %{status: 401} = call(:get, "/internal/v1/admin/stats", [{"x-internal-token", "nope"}])
    assert %{status: 401} = call(:post, "/internal/v1/voice/nodes/a/drain")
  end

  test "admin stats reports sessions, guild processes and voice allocations" do
    online("2")
    conn = call(:get, "/internal/v1/admin/stats", internal())
    assert conn.status == 200

    assert %{
             "sessions_online" => online,
             "sessions_by_node" => by_node,
             "guild_processes" => _,
             "voice_allocations" => 0
           } =
             Jason.decode!(conn.resp_body)

    assert online >= 1
    assert is_map(by_node)
  end

  test "drain and undrain call through to the voice control" do
    assert %{status: 204} = call(:post, "/internal/v1/voice/nodes/n1/drain", internal())
    assert %{status: 204} = call(:post, "/internal/v1/voice/nodes/n1/undrain", internal())
    assert [{:drain, "n1"}, {:undrain, "n1"}] = Fakes.calls(:control)
    assert MapSet.size(Ports.voice_allocations().drained()) == 0
  end

  test "unknown paths are 404" do
    assert %{status: 404} = call(:get, "/nope")
  end
end
