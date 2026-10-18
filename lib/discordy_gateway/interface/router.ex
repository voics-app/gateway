defmodule DiscordyGateway.Interface.Router do
  @moduledoc """
  Routes: `/gateway` (WebSocket, `v=1&encoding=json`), `/healthz`, `/readyz`, `/metrics`, and the
  internal API under `/internal/v1` (P4: admin stats and voice node drain/undrain).
  """

  use Plug.Router

  alias DiscordyGateway.Application.{Stats, Voice.DrainNode}
  alias DiscordyGateway.Interface.{Health, InternalAuth, Socket}

  @max_frame_size 4_000_000
  @idle_timeout_ms 90_000

  plug :match
  plug :dispatch

  get "/gateway" do
    conn = fetch_query_params(conn)

    if conn.query_params["v"] == "1" and conn.query_params["encoding"] == "json" do
      conn
      |> WebSockAdapter.upgrade(Socket, [],
        timeout: @idle_timeout_ms,
        max_frame_size: @max_frame_size,
        compress: false
      )
      |> halt()
    else
      json(conn, 400, %{"error" => "unsupported version or encoding; use v=1&encoding=json"})
    end
  end

  get("/healthz", do: json(conn, 200, %{"status" => "ok"}))

  get "/readyz" do
    case Health.readiness() do
      {:ok, checks} -> json(conn, 200, %{"status" => "ready", "checks" => checks})
      {:error, checks} -> json(conn, 503, %{"status" => "unavailable", "checks" => checks})
    end
  end

  get "/metrics" do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, TelemetryMetricsPrometheus.Core.scrape(DiscordyGateway.Interface.Metrics))
  end

  get "/internal/v1/admin/stats" do
    conn |> InternalAuth.call([]) |> maybe(fn conn -> json(conn, 200, Stats.snapshot()) end)
  end

  post "/internal/v1/voice/nodes/:node_id/drain" do
    conn
    |> InternalAuth.call([])
    |> maybe(fn conn -> node_command(conn, node_id, &DrainNode.drain/1) end)
  end

  post "/internal/v1/voice/nodes/:node_id/undrain" do
    conn
    |> InternalAuth.call([])
    |> maybe(fn conn -> node_command(conn, node_id, &DrainNode.undrain/1) end)
  end

  match(_, do: json(conn, 404, %{"error" => "not_found"}))

  defp maybe(%Plug.Conn{halted: true} = conn, _fun), do: conn
  defp maybe(conn, fun), do: fun.(conn)

  defp node_command(conn, node_id, command) do
    case command.(node_id) do
      :ok -> send_resp(conn, 204, "")
      {:error, _reason} -> json(conn, 502, %{"error" => "voice_control_unavailable"})
    end
  end

  defp json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(body))
  end
end
