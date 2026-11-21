defmodule DiscordyGateway.Interface.Router do
  @moduledoc """
  Routes: `/gateway` (WebSocket, `v=1&encoding=json`), `/healthz`, `/readyz`, `/metrics`, and the
  internal API under `/internal/v1` (P4: admin stats and voice node drain/undrain).
  """

  use Plug.Router

  alias DiscordyGateway.Application.{Guild, Stats, Voice.DrainNode}
  alias DiscordyGateway.Interface.{Health, HttpMetrics, InternalAuth, Socket}

  @max_frame_size 4_000_000
  @idle_timeout_ms 90_000
  # The socket process writes every frame once and keeps none; frequent full sweeps release the
  # large binaries (READY, GUILD_CREATE) it wrote instead of pinning them in its old heap.
  @fullsweep_after 10

  plug :match
  plug HttpMetrics
  plug :dispatch

  get "/gateway" do
    conn = fetch_query_params(conn)

    if conn.query_params["v"] == "1" and conn.query_params["encoding"] == "json" and
         conn.query_params["compress"] in [nil, "zstd-stream"] do
      HttpMetrics.record(conn, 101)

      conn
      |> WebSockAdapter.upgrade(Socket, [compression: conn.query_params["compress"]],
        timeout: @idle_timeout_ms,
        max_frame_size: @max_frame_size,
        fullsweep_after: @fullsweep_after,
        compress: false
      )
      |> halt()
    else
      json(conn, 400, %{"error" => "unsupported version, encoding or compression"})
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

  get "/internal/v1/voice/guilds/:guild_id/channels/:channel_id/members/:user_id/publish-permissions" do
    conn
    |> InternalAuth.call([])
    |> maybe(fn conn ->
      case Guild.publish_permissions(guild_id, channel_id, user_id) do
        {:ok, grant} -> json(conn, 200, grant)
        :unavailable -> json(conn, 503, %{"error" => "capability_unavailable"})
      end
    end)
  end

  get "/internal/v1/guilds/:guild_id/channels/:channel_id/voice-members/:user_id" do
    conn
    |> InternalAuth.call([])
    |> maybe(fn conn ->
      case Guild.user_in_voice?(guild_id, channel_id, user_id) do
        {:ok, connected} -> json(conn, 200, %{"connected" => connected})
        {:error, _reason} -> json(conn, 503, %{"error" => "voice_state_unavailable"})
      end
    end)
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
