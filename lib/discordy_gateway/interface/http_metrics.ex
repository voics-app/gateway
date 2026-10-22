defmodule DiscordyGateway.Interface.HttpMetrics do
  @moduledoc """
  Request counter and latency telemetry for the HTTP router (`http_requests_total`,
  `http_request_duration_seconds`; labels method, route template, status class).

  WebSocket upgrades never reach `send_resp`, so the router records them explicitly (status 101).
  """

  @behaviour Plug

  @event [:discordy_gateway, :http, :request]

  @doc "Telemetry event the metric definitions listen to."
  def event, do: @event

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, _opts) do
    conn = Plug.Conn.put_private(conn, :http_metrics_start, System.monotonic_time())

    Plug.Conn.register_before_send(conn, fn conn ->
      record(conn, conn.status)
      conn
    end)
  end

  @doc "Emits the request event for `conn` with the given response status."
  def record(conn, status) do
    started = conn.private[:http_metrics_start] || System.monotonic_time()
    micros = System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond)

    :telemetry.execute(@event, %{total: 1, seconds: micros / 1_000_000}, %{
      method: conn.method,
      route: route(conn),
      status: status_class(status)
    })
  end

  @doc false
  def status_class(status) when status in 100..199, do: "1xx"
  def status_class(status) when status in 200..299, do: "2xx"
  def status_class(status) when status in 300..399, do: "3xx"
  def status_class(status) when status in 400..499, do: "4xx"
  def status_class(_status), do: "5xx"

  # The route template keeps label cardinality bounded; unmatched paths share one label.
  defp route(%{private: %{plug_route: {route, _fun}}}) when is_binary(route), do: route
  defp route(_conn), do: "unmatched"
end
