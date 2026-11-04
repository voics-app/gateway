defmodule DiscordyGateway.Interface.Metrics do
  @moduledoc "Prometheus metrics: VM figures, session/event counters, and live gauges from the directories."

  import Telemetry.Metrics

  alias DiscordyGateway.Application.{Pipeline, Ports}
  alias DiscordyGateway.Interface.HttpMetrics

  @latency_buckets [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0]
  @stage_buckets [0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05, 0.25, 1.0]

  @doc "Child spec of the Prometheus reporter."
  def reporter, do: {TelemetryMetricsPrometheus.Core, name: __MODULE__, metrics: metrics()}

  @doc "Child spec of the poller feeding the gauges from the directories."
  def poller,
    do:
      {:telemetry_poller,
       measurements: [{__MODULE__, :measure_gauges, []}], period: 10_000, name: :discordy_poller}

  @doc "Metric definitions."
  def metrics do
    [
      last_value("vm.memory.total", unit: :byte),
      last_value("vm.total_run_queue_lengths.total"),
      counter("discordy_gateway.session.started.count"),
      counter("discordy_gateway.session.closed.count", tags: [:code]),
      counter("discordy_gateway.dispatch.sent.count", tags: [:event]),
      counter("discordy_gateway.events.consumed.count", tags: [:topic, :outcome]),
      counter("http.requests.total",
        event_name: HttpMetrics.event(),
        measurement: :total,
        tags: [:method, :route, :status]
      ),
      distribution("http.request.duration.seconds",
        event_name: HttpMetrics.event(),
        measurement: :seconds,
        tags: [:method, :route, :status],
        reporter_options: [buckets: @latency_buckets]
      ),
      last_value("discordy_gateway.gauges.sessions_local"),
      last_value("discordy_gateway.gauges.guild_processes"),
      last_value("discordy_gateway.gauges.voice_allocations"),
      counter("discordy_gateway.admission.refused.count"),
      last_value("discordy_gateway.admission.sample.memory_bytes", unit: :byte),
      last_value("discordy_gateway.admission.sample.memory_limit_bytes", unit: :byte),
      last_value("discordy_gateway.admission.sample.closed"),
      last_value("discordy_gateway.gauges.server_mailbox_max"),
      last_value("discordy_gateway.gauges.session_mailbox_max"),
      distribution("discordy_gateway.pipeline.stage.seconds",
        event_name: Pipeline.event(),
        measurement: :seconds,
        tags: [:stage],
        reporter_options: [buckets: @stage_buckets]
      )
    ]
  end

  @doc false
  def measure_gauges do
    sessions = Ports.session_directory().count_by_node()

    :telemetry.execute([:discordy_gateway, :gauges], %{
      sessions_local: Map.get(sessions, Atom.to_string(node()), 0),
      guild_processes: Ports.server_directory().count(:guild),
      voice_allocations: Ports.voice_allocations().count(),
      server_mailbox_max: max_mailbox(:voice_servers),
      session_mailbox_max: max_mailbox(:sessions)
    })
  end

  # Longest mailbox among the local processes of a `:pg` group. A mailbox that keeps growing means
  # the process cannot keep up with what is sent to it.
  defp max_mailbox(group) do
    DiscordyGateway.Pg
    |> :pg.get_local_members(group)
    |> Enum.map(fn pid ->
      case Process.info(pid, :message_queue_len) do
        {:message_queue_len, length} -> length
        nil -> 0
      end
    end)
    |> Enum.max(fn -> 0 end)
  end
end
