defmodule DiscordyGateway.Interface.Metrics do
  @moduledoc "Prometheus metrics: VM figures, session/event counters, and live gauges from the directories."

  import Telemetry.Metrics

  alias DiscordyGateway.Application.Ports

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
      last_value("discordy_gateway.gauges.sessions_local"),
      last_value("discordy_gateway.gauges.guild_processes"),
      last_value("discordy_gateway.gauges.voice_allocations")
    ]
  end

  @doc false
  def measure_gauges do
    sessions = Ports.session_directory().count_by_node()

    :telemetry.execute([:discordy_gateway, :gauges], %{
      sessions_local: Map.get(sessions, Atom.to_string(node()), 0),
      guild_processes: Ports.server_directory().count(:guild),
      voice_allocations: Ports.voice_allocations().count()
    })
  end
end
