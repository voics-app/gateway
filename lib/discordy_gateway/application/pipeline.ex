defmodule DiscordyGateway.Application.Pipeline do
  @moduledoc """
  Per-stage timing of the path from the event-bus consumer to the socket, reported as the
  `discordy_gateway_pipeline_stage_seconds` histogram (label `stage`). The stages are `ingest`
  (everything the consumer does for one record), `dedup`, `guild_queue` (time a routed event
  waited in the guild process mailbox), `fanout_plan`, `fanout_send` (guild process) and
  `session_dispatch` (frame, resume buffer write and handover to the socket in the session).
  The stage whose time grows with load while the others stay flat is the serial bottleneck.
  """

  @event [:discordy_gateway, :pipeline, :stage]

  @doc "Telemetry event the histogram listens to."
  @spec event() :: [atom()]
  def event, do: @event

  @doc "Runs `fun`, reports how long it took under `stage`, and returns its result."
  @spec measure(atom(), (-> result)) :: result when result: term()
  def measure(stage, fun) do
    started = System.monotonic_time()
    result = fun.()
    since(stage, started)
    result
  end

  @doc "Reports the time elapsed since `started` (a `System.monotonic_time/0` value) under `stage`."
  @spec since(atom(), integer()) :: :ok
  def since(stage, started) do
    elapsed = System.monotonic_time() - started
    seconds = System.convert_time_unit(elapsed, :native, :microsecond) / 1_000_000
    :telemetry.execute(@event, %{seconds: seconds}, %{stage: stage})
  end
end
