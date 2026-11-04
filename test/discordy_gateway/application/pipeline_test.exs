defmodule DiscordyGateway.Application.PipelineTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Application.Pipeline

  setup do
    id = "pipeline-test-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      id,
      Pipeline.event(),
      fn _event, measurements, meta, _config -> send(test, {:stage, meta.stage, measurements}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  test "measure reports the duration of the stage and returns the result" do
    assert Pipeline.measure(:demo, fn ->
             Process.sleep(5)
             :result
           end) == :result

    assert_receive {:stage, :demo, %{seconds: seconds}}
    assert seconds >= 0.005
  end

  test "since reports the time elapsed since a monotonic timestamp" do
    started = System.monotonic_time() - System.convert_time_unit(20, :millisecond, :native)
    Pipeline.since(:queue, started)
    assert_receive {:stage, :queue, %{seconds: seconds}}
    assert seconds >= 0.02
  end
end
