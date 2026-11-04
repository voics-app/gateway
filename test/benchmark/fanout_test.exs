defmodule DiscordyGateway.Benchmark.FanoutTest do
  @moduledoc """
  One-guild fan-out ceiling, measured without a network: `BENCH_RECEIVERS` sessions (default 50)
  in one guild receive `BENCH_MESSAGES` MESSAGE_CREATE events (default 6000) that a single process
  feeds through the same `Ingest` path the Kafka consumer uses. Prints the sustained rate and the
  consumer-to-socket delivery latency. Run with `mix test --include benchmark`; the figures are
  local to the machine that ran them, not cluster figures.
  """

  use DiscordyGateway.Case, async: false

  import DiscordyGateway.Benchmark

  alias DiscordyGateway.Fixtures

  @moduletag :benchmark
  @moduletag timeout: :infinity

  defp env(name, default), do: String.to_integer(System.get_env(name, "#{default}"))

  defp ms(micros), do: Float.round(micros / 1000, 1)

  # `rate` messages per second, or as fast as the consumer can go when 0.
  defp pace(_started, _n, 0), do: :ok

  defp pace(started_ms, n, rate) do
    due = started_ms * 1000 + div(n * 1_000_000, rate)
    wait = div(due - System.monotonic_time(:microsecond), 1000)
    if wait > 0, do: Process.sleep(wait)
  end

  test "deliveries per second of one guild" do
    production_timers()
    use_valkey()
    receivers = env("BENCH_RECEIVERS", 50)
    messages = env("BENCH_MESSAGES", 6000)
    rate = env("BENCH_RATE", 0)
    slow = env("BENCH_SLOW", 0)
    guild = Fixtures.large_guild_payload("g1", receivers + slow)
    seed_users(guild, receivers + slow)

    clients = for n <- 1..receivers, do: connect("u#{n}")
    stalled = for n <- (receivers + 1)..(receivers + slow)//1, do: connect_slow("u#{n}")
    wait_until(fn -> Enum.all?(clients, &(counts(&1.sink).create == 1)) end, 60_000)
    wait_until(fn -> drained?(clients) end, 60_000)

    stages = if env("BENCH_STAGES", 0) == 1, do: collect_stages()
    memory_before = :erlang.memory(:total)
    started = System.monotonic_time(:millisecond)

    for n <- 1..messages do
      pace(started, n, rate)
      publish_message("g1", "c#{rem(n, 8) + 1}", n)
    end

    published = System.monotonic_time(:millisecond) - started
    wait_until(fn -> delivered(clients) >= messages * receivers end, 300_000)
    elapsed = System.monotonic_time(:millisecond) - started

    latencies = latencies(clients)
    memory_delta = :erlang.memory(:total) - memory_before
    cut = Enum.count(stalled, &(not Process.alive?(&1.sink)))

    stage_lines =
      if stages,
        do: Enum.join(stage_report(stages), "
      "),
        else: "off (BENCH_STAGES=1 turns it on; the handler itself slows the run)"

    IO.puts("""

    [benchmark] #{messages} messages to #{receivers} receivers in one guild (local figures, no network)
      consumer finished publishing after: #{published} ms (#{round(messages * 1000 / max(published, 1))} msg/s)
      all deliveries done after:          #{elapsed} ms
      sustained:                          #{round(messages * 1000 / elapsed)} msg/s, #{round(messages * receivers * 1000 / elapsed)} deliveries/s
      consumer-to-socket latency p50 / p95 / p99: #{ms(percentile(latencies, 0.5))} / #{ms(percentile(latencies, 0.95))} / #{ms(percentile(latencies, 0.99))} ms
      stalled readers (never read their socket): #{slow}, cut off by the session: #{cut}
      BEAM memory growth during the run:  #{div(memory_delta, 1024)} KiB
      stages (mean per call, max):
      #{stage_lines}
    """)

    assert delivered(clients) == messages * receivers
  end
end
