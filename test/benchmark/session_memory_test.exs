defmodule DiscordyGateway.Benchmark.SessionMemoryTest do
  @moduledoc """
  Memory held by sessions that joined a large guild. Run with `mix test --include benchmark`;
  `BENCH_SESSIONS` (default 300) and `BENCH_GUILD_MEMBERS` (default 200) size it. The figures are
  local to the machine that ran them, not cluster figures.
  """

  use DiscordyGateway.Case, async: false

  import DiscordyGateway.Benchmark

  alias DiscordyGateway.Fixtures

  @moduletag :benchmark
  @moduletag timeout: :infinity

  defp env(name, default), do: String.to_integer(System.get_env(name, "#{default}"))

  defp memory(kind), do: :erlang.memory(kind)

  defp session_stats(clients) do
    infos =
      for %{session: pid} <- clients,
          info = Process.info(pid, [:memory, :heap_size, :total_heap_size, :message_queue_len]),
          do: info

    count = length(infos)
    avg = fn key -> Enum.sum(Enum.map(infos, & &1[key])) / count end
    %{memory: avg.(:memory), heap_words: avg.(:total_heap_size), queue: avg.(:message_queue_len)}
  end

  defp breakdown(before, sessions) do
    [:processes, :binary, :ets, :system]
    |> Enum.map_join(", ", fn kind ->
      "#{kind} #{kib((memory(kind) - before[kind]) / sessions)} KiB"
    end)
  end

  defp kib(bytes), do: Float.round(bytes / 1024, 1)

  test "memory per session in a guild of BENCH_GUILD_MEMBERS members" do
    production_timers()
    use_valkey()
    sessions = env("BENCH_SESSIONS", 300)
    members = env("BENCH_GUILD_MEMBERS", 200)
    guild = Fixtures.large_guild_payload("g1", members)
    seed_users(guild, members)

    :erlang.garbage_collect()
    before = Map.new([:total, :processes, :binary, :ets, :system], &{&1, memory(&1)})
    before_total = before.total

    clients = for n <- 1..sessions, do: connect("u#{rem(n - 1, members) + 1}")
    started = System.monotonic_time(:millisecond)
    wait_until(fn -> Enum.all?(clients, &(counts(&1.sink).create == 1)) end, 120_000)
    wait_until(fn -> drained?(clients) end, 300_000)
    settle_ms = System.monotonic_time(:millisecond) - started

    as_is = session_stats(clients)
    total_as_is = memory(:total) - before_total
    binary_as_is = memory(:binary)
    Enum.each(clients, &(:erlang.garbage_collect(&1.session) && :erlang.garbage_collect(&1.sink)))
    retained = memory(:total) - before_total
    after_gc = session_stats(clients)

    IO.puts("""

    [benchmark] #{sessions} sessions in a #{members}-member guild (local figures)
      session process memory, as is:    #{kib(as_is.memory)} KiB avg, heap #{round(as_is.heap_words)} words
      session process memory, after GC: #{kib(after_gc.memory)} KiB avg
      BEAM total delta per session:     #{kib(total_as_is / sessions)} KiB
      BEAM total delta after GC:        #{kib(retained / sessions)} KiB per session
      after GC, per session: #{breakdown(before, sessions)}
      BEAM binary memory, as is:        #{kib(binary_as_is)} KiB total
      connect storm settled in:         #{settle_ms} ms
    """)

    assert as_is.memory < 64 * 1024
  end
end
