defmodule DiscordyGateway.Benchmark do
  @moduledoc """
  Helpers of the local capacity benchmarks (`mix test --include benchmark`, see README
  "Benchmarks"). A sink stands in for the WebSocket process of a session: it counts the frames the
  session hands over and records the send-to-receive latency of MESSAGE_CREATE frames. It sweeps
  its heap on every collection, as the socket process does through the `fullsweep_after` upgrade
  option, so frames it has written do not stay pinned in its old heap.
  """

  alias DiscordyGateway.Application.Events.Ingest
  alias DiscordyGateway.Application.Session
  alias DiscordyGateway.Fakes

  @doc "Starts a session whose transport is a sink and identifies it as `user_id`."
  @spec connect(String.t()) :: %{session: pid(), sink: pid()}
  def connect(user_id) do
    sink =
      :erlang.spawn_opt(
        fn -> sink_loop(%{create: 0, message: 0, latencies: []}) end,
        [:link, fullsweep_after: 0]
      )

    {:ok, session} = Session.start(sink)
    Session.frame(session, %{"op" => 2, "d" => %{"token" => "user-" <> user_id, "intents" => 0}})
    %{session: session, sink: sink}
  end

  @doc """
  Replaces the short timers of the test configuration with production-like ones for the running
  test, so idle sinks that never send heartbeats are not timed out. Restored on exit.
  """
  @spec production_timers() :: :ok
  def production_timers do
    previous = Application.fetch_env!(:discordy_gateway, :settings)

    Application.put_env(
      :discordy_gateway,
      :settings,
      Keyword.merge(previous,
        heartbeat_interval_ms: 600_000,
        identify_timeout_ms: 600_000,
        resume_window_ms: 600_000,
        guild_idle_ms: 600_000
      )
    )

    ExUnit.Callbacks.on_exit(fn ->
      Application.put_env(:discordy_gateway, :settings, previous)
    end)
  end

  @doc """
  Binds the resume store to the real Valkey adapter at `BENCH_VALKEY_URL` for the running test, so
  the cost of the per-dispatch resume-buffer append is part of the measurement.
  """
  @spec use_valkey() :: :ok
  def use_valkey do
    url = System.fetch_env!("BENCH_VALKEY_URL")
    ports = Application.fetch_env!(:discordy_gateway, :ports)

    Application.put_env(
      :discordy_gateway,
      :ports,
      Keyword.put(ports, :resume_store, DiscordyGateway.Infrastructure.Valkey.ResumeStore)
    )

    ExUnit.Callbacks.start_supervised!({DiscordyGateway.Infrastructure.Valkey.Pool, url: url})
    ExUnit.Callbacks.on_exit(fn -> Application.put_env(:discordy_gateway, :ports, ports) end)
    :ok
  end

  @doc "Registers the api responses for `count` users that are members of `guild`."
  @spec seed_users(map(), pos_integer()) :: :ok
  def seed_users(guild, count) do
    Fakes.put({:guild, guild["id"]}, {:ok, guild})

    for n <- 1..count do
      Fakes.put(
        {:ready, "u#{n}"},
        {:ok,
         %{
           "user" => %{"id" => "u#{n}"},
           "guilds" => [%{"id" => guild["id"]}],
           "private_channels" => [],
           "relationships" => [],
           "user_settings" => %{}
         }}
      )
    end

    :ok
  end

  @doc "Counters of one sink: GUILD_CREATE frames and MESSAGE_CREATE frames received."
  @spec counts(pid()) :: %{create: non_neg_integer(), message: non_neg_integer()}
  def counts(sink) do
    send(sink, {:counts, self()})

    receive do
      {:counts, counts} -> counts
    after
      5_000 -> raise "sink did not answer"
    end
  end

  @doc "Total MESSAGE_CREATE frames received across `clients`."
  @spec delivered([map()]) :: non_neg_integer()
  def delivered(clients), do: clients |> Enum.map(&counts(&1.sink).message) |> Enum.sum()

  @doc "Sorted delivery latencies in microseconds recorded by every sink."
  @spec latencies([map()]) :: [integer()]
  def latencies(clients) do
    for %{sink: sink} <- clients, reduce: [] do
      acc ->
        send(sink, {:latencies, self()})

        receive do
          {:latencies, list} -> list ++ acc
        after
          5_000 -> raise "sink did not answer"
        end
    end
    |> Enum.sort()
  end

  @doc "The `fraction` percentile of a sorted list."
  @spec percentile([integer()], float()) :: integer()
  def percentile(sorted, fraction), do: Enum.at(sorted, trunc((length(sorted) - 1) * fraction))

  @doc "Publishes one MESSAGE_CREATE envelope on the message topic, as the consumer would."
  @spec publish_message(String.t(), String.t(), non_neg_integer()) :: :ok
  def publish_message(guild_id, channel_id, n) do
    envelope = %{
      "id" => "bench-#{n}",
      "type" => "MESSAGE_CREATE",
      "v" => 1,
      "guild_id" => guild_id,
      "channel_id" => channel_id,
      "occurred_at" => "2026-10-18T00:00:00Z",
      "data" => %{
        "id" => "m#{n}",
        "channel_id" => channel_id,
        "content" => "t=#{System.monotonic_time(:microsecond)} " <> String.duplicate("x", 120),
        "author" => %{"id" => "u1", "username" => "member-1"},
        "timestamp" => "2026-10-18T00:00:00Z",
        "mentions" => [],
        "attachments" => [],
        "embeds" => []
      }
    }

    Ingest.handle(:message, Jason.encode!(envelope))
  end

  @doc "Whether every session of `clients` has an empty mailbox."
  @spec drained?([map()]) :: boolean()
  def drained?(clients) do
    Enum.all?(clients, fn %{session: pid} ->
      match?({:message_queue_len, 0}, Process.info(pid, :message_queue_len))
    end)
  end

  @doc "Polls `fun` until it returns true; raises after `timeout_ms`."
  @spec wait_until((-> boolean()), non_neg_integer()) :: :ok
  def wait_until(fun, timeout_ms) do
    cond do
      fun.() ->
        :ok

      timeout_ms <= 0 ->
        raise "condition not reached"

      true ->
        Process.sleep(20)
        wait_until(fun, timeout_ms - 20)
    end
  end

  defp sink_loop(state) do
    receive do
      {:gw_frame, frame} ->
        sink_loop(record(state, IO.iodata_to_binary(frame)))

      {:counts, from} ->
        send(from, {:counts, Map.take(state, [:create, :message])})
        sink_loop(state)

      {:latencies, from} ->
        send(from, {:latencies, state.latencies})
        sink_loop(state)

      _other ->
        sink_loop(state)
    end
  end

  defp record(state, frame) do
    cond do
      :binary.match(frame, ~s("t":"GUILD_CREATE")) != :nomatch ->
        %{state | create: state.create + 1}

      :binary.match(frame, ~s("t":"MESSAGE_CREATE")) != :nomatch ->
        %{state | message: state.message + 1, latencies: [latency(frame) | state.latencies]}

      true ->
        state
    end
  end

  defp latency(frame) do
    [_, sent] = Regex.run(~r/"content":"t=(\d+) /, frame)
    System.monotonic_time(:microsecond) - String.to_integer(sent)
  end
end
