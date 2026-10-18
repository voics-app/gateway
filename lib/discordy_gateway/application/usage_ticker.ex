defmodule DiscordyGateway.Application.UsageTicker do
  @moduledoc "Publishes a USAGE_TICK for this node to the telemetry topic every minute."

  use GenServer

  alias DiscordyGateway.Application.{Ids, Ports, Stats}
  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Events.EventEnvelope

  @default_interval_ms 60_000

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "Publishes a tick immediately."
  @spec tick_now(GenServer.server()) :: :ok | {:error, term()}
  def tick_now(server \\ __MODULE__), do: GenServer.call(server, :tick)

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    schedule(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_call(:tick, _from, state), do: {:reply, publish(), state}

  @impl true
  def handle_info(:tick, state) do
    publish()
    schedule(state.interval)
    {:noreply, state}
  end

  defp publish do
    node = Atom.to_string(node())
    stats = Stats.snapshot()

    data = %{
      "node" => node,
      "sessions_online" => Map.get(stats["sessions_by_node"], node, 0),
      "guild_processes" => stats["guild_processes"],
      "voice_allocations" => stats["voice_allocations"]
    }

    envelope =
      EventEnvelope.new("USAGE_TICK", data,
        id: Ids.uuid_v7(),
        occurred_at: DateTime.to_iso8601(DateTime.utc_now())
      )

    Ports.event_publisher().publish(
      Contracts.topic(:telemetry),
      node,
      EventEnvelope.to_map(envelope)
    )
  end

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)
end
