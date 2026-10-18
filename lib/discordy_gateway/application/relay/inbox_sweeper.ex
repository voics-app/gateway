defmodule DiscordyGateway.Application.Relay.InboxSweeper do
  @moduledoc "Deletes expired E2EE inbox entries once a minute (P7)."

  use GenServer

  alias DiscordyGateway.Application.Ports

  require Logger

  @batch 1_000
  @default_interval_ms 60_000

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "Sweeps now and returns the number of deleted entries."
  @spec sweep_now(GenServer.server()) :: non_neg_integer()
  def sweep_now(server \\ __MODULE__), do: GenServer.call(server, :sweep, 30_000)

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    schedule(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_call(:sweep, _from, state), do: {:reply, sweep(0), state}

  @impl true
  def handle_info(:tick, state) do
    sweep(0)
    schedule(state.interval)
    {:noreply, state}
  end

  defp sweep(total) do
    case Ports.inbox_store().sweep_expired(System.os_time(:millisecond), @batch) do
      {:ok, @batch} -> sweep(total + @batch)
      {:ok, count} -> total + count
      {:error, reason} -> log_failure(reason, total)
    end
  end

  defp log_failure(reason, total) do
    Logger.warning("e2ee inbox sweep failed: #{inspect(reason)}")
    total
  end

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)
end
