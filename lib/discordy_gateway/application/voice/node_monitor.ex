defmodule DiscordyGateway.Application.Voice.NodeMonitor do
  @moduledoc """
  Watches the voice node directory. When a node disappears from etcd, every guild and DM call
  process on this gateway node is told to reallocate (P8 step 5).
  """

  use GenServer

  alias DiscordyGateway.Application.Ports

  @default_interval_ms 2_000

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "Runs a check immediately (used by tests and after etcd resyncs)."
  @spec check_now(GenServer.server()) :: :ok
  def check_now(server \\ __MODULE__), do: GenServer.call(server, :check)

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    state = %{interval: interval, known: node_ids()}
    schedule(interval)
    {:ok, state}
  end

  @impl true
  def handle_call(:check, _from, state), do: {:reply, :ok, check(state)}

  @impl true
  def handle_info(:tick, state) do
    schedule(state.interval)
    {:noreply, check(state)}
  end

  defp check(%{known: known} = state) do
    current = node_ids()

    known
    |> MapSet.difference(current)
    |> Enum.each(&notify_gone/1)

    %{state | known: current}
  end

  defp notify_gone(node_id) do
    for pid <- :pg.get_local_members(DiscordyGateway.Pg, :voice_servers),
        do: send(pid, {:voice_node_gone, node_id})
  end

  defp node_ids, do: Ports.voice_nodes().list_nodes() |> MapSet.new(& &1.node_id)

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)
end
