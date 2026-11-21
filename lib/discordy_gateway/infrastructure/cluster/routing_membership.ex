defmodule DiscordyGateway.Infrastructure.Cluster.RoutingMembership do
  @moduledoc "Requires every DNS-discovered gateway to participate in the routing cluster."
  use GenServer

  @scope DiscordyGateway.Pg
  @group :routing_nodes
  @table :discordy_gateway_routing_membership
  @poll_ms 1_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec ready?() :: boolean()
  def ready? do
    case :ets.lookup(@table, :snapshot) do
      [{:snapshot, [_ | _] = expected, synchronized, deadline}] ->
        System.monotonic_time(:millisecond) < deadline and complete?(expected, synchronized)

      _ ->
        false
    end
  rescue
    ArgumentError -> false
  end

  @impl true
  def init(opts) do
    :ok = :pg.join(@scope, @group, self())
    :ets.new(@table, [:named_table, :protected, :set, read_concurrency: true])
    {:ok, opts, {:continue, :poll}}
  end

  @impl true
  def handle_continue(:poll, opts), do: {:noreply, poll(opts)}

  @impl true
  def handle_info(:poll, opts), do: {:noreply, poll(opts)}

  defp poll(opts) do
    resolver = Keyword.get(opts, :resolver, &resolve/1)
    expected = resolver.(Keyword.fetch!(opts, :query))
    synchronized = synchronized_nodes()
    deadline = System.monotonic_time(:millisecond) + 2 * @poll_ms
    :ets.insert(@table, {:snapshot, expected, synchronized, deadline})
    :telemetry.execute([:discordy_gateway, :cluster], %{ready: if(ready?(), do: 1, else: 0)})
    Process.send_after(self(), :poll, @poll_ms)
    opts
  end

  defp complete?(expected, synchronized) do
    visible = Enum.map([node() | Node.list()], &Atom.to_string/1)

    Node.alive?() and Atom.to_string(node()) in expected and
      Enum.all?(expected, &(&1 in visible and &1 in synchronized))
  end

  # An older replica has the same :pg scope but no readiness process. Seeing our marker there
  # verifies distribution and directory synchronization without deadlocking rolling upgrades.
  defp synchronized_nodes do
    peers = Node.list()
    replies = :erpc.multicall(peers, :pg, :get_members, [@scope, @group], @poll_ms)

    ready = for {peer, {:ok, members}} <- Enum.zip(peers, replies), self() in members, do: peer
    Enum.map([node() | ready], &Atom.to_string/1)
  end

  defp resolve(query) do
    case :inet.getaddrs(String.to_charlist(query), :inet) do
      {:ok, ips} -> Enum.map(ips, &("gateway@" <> to_string(:inet.ntoa(&1))))
      {:error, _reason} -> []
    end
  end
end
