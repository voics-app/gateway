defmodule DiscordyGateway.Infrastructure.Etcd.VoiceNodeDirectory do
  @moduledoc """
  Live view of the voice nodes registered under `/discordy/voice/nodes/` in etcd (P8). The
  process loads the prefix, then follows a watch; nodes whose lease expires are deleted by etcd
  and drop out of the view. After any watch failure it reloads from scratch. Reads go to an ETS
  table, so they never block on etcd.
  """

  use GenServer

  @behaviour DiscordyGateway.Application.Ports.VoiceNodeDirectory

  alias DiscordyGateway.Domain.Voice.Discovery

  require Logger

  @table :discordy_voice_nodes
  @connection :discordy_etcd
  @retry_ms 2_000
  @freshness_ms 2_000
  @poll_ms 1_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Name of the etcd connection shared with the other etcd adapters."
  @spec connection() :: atom()
  def connection, do: @connection

  @doc "Whether the directory is connected to etcd and has loaded its initial view."
  @spec ready?() :: boolean()
  def ready?, do: GenServer.call(__MODULE__, :ready?)

  @impl DiscordyGateway.Application.Ports.VoiceNodeDirectory
  def list_nodes do
    [{:view, records, _revision, ready, deadline}] = :ets.lookup(@table, :view)
    Discovery.nodes(records, ready and now() < deadline)
  end

  @impl DiscordyGateway.Application.Ports.VoiceNodeDirectory
  def get_node(node_id) do
    case Enum.find(list_nodes(), &(&1.node_id == node_id)) do
      nil -> :error
      node -> {:ok, node}
    end
  end

  ## Server

  @impl GenServer
  def init(opts) do
    :ets.new(@table, [:named_table, :protected, :set, read_concurrency: true])
    :ets.insert(@table, {:view, %{}, 0, false, 0})
    Process.send_after(self(), :refresh_snapshot, @poll_ms)
    state = %{endpoints: Keyword.fetch!(opts, :endpoints), watch: nil, ready: false}
    {:ok, state, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, state), do: {:noreply, sync(state)}

  @impl GenServer
  def handle_call(:ready?, _from, state), do: {:reply, state.ready, state}

  @impl GenServer
  def handle_info(:resync, state), do: {:noreply, sync(state)}

  def handle_info(:refresh_snapshot, state) do
    # A silent network partition may leave a watch open indefinitely. Linearizable snapshots
    # and a monotonic read deadline prevent a stale healthy result from authorizing new joins.
    ready = refresh_snapshot(state.watch != nil)
    Process.send_after(self(), :refresh_snapshot, @poll_ms)
    {:noreply, %{state | ready: ready}}
  end

  def handle_info(message, %{watch: watch} = state) when watch != nil do
    case :eetcd_watch.watch_stream(watch, message) do
      {:ok, watch, response} ->
        apply_events(Map.get(response, :events, []))
        {:noreply, %{state | watch: watch}}

      {:more, watch} ->
        {:noreply, %{state | watch: watch}}

      {:error, reason} ->
        Logger.warning("etcd watch failed: #{inspect(reason)}")
        {:noreply, schedule_resync(state)}

      unknown ->
        _ = unknown
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  ## Loading and watching

  defp sync(state) do
    with :ok <- ensure_connection(state.endpoints),
         {:ok, revision} <- load(),
         {:ok, watch} <- start_watch(revision) do
      set_ready(true)
      %{state | watch: watch, ready: true}
    else
      {:error, reason} ->
        Logger.warning("etcd voice node sync failed: #{inspect(reason)}")
        schedule_resync(state)
    end
  catch
    # A crash here would restart this process while the eetcd connection (owned by eetcd's own
    # supervisor, not by us) survives; etcd trouble must never take the process down.
    kind, reason ->
      Logger.warning("etcd voice node sync crashed: #{Exception.format_banner(kind, reason)}")
      schedule_resync(state)
  end

  defp schedule_resync(state) do
    set_ready(false)
    Process.send_after(self(), :resync, @retry_ms)
    %{state | watch: nil, ready: false}
  end

  # The eetcd connection outlives this process and reconnects on its own, so a second `open/2`
  # under the same name fails with `:already_started`. Reuse the registered connection.
  defp ensure_connection(endpoints) do
    case :eetcd_conn.lookup(@connection) do
      {:ok, _pid} -> :ok
      {:error, _unavailable} -> open_connection(endpoints)
    end
  end

  defp open_connection(endpoints) do
    case :eetcd.open(@connection, endpoints) do
      {:ok, _pid} -> :ok
      {:error, reason} -> if already_started?(reason), do: :ok, else: {:error, reason}
    end
  end

  # `connect_all` reports failures as `[{host, reason}]`, other modes as a bare reason.
  defp already_started?(:already_started), do: true
  defp already_started?({:already_started, _pid}), do: true

  defp already_started?(failures) when is_list(failures),
    do: Enum.any?(failures, &match?({_host, :already_started}, &1))

  defp already_started?(_reason), do: false

  defp load do
    deadline = now() + @freshness_ms

    request =
      @connection
      |> :eetcd_kv.new()
      |> :eetcd_kv.with_timeout(750)
      |> :eetcd_kv.with_key(prefix())
      |> :eetcd_kv.with_prefix()

    case :eetcd_kv.get(request) do
      {:ok, %{kvs: kvs, header: %{revision: revision}}} ->
        records = Map.new(kvs, &{&1.key, &1})
        :ets.insert(@table, {:view, records, revision, false, deadline})
        {:ok, revision}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp start_watch(revision) do
    request =
      :eetcd_watch.new()
      |> :eetcd_watch.with_key(prefix())
      |> :eetcd_watch.with_prefix()
      |> :eetcd_watch.with_start_revision(revision + 1)

    case :eetcd_watch.watch(@connection, request) do
      {:ok, watch, _id} -> {:ok, watch}
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_events(events) do
    [{:view, records, revision, ready, deadline}] = :ets.lookup(@table, :view)

    # Watch responses already in the mailbox may precede the latest snapshot revision.
    {records, latest} =
      events
      |> Enum.filter(&(&1.kv.mod_revision > revision))
      |> Enum.reduce({records, revision}, fn event, {acc, latest} ->
        kv = event.kv

        acc =
          if event[:type] == :DELETE, do: Map.delete(acc, kv.key), else: Map.put(acc, kv.key, kv)

        {acc, max(latest, kv.mod_revision)}
      end)

    :ets.insert(@table, {:view, records, latest, ready, deadline})
  end

  defp refresh_snapshot(watching?) do
    case load() do
      {:ok, _revision} -> set_ready(watching?)
      {:error, _reason} -> set_ready(false)
    end
  catch
    _kind, _reason -> set_ready(false)
  end

  defp set_ready(ready) do
    :ets.update_element(@table, :view, {4, ready})
    ready
  end

  defp now, do: System.monotonic_time(:millisecond)
  defp prefix, do: "/discordy/voice/"
end
