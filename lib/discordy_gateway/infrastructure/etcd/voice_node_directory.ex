defmodule DiscordyGateway.Infrastructure.Etcd.VoiceNodeDirectory do
  @moduledoc """
  Live view of the voice nodes registered under `/discordy/voice/nodes/` in etcd (P8). The
  process loads the prefix, then follows a watch; nodes whose lease expires are deleted by etcd
  and drop out of the view. After any watch failure it reloads from scratch. Reads go to an ETS
  table, so they never block on etcd.
  """

  use GenServer

  @behaviour DiscordyGateway.Application.Ports.VoiceNodeDirectory

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Voice.VoiceNode

  require Logger

  @table :discordy_voice_nodes
  @connection :discordy_etcd
  @retry_ms 2_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Name of the etcd connection shared with the other etcd adapters."
  @spec connection() :: atom()
  def connection, do: @connection

  @doc "Whether the directory is connected to etcd and has loaded its initial view."
  @spec ready?() :: boolean()
  def ready?, do: GenServer.call(__MODULE__, :ready?)

  @impl DiscordyGateway.Application.Ports.VoiceNodeDirectory
  def list_nodes, do: @table |> :ets.tab2list() |> Enum.map(&elem(&1, 1))

  @impl DiscordyGateway.Application.Ports.VoiceNodeDirectory
  def get_node(node_id) do
    case :ets.select(@table, [{{:_, %{node_id: node_id}}, [], [:"$_"]}]) do
      [{_key, node} | _] -> {:ok, node}
      [] -> :error
    end
  end

  ## Server

  @impl GenServer
  def init(opts) do
    :ets.new(@table, [:named_table, :protected, :set, read_concurrency: true])
    state = %{endpoints: Keyword.fetch!(opts, :endpoints), watch: nil, ready: false}
    {:ok, state, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, state), do: {:noreply, sync(state)}

  @impl GenServer
  def handle_call(:ready?, _from, state), do: {:reply, state.ready, state}

  @impl GenServer
  def handle_info(:resync, state), do: {:noreply, sync(state)}

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
      %{state | watch: watch, ready: true}
    else
      {:error, reason} ->
        Logger.warning("etcd voice node sync failed: #{inspect(reason)}")
        schedule_resync(state)
    end
  end

  defp schedule_resync(state) do
    Process.send_after(self(), :resync, @retry_ms)
    %{state | watch: nil, ready: false}
  end

  defp ensure_connection(endpoints) do
    case :eetcd.open(@connection, endpoints) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp load do
    request =
      @connection |> :eetcd_kv.new() |> :eetcd_kv.with_key(prefix()) |> :eetcd_kv.with_prefix()

    case :eetcd_kv.get(request) do
      {:ok, %{kvs: kvs, header: %{revision: revision}}} ->
        :ets.delete_all_objects(@table)
        Enum.each(kvs, &put_kv/1)
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
    Enum.each(events, fn
      %{type: :DELETE, kv: %{key: key}} -> :ets.delete(@table, key)
      %{kv: kv} -> put_kv(kv)
    end)
  end

  defp put_kv(%{key: key, value: value}) do
    with {:ok, raw} <- Jason.decode(value),
         {:ok, node} <- VoiceNode.parse(raw) do
      :ets.insert(@table, {key, node})
    else
      _ -> Logger.warning("ignoring malformed voice node entry #{key}")
    end
  end

  defp prefix, do: Contracts.voice_nodes_prefix()
end
