defmodule DiscordyGateway.Infrastructure.Valkey.ResumeWriter do
  @moduledoc """
  Batched, asynchronous writes of the resume buffer. A session used to wait for its own Valkey
  round trip on every dispatch, which put a network round trip into the serial path of each
  session and let its mailbox grow whenever Valkey slowed down. Now `append/2` is a cast: a small
  pool of writer processes collects the entries of all sessions that arrived while the previous
  batch was in flight and sends them as one pipeline (one RPUSH, LTRIM and EXPIRE per session).
  Entries of one session always go through the same writer, so their order is kept.

  `flush/1` waits until everything appended so far for a session is in Valkey; a resume reads
  the buffer only after it. A writer whose mailbox is over `@max_pending` drops new entries (the
  resume buffer is best effort: a client that cannot resume re-identifies) and counts them.
  """

  use GenServer

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Infrastructure.Valkey.Pool

  require Logger

  @size 4
  @ttl_seconds 300
  @max_batch 500
  @max_pending 50_000
  @names List.to_tuple(for index <- 0..(@size - 1), do: :"valkey_resume_writer_#{index}")

  @doc "Child specs of the writer pool, to be supervised next to the Valkey connections."
  @spec child_specs() :: [Supervisor.child_spec()]
  def child_specs do
    for index <- 0..(@size - 1),
        do: Supervisor.child_spec({__MODULE__, index}, id: {__MODULE__, index})
  end

  def start_link(index), do: GenServer.start_link(__MODULE__, :ok, name: elem(@names, index))

  @doc "Queues entries (`{seq, frame}`) of a session for the next batch."
  @spec append(String.t(), [{non_neg_integer(), binary()}]) :: :ok
  def append(session_id, entries),
    do: GenServer.cast(writer(session_id), {:append, session_id, entries})

  @doc "Blocks until the entries appended so far for `session_id` have been written."
  @spec flush(String.t()) :: :ok
  def flush(session_id), do: GenServer.call(writer(session_id), :flush, 10_000)

  defp writer(session_id), do: elem(@names, :erlang.phash2(session_id, @size))

  @impl true
  def init(:ok), do: {:ok, %{pending: %{}, count: 0}}

  # Every clause returns a zero timeout, which fires only once the mailbox is empty: every entry
  # already waiting joins the batch before it is written (a full batch is written at once).
  @impl true
  def handle_cast({:append, session_id, entries}, state) do
    {:message_queue_len, waiting} = Process.info(self(), :message_queue_len)

    if waiting > @max_pending do
      :telemetry.execute([:discordy_gateway, :resume, :dropped], %{count: length(entries)})
      {:noreply, state, 0}
    else
      {:noreply, enqueue(state, session_id, entries), 0}
    end
  end

  @impl true
  def handle_call(:flush, _from, state), do: {:reply, :ok, write(state)}

  @impl true
  def handle_info(:timeout, state), do: {:noreply, write(state)}

  defp enqueue(state, session_id, entries) do
    pending = Map.update(state.pending, session_id, entries, &(&1 ++ entries))
    state = %{state | pending: pending, count: state.count + length(entries)}

    if state.count >= @max_batch, do: write(state), else: state
  end

  defp write(%{pending: pending} = state) when map_size(pending) == 0, do: state

  defp write(%{pending: pending}) do
    commands =
      Enum.flat_map(pending, fn {session_id, entries} ->
        key = Contracts.resume_key(session_id)
        values = for {seq, frame} <- entries, do: <<seq::64, frame::binary>>

        [
          ["RPUSH", key | values],
          ["LTRIM", key, -Contracts.resume_buffer_cap(), -1],
          ["EXPIRE", key, @ttl_seconds]
        ]
      end)

    case Pool.pipeline(commands) do
      {:ok, _results} -> :ok
      {:error, reason} -> Logger.warning("resume buffer write failed: #{inspect(reason)}")
    end

    %{pending: %{}, count: 0}
  end
end
