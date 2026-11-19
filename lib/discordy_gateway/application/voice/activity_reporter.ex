defmodule DiscordyGateway.Application.Voice.ActivityReporter do
  @moduledoc """
  Bounded, ordered retry queue for voice checkpoints. Accepted checkpoints are
  durable in the API. Queue loss on gateway failure is possible and is why the
  API reports voice coverage as incomplete.
  """
  use GenServer
  require Logger
  alias DiscordyGateway.Application.Ports

  @limit 5_000
  @retry_ms 2_000

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  def submit(checkpoint), do: GenServer.cast(__MODULE__, {:submit, checkpoint})

  @impl true
  def init(_opts), do: {:ok, %{queue: :queue.new(), busy: false}}

  @impl true
  def handle_cast({:submit, checkpoint}, state) do
    if :queue.len(state.queue) >= @limit do
      Logger.warning("voice activity checkpoint queue full; coverage is incomplete")
      {:noreply, state}
    else
      send(self(), :flush)
      {:noreply, %{state | queue: :queue.in(checkpoint, state.queue)}}
    end
  end

  @impl true
  def handle_info(:flush, %{busy: true} = state), do: {:noreply, state}

  def handle_info(:flush, state) do
    case :queue.peek(state.queue) do
      :empty ->
        {:noreply, state}

      {:value, checkpoint} ->
        owner = self()

        Task.start(fn ->
          result =
            try do
              Ports.api_client().voice_activity_checkpoint(checkpoint)
            rescue
              _ -> {:error, :exception}
            catch
              :exit, _ -> {:error, :exit}
            end

          send(owner, {:delivered, result})
        end)

        {:noreply, %{state | busy: true}}
    end
  end

  def handle_info({:delivered, :ok}, state) do
    {{:value, _}, queue} = :queue.out(state.queue)
    send(self(), :flush)
    {:noreply, %{state | queue: queue, busy: false}}
  end

  def handle_info({:delivered, {:error, reason}}, state)
      when reason in [:bad_request, :not_found] do
    Logger.warning("voice activity checkpoint rejected permanently; coverage is incomplete")
    {{:value, _}, queue} = :queue.out(state.queue)
    send(self(), :flush)
    {:noreply, %{state | queue: queue, busy: false}}
  end

  def handle_info({:delivered, {:error, _}}, state) do
    Process.send_after(self(), :flush, @retry_ms)
    {:noreply, %{state | busy: false}}
  end
end
