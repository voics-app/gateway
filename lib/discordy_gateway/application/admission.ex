defmodule DiscordyGateway.Application.Admission do
  @moduledoc """
  Admission control: under memory pressure the gateway refuses new connections with the retryable
  close code `SERVER_OVERLOADED` (4015) instead of letting the container run into its limit and be
  killed, which would drop every connected user. The memory probe is sampled once a second; the
  gate state lives in an `:atomics` cell so `admit?/0` is a lock-free read on the connection path.
  """

  use GenServer

  alias DiscordyGateway.Application.{Ports, Settings}
  alias DiscordyGateway.Domain.Admission

  @cell :discordy_gateway_admission

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether a new connection is accepted. Counts a refusal in the telemetry when it is not."
  @spec admit?() :: boolean()
  def admit? do
    open? = :atomics.get(:persistent_term.get(@cell), 1) == 0

    unless open?, do: :telemetry.execute([:discordy_gateway, :admission, :refused], %{count: 1})
    open?
  end

  @doc "Samples the probe now and updates the gate. Runs on a timer; exposed for tests."
  @spec sample() :: :ok
  def sample, do: GenServer.call(__MODULE__, :sample)

  @impl true
  def init(_opts) do
    cell = :atomics.new(1, signed: false)
    :persistent_term.put(@cell, cell)
    {:ok, tick(%{cell: cell, gate: :open})}
  end

  @impl true
  def handle_call(:sample, _from, state), do: {:reply, :ok, sample_now(state)}

  @impl true
  def handle_info(:tick, state), do: {:noreply, tick(state)}

  defp tick(state) do
    Process.send_after(self(), :tick, Settings.admission_interval_ms())
    sample_now(state)
  end

  defp sample_now(%{cell: cell, gate: gate} = state) do
    used = Ports.memory_probe().used_bytes()
    limit = Ports.memory_probe().limit_bytes()
    next = Admission.decide(gate, used, limit, Settings.admission_memory_fraction())
    :atomics.put(cell, 1, if(next == :open, do: 0, else: 1))

    :telemetry.execute(
      [:discordy_gateway, :admission, :sample],
      %{
        memory_bytes: used,
        memory_limit_bytes: limit || 0,
        closed: if(next == :open, do: 0, else: 1)
      }
    )

    %{state | gate: next}
  end
end
