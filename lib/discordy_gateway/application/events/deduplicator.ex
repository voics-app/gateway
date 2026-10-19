defmodule DiscordyGateway.Application.Events.Deduplicator do
  @moduledoc "Node-wide idempotency filter: remembers the last N event ids (P6) in a `DedupLru`."

  use GenServer

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Events.DedupLru

  def start_link(opts) do
    capacity = Keyword.get(opts, :capacity, Contracts.dedup_capacity())
    GenServer.start_link(__MODULE__, capacity, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Marks `event_id` as seen and reports whether this is the first time."
  @spec check(String.t(), GenServer.server()) :: :new | :duplicate
  def check(event_id, server \\ __MODULE__), do: GenServer.call(server, {:check, event_id})

  @impl true
  def init(capacity), do: {:ok, DedupLru.new(capacity)}

  @impl true
  def handle_call({:check, event_id}, _from, lru) do
    {result, lru} = DedupLru.check_and_put(lru, event_id)
    {:reply, result, lru}
  end
end
