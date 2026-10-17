defmodule DiscordyGateway.Domain.Events.DedupLru do
  @moduledoc """
  Bounded least-recently-seen set of event ids, used to make consumers idempotent on the
  envelope `id` (P6: the last 10k ids per node).
  """

  @enforce_keys [:capacity]
  defstruct [:capacity, tick: 0, by_id: %{}, by_tick: :gb_trees.empty()]

  @type t :: %__MODULE__{}

  @spec new(pos_integer()) :: t()
  def new(capacity) when capacity > 0, do: %__MODULE__{capacity: capacity}

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{by_id: by_id}), do: map_size(by_id)

  @doc "Marks `id` as seen and reports whether it had been seen before."
  @spec check_and_put(t(), term()) :: {:new | :duplicate, t()}
  def check_and_put(%__MODULE__{} = lru, id) do
    case Map.fetch(lru.by_id, id) do
      {:ok, old_tick} -> {:duplicate, touch(lru, id, old_tick)}
      :error -> {:new, lru |> insert(id) |> evict()}
    end
  end

  defp touch(lru, id, old_tick) do
    tick = lru.tick + 1

    %{
      lru
      | tick: tick,
        by_id: Map.put(lru.by_id, id, tick),
        by_tick: :gb_trees.insert(tick, id, :gb_trees.delete(old_tick, lru.by_tick))
    }
  end

  defp insert(lru, id) do
    tick = lru.tick + 1
    %{lru | tick: tick, by_id: Map.put(lru.by_id, id, tick), by_tick: :gb_trees.insert(tick, id, lru.by_tick)}
  end

  defp evict(%__MODULE__{capacity: capacity, by_id: by_id} = lru) when map_size(by_id) > capacity do
    {_tick, id, by_tick} = :gb_trees.take_smallest(lru.by_tick)
    evict(%{lru | by_id: Map.delete(by_id, id), by_tick: by_tick})
  end

  defp evict(lru), do: lru
end
