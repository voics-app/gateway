defmodule DiscordyGateway.Domain.Events.DedupLruTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Events.DedupLru

  test "first sighting is new, the second is a duplicate" do
    lru = DedupLru.new(3)
    assert {:new, lru} = DedupLru.check_and_put(lru, "a")
    assert {:duplicate, _} = DedupLru.check_and_put(lru, "a")
  end

  test "evicts the least recently seen id beyond capacity" do
    lru = DedupLru.new(2)
    {:new, lru} = DedupLru.check_and_put(lru, "a")
    {:new, lru} = DedupLru.check_and_put(lru, "b")
    {:new, lru} = DedupLru.check_and_put(lru, "c")
    assert DedupLru.size(lru) == 2
    assert {:new, lru} = DedupLru.check_and_put(lru, "a")
    assert {:duplicate, _} = DedupLru.check_and_put(lru, "c")
  end

  test "a duplicate refreshes recency" do
    lru = DedupLru.new(2)
    {:new, lru} = DedupLru.check_and_put(lru, "a")
    {:new, lru} = DedupLru.check_and_put(lru, "b")
    {:duplicate, lru} = DedupLru.check_and_put(lru, "a")
    {:new, lru} = DedupLru.check_and_put(lru, "c")
    assert {:duplicate, _} = DedupLru.check_and_put(lru, "a")
  end
end
