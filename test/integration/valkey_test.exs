defmodule DiscordyGateway.Integration.ValkeyTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias DiscordyGateway.Containers

  alias DiscordyGateway.Infrastructure.Valkey.{
    InboxStore,
    Pool,
    PresenceStore,
    ResumeStore,
    VoiceAllocationStore
  }

  setup_all do
    {_name, port} =
      Containers.start("valkey/valkey:8-alpine", 6379, [], "Ready to accept connections")

    start_supervised!({Pool, url: "redis://127.0.0.1:#{port}"})
    assert Pool.ready?()
    :ok
  end

  describe "ResumeStore" do
    test "keeps ordered entries and trims to the last 1000" do
      id = "s-#{System.unique_integer([:positive])}"
      assert :ok = ResumeStore.append(id, [{1, "a"}, {2, "b"}])
      assert {:ok, [{1, "a"}, {2, "b"}]} = ResumeStore.fetch(id)

      assert :ok = ResumeStore.append(id, for(n <- 3..1100, do: {n, "f#{n}"}))
      assert {:ok, entries} = ResumeStore.fetch(id)
      assert length(entries) == 1000
      assert {101, "f101"} = hd(entries)

      assert :ok = ResumeStore.delete(id)
      assert {:ok, []} = ResumeStore.fetch(id)
    end
  end

  describe "InboxStore" do
    test "stores, lists, acks one entry and sweeps expired ones" do
      device = "dev-#{System.unique_integer([:positive])}"
      now = System.os_time(:millisecond)
      assert :ok = InboxStore.store(device, "m1", ~s({"n":1}), now + 60_000)
      assert :ok = InboxStore.store(device, "m2", ~s({"n":2}), now - 1)
      assert :ok = InboxStore.store(device, "m3", ~s({"n":3}), now + 60_000)

      assert {:ok, [{"m1", _}, {"m2", _}, {"m3", _}]} = InboxStore.list(device)

      assert :ok = InboxStore.ack(device, ["m1"])
      assert {:ok, [{"m2", _}, {"m3", _}]} = InboxStore.list(device)

      assert {:ok, 1} = InboxStore.sweep_expired(now, 100)
      assert {:ok, [{"m3", _}]} = InboxStore.list(device)
    end
  end

  describe "PresenceStore" do
    test "tracks sessions, detects aggregate changes, and reads aggregates back" do
      user = "u-#{System.unique_integer([:positive])}"
      presence = %{status: "online", activities: [], afk: false, platform: "desktop"}

      assert {:ok, [%{status: "online", platform: "desktop"}]} =
               PresenceStore.put(user, "s1", presence)

      assert {:ok, [_, _]} = PresenceStore.put(user, "s2", %{presence | platform: "mobile"})
      assert {:ok, [_]} = PresenceStore.remove(user, "s1")

      aggregate = %{status: "online", activities: [], client_status: %{"desktop" => "online"}}
      assert :changed = PresenceStore.swap_aggregate(user, aggregate)
      assert :unchanged = PresenceStore.swap_aggregate(user, aggregate)
      assert :changed = PresenceStore.swap_aggregate(user, %{aggregate | status: "dnd"})

      assert %{^user => %{status: "dnd"}} = PresenceStore.get_aggregates([user, "nobody"])
      assert :ok = PresenceStore.refresh(user)
    end
  end

  describe "VoiceAllocationStore" do
    test "claims once, releases only for the owner, counts and drains" do
      server = "srv-#{System.unique_integer([:positive])}"
      assert :none = VoiceAllocationStore.get(server)
      assert {:ok, "n1"} = VoiceAllocationStore.claim(server, "n1")
      assert {:ok, "n1"} = VoiceAllocationStore.claim(server, "n2")
      assert {:ok, "n1"} = VoiceAllocationStore.get(server)
      assert VoiceAllocationStore.count() >= 1

      VoiceAllocationStore.release(server, "n2")
      assert {:ok, "n1"} = VoiceAllocationStore.get(server)
      VoiceAllocationStore.release(server, "n1")
      assert :none = VoiceAllocationStore.get(server)

      assert :ok = VoiceAllocationStore.drain("n9")
      assert MapSet.member?(VoiceAllocationStore.drained(), "n9")
      assert :ok = VoiceAllocationStore.undrain("n9")
      refute MapSet.member?(VoiceAllocationStore.drained(), "n9")
    end
  end
end
