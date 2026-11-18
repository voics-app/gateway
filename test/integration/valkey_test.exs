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

  # `VALKEY_TEST_URL` points the suite at an existing Valkey (for a runner that cannot reach the
  # ports of containers it starts); without it a throwaway container is started.
  setup_all do
    url =
      System.get_env("VALKEY_TEST_URL") ||
        (
          {_name, port} =
            Containers.start("valkey/valkey:8-alpine", 6379, [], "Ready to accept connections")

          "redis://127.0.0.1:#{port}"
        )

    start_supervised!({Pool, url: url})
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

    test "batched appends of many sessions stay ordered per session and are visible to fetch" do
      ids = for n <- 1..20, do: "batch-#{System.unique_integer([:positive])}-#{n}"

      for seq <- 1..50, id <- ids, do: :ok = ResumeStore.append(id, [{seq, "f#{seq}"}])

      for id <- ids do
        assert {:ok, entries} = ResumeStore.fetch(id)
        assert Enum.map(entries, &elem(&1, 0)) == Enum.to_list(1..50)
      end
    end

    test "delete removes entries that were still waiting for their batch" do
      id = "del-#{System.unique_integer([:positive])}"
      :ok = ResumeStore.append(id, [{1, "a"}])
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

      assert %{^user => %{status: "online"}} = PresenceStore.get_aggregates([user, "nobody"])
      assert {:ok, [_]} = PresenceStore.refresh(user, "s2")
    end

    test "session values remain readable by the previous gateway during a rolling upgrade" do
      user = "rolling-presence-#{System.unique_integer([:positive])}"
      presence = %{status: "idle", activities: [], afk: true, platform: "mobile"}
      assert {:ok, [_]} = PresenceStore.put(user, "phone", presence)
      assert {:ok, stored} = Pool.command(["HGET", "presence:sessions:" <> user, "phone"])
      legacy = Jason.decode!(stored)
      assert legacy["status"] == "idle"
      assert legacy["platform"] == "mobile"
      assert legacy["afk"] == true
      assert {:ok, []} = PresenceStore.remove(user, "phone")

      assert {:ok, 1} =
               Pool.command([
                 "HSET",
                 "presence:sessions:" <> user,
                 "old",
                 Jason.encode!(presence)
               ])

      assert {:ok, [%{status: "idle", platform: "mobile"}]} = PresenceStore.refresh(user, "old")
      assert {:ok, []} = PresenceStore.remove(user, "old")
    end

    test "refreshing the desktop cannot extend an expired phone or return a stale aggregate" do
      user = "expiry-#{System.unique_integer([:positive])}"
      desktop = %{status: "idle", activities: [], afk: true, platform: "desktop"}
      phone = %{status: "online", activities: [], afk: false, platform: "mobile"}
      assert {:ok, [_]} = PresenceStore.put(user, "desktop", desktop)
      assert {:ok, [_, _]} = PresenceStore.put(user, "phone", phone)
      stale = Map.put(phone, :expires_at, 0)
      Pool.command(["HSET", "presence:sessions:" <> user, "phone", Jason.encode!(stale)])
      PresenceStore.swap_aggregate(user, %{status: "online", activities: [], client_status: %{}})

      assert {:ok, [%{platform: "desktop", activities: []}]} =
               PresenceStore.refresh(user, "desktop")

      assert %{^user => %{status: "idle", client_status: %{"desktop" => "idle"}}} =
               PresenceStore.get_aggregates([user])

      PresenceStore.remove(user, "desktop")
      assert %{^user => %{status: "offline"}} = PresenceStore.get_aggregates([user])
      assert {:ok, []} = PresenceStore.refresh(user, "phone")
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
