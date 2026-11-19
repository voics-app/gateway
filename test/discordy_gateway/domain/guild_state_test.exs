defmodule DiscordyGateway.Domain.GuildStateTest do
  use ExUnit.Case, async: true
  import Bitwise

  alias DiscordyGateway.Domain.GuildState
  alias DiscordyGateway.Fixtures

  @view 1 <<< 10
  @send 1 <<< 11

  setup do
    {:ok, state: GuildState.from_payload(Fixtures.guild_payload())}
  end

  describe "from_payload/1" do
    test "reads a flat Discord guild object", %{state: state} do
      assert state.id == "100"
      assert state.owner_id == "1"
      assert state.voice_region == "eu-west"
      assert map_size(state.channels) == 4
      assert GuildState.member?(state, "2")
    end

    test "reads the nested GuildFull shape" do
      p = Fixtures.guild_payload()

      nested = %{
        "guild" => Map.drop(p, ["roles", "channels", "members"]),
        "roles" => p["roles"],
        "channels" => p["channels"],
        "members" => p["members"],
        "voice_region" => "nl-home"
      }

      state = GuildState.from_payload(nested)
      assert state.id == "100"
      assert state.voice_region == "nl-home"
      assert map_size(state.members) == 3
    end
  end

  describe "channel_permissions/4" do
    test "everyone sees general", %{state: state} do
      assert (GuildState.channel_permissions(state, "3", "10", 0) &&& @view) != 0
    end

    test "staff is hidden from @everyone but visible to mods and the owner", %{state: state} do
      assert GuildState.channel_permissions(state, "3", "11", 0) == 0
      assert (GuildState.channel_permissions(state, "2", "11", 0) &&& @view) != 0
      assert (GuildState.channel_permissions(state, "1", "11", 0) &&& @view) != 0
    end

    test "threads inherit their parent's permissions", %{state: state} do
      assert GuildState.channel_permissions(state, "3", "13", 0) == 0
      assert GuildState.channel_permissions(state, "2", "13", 0) != 0
    end

    test "private threads require ownership or manage threads", %{state: state} do
      state =
        state
        |> GuildState.apply_event("GUILD_ROLE_CREATE", %{
          "role" => %{"id" => "301", "permissions" => Integer.to_string(1 <<< 34)}
        })
        |> GuildState.apply_event("GUILD_MEMBER_ADD", %{
          "user" => %{"id" => "4"},
          "roles" => ["301"]
        })
        |> GuildState.apply_event("THREAD_CREATE", %{
          "id" => "21",
          "type" => 12,
          "parent_id" => "10",
          "owner_id" => "2"
        })

      assert GuildState.channel_permissions(state, "3", "21", 0) == 0
      assert (GuildState.channel_permissions(state, "2", "21", 0) &&& @view) != 0
      assert (GuildState.channel_permissions(state, "1", "21", 0) &&& @view) != 0
      assert (GuildState.channel_permissions(state, "4", "21", 0) &&& @view) != 0
      assert GuildState.channel_permissions(state, "999", "21", 0) == 0

      state =
        GuildState.apply_event(state, "CHANNEL_UPDATE", %{
          "id" => "10",
          "type" => 0,
          "permission_overwrites" => [
            %{"id" => "100", "type" => 0, "allow" => "0", "deny" => Integer.to_string(@view)}
          ]
        })

      assert GuildState.channel_permissions(state, "2", "21", 0) == 0
      assert GuildState.channel_permissions(state, "4", "21", 0) == 0
      assert (GuildState.channel_permissions(state, "1", "21", 0) &&& @view) != 0
    end

    test "non-members and unknown channels get nothing", %{state: state} do
      assert GuildState.channel_permissions(state, "999", "10", 0) == 0
      assert GuildState.channel_permissions(state, "3", "999", 0) == 0
    end

    test "timed-out members keep only view and history", %{state: state} do
      state =
        GuildState.apply_event(state, "GUILD_MEMBER_UPDATE", %{
          "guild_id" => "100",
          "user" => %{"id" => "3"},
          "roles" => [],
          "communication_disabled_until" => "2026-01-01T00:10:00Z"
        })

      during = DateTime.to_unix(~U[2026-01-01 00:05:00Z], :millisecond)
      after_timeout = DateTime.to_unix(~U[2026-01-01 00:15:00Z], :millisecond)
      assert GuildState.channel_permissions(state, "3", "10", during) == @view
      assert (GuildState.channel_permissions(state, "3", "10", after_timeout) &&& @send) != 0
    end
  end

  test "guild_permissions/2 is the base permission set", %{state: state} do
    assert (GuildState.guild_permissions(state, "2") &&& 4) != 0
    assert GuildState.guild_permissions(state, "999") == 0
  end

  describe "apply_event/3 keeps the cache current" do
    test "CHANNEL_CREATE/UPDATE/DELETE", %{state: state} do
      state =
        GuildState.apply_event(state, "CHANNEL_CREATE", %{
          "id" => "20",
          "type" => 0,
          "guild_id" => "100"
        })

      assert Map.has_key?(state.channels, "20")

      hidden = %{
        "id" => "20",
        "type" => 0,
        "permission_overwrites" => [
          %{"id" => "100", "type" => 0, "allow" => "0", "deny" => "1024"}
        ]
      }

      state = GuildState.apply_event(state, "CHANNEL_UPDATE", hidden)
      assert GuildState.channel_permissions(state, "3", "20", 0) == 0

      state = GuildState.apply_event(state, "CHANNEL_DELETE", %{"id" => "20"})
      refute Map.has_key?(state.channels, "20")
    end

    test "THREAD events are tracked as channels", %{state: state} do
      state =
        GuildState.apply_event(state, "THREAD_CREATE", %{
          "id" => "21",
          "type" => 11,
          "parent_id" => "10"
        })

      assert (GuildState.channel_permissions(state, "3", "21", 0) &&& @view) != 0
      state = GuildState.apply_event(state, "THREAD_DELETE", %{"id" => "21"})
      refute Map.has_key?(state.channels, "21")
    end

    test "GUILD_ROLE_CREATE/UPDATE/DELETE", %{state: state} do
      state =
        GuildState.apply_event(state, "GUILD_ROLE_UPDATE", %{
          "guild_id" => "100",
          "role" => %{"id" => "100", "permissions" => "0"}
        })

      assert GuildState.channel_permissions(state, "3", "10", 0) == 0

      state =
        GuildState.apply_event(state, "GUILD_ROLE_CREATE", %{
          "guild_id" => "100",
          "role" => %{"id" => "301", "permissions" => Integer.to_string(@view)}
        })

      assert Map.has_key?(state.roles, "301")

      state =
        GuildState.apply_event(state, "GUILD_ROLE_DELETE", %{
          "guild_id" => "100",
          "role_id" => "301"
        })

      refute Map.has_key?(state.roles, "301")
    end

    test "GUILD_MEMBER_ADD/UPDATE/REMOVE", %{state: state} do
      state =
        GuildState.apply_event(state, "GUILD_MEMBER_ADD", %{
          "guild_id" => "100",
          "user" => %{"id" => "4", "username" => "carol"},
          "roles" => []
        })

      assert GuildState.member?(state, "4")

      state =
        GuildState.apply_event(state, "GUILD_MEMBER_UPDATE", %{
          "guild_id" => "100",
          "user" => %{"id" => "4"},
          "roles" => ["300"]
        })

      assert (GuildState.channel_permissions(state, "4", "11", 0) &&& @view) != 0

      state =
        GuildState.apply_event(state, "GUILD_MEMBER_REMOVE", %{
          "guild_id" => "100",
          "user" => %{"id" => "4"}
        })

      refute GuildState.member?(state, "4")
    end

    test "GUILD_UPDATE merges guild fields", %{state: state} do
      state =
        GuildState.apply_event(state, "GUILD_UPDATE", %{
          "id" => "100",
          "owner_id" => "3",
          "voice_region" => "x"
        })

      assert state.owner_id == "3"
      assert state.voice_region == "x"
      assert state.guild["name"] == "Guild"
    end

    test "unrelated events leave the state untouched", %{state: state} do
      assert GuildState.apply_event(state, "MESSAGE_CREATE", %{"id" => "m"}) == state
    end
  end

  test "snapshot/1 renders a flat Discord guild object", %{state: state} do
    snap = GuildState.snapshot(state)
    assert snap["id"] == "100"
    assert snap["name"] == "Guild"
    assert length(snap["roles"]) == 2
    assert length(snap["channels"]) == 4
    assert length(snap["members"]) == 3
    assert snap["voice_region"] == "eu-west"
  end

  describe "voice_region_for/3" do
    test "per-channel override, then guild region, then default", %{state: state} do
      assert GuildState.voice_region_for(state, "12", "nl-home") == "us-east"
      assert GuildState.voice_region_for(state, "10", "nl-home") == "eu-west"

      assert GuildState.voice_region_for(%{state | voice_region: nil}, "10", "nl-home") ==
               "nl-home"
    end
  end

  describe "member search" do
    test "query matches username or nick prefix, case-insensitive, honouring the limit", %{
      state: state
    } do
      assert [%{"user" => %{"id" => "2"}}] = GuildState.search_members(state, "al", 10)
      assert [%{"user" => %{"id" => "2"}}] = GuildState.search_members(state, "ALLY", 10)
      assert length(GuildState.search_members(state, "", 2)) == 2
      assert length(GuildState.search_members(state, "", 0)) == 3
    end

    test "lookup by ids reports the ids that were not found", %{state: state} do
      assert {[%{"user" => %{"id" => "3"}}], ["9"]} = GuildState.members_by_ids(state, ["3", "9"])
    end
  end

  test "voice_channel?/2", %{state: state} do
    assert GuildState.voice_channel?(state, "12")
    refute GuildState.voice_channel?(state, "10")
  end
end
