defmodule DiscordyGateway.Domain.PresenceTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Presence

  defp s(status, platform, activities \\ []),
    do: %{status: status, activities: activities, afk: false, platform: platform}

  test "no sessions means offline" do
    assert Presence.aggregate([]) == %{
             status: "offline",
             activities: [],
             client_status: %{},
             afk: false
           }
  end

  test "AFK requires every session, including invisible sessions, to be inactive" do
    idle = %{s("idle", "desktop") | afk: true}
    assert Presence.aggregate([idle]).afk
    refute Presence.aggregate([idle, s("invisible", "mobile")]).afk
  end

  test "dnd beats online beats idle" do
    assert Presence.aggregate([s("idle", "desktop"), s("online", "mobile")]).status == "online"
    assert Presence.aggregate([s("online", "desktop"), s("dnd", "web")]).status == "dnd"
    assert Presence.aggregate([s("idle", "desktop")]).status == "idle"
  end

  test "invisible sessions appear offline and are left out of client_status" do
    agg = Presence.aggregate([s("invisible", "desktop")])
    assert agg.status == "offline"
    assert agg.client_status == %{}

    agg = Presence.aggregate([s("invisible", "desktop"), s("idle", "mobile")])
    assert agg.status == "idle"
    assert agg.client_status == %{"mobile" => "idle"}
  end

  test "client_status reports the best status per platform" do
    agg = Presence.aggregate([s("idle", "desktop"), s("online", "desktop"), s("dnd", "mobile")])
    assert agg.client_status == %{"desktop" => "online", "mobile" => "dnd"}
  end

  test "activities come from the highest-priority session" do
    agg =
      Presence.aggregate([
        s("idle", "desktop", [%{"name" => "a"}]),
        s("online", "web", [%{"name" => "b"}])
      ])

    assert agg.activities == [%{"name" => "b"}]
  end

  test "platform_for/1 maps identify properties" do
    assert Presence.platform_for(%{os: "android", client: "discordy"}) == "mobile"
    assert Presence.platform_for(%{os: "ios", client: "x"}) == "mobile"
    assert Presence.platform_for(%{os: "linux", client: "web"}) == "web"
    assert Presence.platform_for(%{os: "windows", client: "discordy"}) == "desktop"
  end

  test "dispatch_payload/3 renders PRESENCE_UPDATE" do
    agg = %{status: "online", activities: [], client_status: %{"desktop" => "online"}}

    assert Presence.dispatch_payload("7", agg, "100") == %{
             "user" => %{"id" => "7"},
             "guild_id" => "100",
             "status" => "online",
             "activities" => [],
             "client_status" => %{"desktop" => "online"}
           }

    refute Map.has_key?(Presence.dispatch_payload("7", agg, nil), "guild_id")
  end
end
