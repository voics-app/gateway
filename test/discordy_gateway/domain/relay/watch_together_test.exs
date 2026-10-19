defmodule DiscordyGateway.Domain.Relay.WatchTogetherTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Relay.WatchTogether

  defp cmd(action, url, position),
    do: %{channel_id: "c1", action: action, url: url, position_ms: position}

  test "stamp/3 adds the server clock and the actor" do
    assert WatchTogether.stamp(cmd("play", nil, 1_500), "u1", 99_000) == %{
             "channel_id" => "c1",
             "action" => "play",
             "position_ms" => 1_500,
             "at_server_ms" => 99_000,
             "user_id" => "u1"
           }
  end

  test "stamp/3 keeps the url on load" do
    assert WatchTogether.stamp(cmd("load", "https://v", 0), "u1", 1)["url"] == "https://v"
  end

  test "track/2 remembers the latest state per channel and carries the url across actions" do
    state = WatchTogether.track(%{}, WatchTogether.stamp(cmd("load", "https://v", 0), "u1", 1))
    state = WatchTogether.track(state, WatchTogether.stamp(cmd("seek", nil, 50), "u1", 2))
    assert %{"action" => "seek", "url" => "https://v", "position_ms" => 50} = state["c1"]
  end

  test "forget/2 drops a channel's state" do
    state = WatchTogether.track(%{}, WatchTogether.stamp(cmd("load", "https://v", 0), "u1", 1))
    assert WatchTogether.forget(state, "c1") == %{}
  end
end
