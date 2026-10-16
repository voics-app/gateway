defmodule DiscordyGateway.Domain.Session.HeartbeatTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Session.Heartbeat

  test "a fresh heartbeat state is alive" do
    hb = Heartbeat.new(41_250, 0)
    refute Heartbeat.expired?(hb, 41_250)
  end

  test "expires after one and a half intervals without a heartbeat" do
    hb = Heartbeat.new(1_000, 0)
    refute Heartbeat.expired?(hb, 1_500)
    assert Heartbeat.expired?(hb, 1_501)
  end

  test "beat/2 pushes the deadline" do
    hb = Heartbeat.new(1_000, 0) |> Heartbeat.beat(1_400)
    refute Heartbeat.expired?(hb, 2_800)
    assert Heartbeat.expired?(hb, 2_901)
  end

  test "check interval is the plain interval" do
    assert Heartbeat.check_interval(Heartbeat.new(1_000, 0)) == 1_000
  end
end
