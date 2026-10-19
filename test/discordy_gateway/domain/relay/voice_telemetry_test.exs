defmodule DiscordyGateway.Domain.Relay.VoiceTelemetryTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Relay.VoiceTelemetry

  test "event_data/3 tags the sample with its origin" do
    sample = %{channel_id: "c1", jitter_ms: 2, loss_pct: 0.5, rtt_ms: 30, bitrate_kbps: 64}

    assert VoiceTelemetry.event_data(sample, "u1", "s1") == %{
             "source" => "client",
             "user_id" => "u1",
             "session_id" => "s1",
             "channel_id" => "c1",
             "jitter_ms" => 2,
             "loss_pct" => 0.5,
             "rtt_ms" => 30,
             "bitrate_kbps" => 64
           }
  end
end
