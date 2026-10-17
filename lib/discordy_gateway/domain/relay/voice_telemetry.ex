defmodule DiscordyGateway.Domain.Relay.VoiceTelemetry do
  @moduledoc "Client voice-quality samples (op 23) turned into VOICE_QUALITY event data."

  @spec event_data(map(), String.t(), String.t()) :: map()
  def event_data(sample, user_id, session_id) do
    %{
      "source" => "client",
      "user_id" => user_id,
      "session_id" => session_id,
      "channel_id" => sample.channel_id,
      "jitter_ms" => sample.jitter_ms,
      "loss_pct" => sample.loss_pct,
      "rtt_ms" => sample.rtt_ms,
      "bitrate_kbps" => sample.bitrate_kbps
    }
  end
end
