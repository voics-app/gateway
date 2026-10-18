defmodule DiscordyGateway.Application.Relay.TelemetryRelay do
  @moduledoc "Publishes client voice-quality samples (op 23) as VOICE_QUALITY events on the telemetry topic."

  alias DiscordyGateway.Application.{Ids, Ports}
  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Events.EventEnvelope
  alias DiscordyGateway.Domain.Relay.VoiceTelemetry

  @spec publish(String.t(), String.t(), map()) :: :ok | {:error, term()}
  def publish(user_id, session_id, sample) do
    envelope =
      EventEnvelope.new("VOICE_QUALITY", VoiceTelemetry.event_data(sample, user_id, session_id),
        id: Ids.uuid_v7(),
        occurred_at: DateTime.to_iso8601(DateTime.utc_now()),
        channel_id: sample.channel_id
      )

    Ports.event_publisher().publish(
      Contracts.topic(:telemetry),
      user_id,
      EventEnvelope.to_map(envelope)
    )
  end
end
