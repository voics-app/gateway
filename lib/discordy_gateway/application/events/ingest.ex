defmodule DiscordyGateway.Application.Events.Ingest do
  @moduledoc """
  Entry point for a record read from an event-bus topic: decode, validate the P6 envelope,
  drop duplicates, and route. Always returns `:ok` so that a malformed record never blocks the
  partition; the outcome is reported through telemetry.
  """

  alias DiscordyGateway.Application.Events.{Deduplicator, Router}
  alias DiscordyGateway.Domain.Events.EventEnvelope

  require Logger

  @type outcome :: :routed | :duplicate | :invalid

  @spec handle(atom(), binary()) :: :ok
  def handle(topic, value) do
    outcome = process(topic, value)

    :telemetry.execute([:discordy_gateway, :events, :consumed], %{count: 1}, %{
      topic: topic,
      outcome: outcome
    })

    :ok
  end

  defp process(topic, value) do
    with {:ok, raw} <- Jason.decode(value),
         {:ok, envelope} <- EventEnvelope.parse(raw) do
      case Deduplicator.check(envelope.id) do
        :new ->
          Router.deliver(topic, envelope)
          :routed

        :duplicate ->
          :duplicate
      end
    else
      _invalid ->
        Logger.warning("dropping malformed event on #{topic}")
        :invalid
    end
  end
end
