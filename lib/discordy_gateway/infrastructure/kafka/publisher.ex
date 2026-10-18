defmodule DiscordyGateway.Infrastructure.Kafka.Publisher do
  @moduledoc "Publishes envelopes to the event bus through the shared brod client."

  @behaviour DiscordyGateway.Application.Ports.EventPublisher

  @client :discordy_gateway_kafka

  @doc "Name of the brod client."
  @spec client() :: atom()
  def client, do: @client

  @impl true
  def publish(topic, key, envelope) do
    case :brod.produce_sync(@client, topic, :hash, key, Jason.encode!(envelope)) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, reason -> {:error, reason}
  end
end
