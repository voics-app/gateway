defmodule DiscordyGateway.Infrastructure.Kafka.Supervisor do
  @moduledoc "Supervises the brod client and one group subscriber per consumed topic."

  use Supervisor

  alias DiscordyGateway.Infrastructure.Kafka.{Publisher, TopicConsumer}

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    brokers = Keyword.fetch!(opts, :brokers)

    client_config = [
      auto_start_producers: true,
      reconnect_cool_down_seconds: 2,
      query_api_versions: true
    ]

    client = %{
      id: :brod_client,
      start: {:brod_client, :start_link, [brokers, Publisher.client(), client_config]},
      restart: :permanent
    }

    consumers = Enum.map(TopicConsumer.topics(), &TopicConsumer.child_spec/1)

    Supervisor.init([client | consumers],
      strategy: :rest_for_one,
      max_restarts: 20,
      max_seconds: 30
    )
  end
end
