defmodule DiscordyGateway.Infrastructure.Kafka.TopicConsumer do
  @moduledoc """
  brod group subscriber for one topic. All gateway nodes share one consumer group per topic, so
  each record is consumed by exactly one node; that node routes it cluster-wide (guild process,
  user sessions), which is why guild and DM-call processes are cluster singletons.
  """

  @behaviour :brod_group_subscriber_v2

  alias DiscordyGateway.Application.Events.Ingest
  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Infrastructure.Kafka.Publisher

  require Record

  Record.defrecordp(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  )

  @consumed_topics [:guild, :message, :user, :interaction, :audit]

  # brod's defaults (max_wait_time 10 s, sleep_timeout 1 s) are tuned for throughput, not delivery
  # latency: measured against Redpanda they put a ~450 ms p50 / ~860 ms p95 floor under every
  # realtime event (gateway#45). `max_wait_time` is a long poll: a record that arrives mid-poll is
  # returned at once, so a short value costs idle CPU (one fetch per partition per interval) and
  # buys nothing; 500 ms measured the same latency as 100 ms at a third of the idle CPU.
  # `sleep_timeout` paces retries after a failed or empty fetch. `min_bytes: 1` returns on the
  # first byte.
  @consumer_config [
    begin_offset: :latest,
    max_wait_time: 500,
    sleep_timeout: 20,
    min_bytes: 1,
    prefetch_count: 50,
    prefetch_bytes: 1_048_576
  ]

  @doc "brod consumer settings of every topic subscriber."
  @spec consumer_config() :: keyword()
  def consumer_config, do: @consumer_config

  @doc "Topics the gateway consumes, as `{topic_atom, topic_name}`."
  @spec topics() :: [{atom(), String.t()}]
  def topics, do: Enum.map(@consumed_topics, &{&1, Contracts.topic(&1)})

  @doc "Child spec of the group subscriber for `topic`."
  @spec child_spec({atom(), String.t()}) :: Supervisor.child_spec()
  def child_spec({atom, name}) do
    config = %{
      client: Publisher.client(),
      group_id: "discordy-gateway." <> name,
      topics: [name],
      cb_module: __MODULE__,
      init_data: atom,
      message_type: :message,
      consumer_config: @consumer_config,
      group_config: [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5]
    }

    %{
      id: {__MODULE__, atom},
      start: {:brod_group_subscriber_v2, :start_link, [config]},
      restart: :permanent
    }
  end

  @impl true
  def init(_init_info, topic_atom), do: {:ok, topic_atom}

  @impl true
  def handle_message(message, topic_atom) do
    Ingest.handle(topic_atom, kafka_message(message, :value))
    {:ok, :commit, topic_atom}
  end
end
