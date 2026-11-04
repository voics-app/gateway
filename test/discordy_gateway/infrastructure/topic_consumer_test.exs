defmodule DiscordyGateway.Infrastructure.Kafka.TopicConsumerTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Infrastructure.Kafka.TopicConsumer

  test "fetches long-poll briefly and retry quickly so delivery is not paced by brod defaults" do
    config = TopicConsumer.consumer_config()

    assert config[:max_wait_time] <= 500
    assert config[:sleep_timeout] <= 50
    assert config[:min_bytes] == 1
    assert config[:begin_offset] == :latest
    assert config[:prefetch_count] > 0 and config[:prefetch_bytes] > 0
  end

  test "every group subscriber is built with that config" do
    for topic <- TopicConsumer.topics() do
      %{start: {_, _, [group]}} = TopicConsumer.child_spec(topic)
      assert group.consumer_config == TopicConsumer.consumer_config()
    end
  end
end
