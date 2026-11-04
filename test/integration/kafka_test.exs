defmodule DiscordyGateway.Integration.KafkaTest do
  @moduledoc """
  Needs a Kafka-compatible broker. By default it starts Redpanda in Docker; set `KAFKA_TEST_PORT`
  (and optionally `KAFKA_TEST_HOST`, default `127.0.0.1`) to use a broker that is already running,
  with topics created and advertised address `<host>:<port>`. This is how `make test-kafka` runs
  the suite inside a Linux container on the same Docker network as Redpanda, because the CRC32C
  NIF that brod needs is only compiled there.
  """

  use DiscordyGateway.Case, async: false

  @moduletag :integration

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Infrastructure.Kafka.{Publisher, Supervisor, TopicConsumer}

  @image "docker.redpanda.com/redpandadata/redpanda:v25.3.17"

  setup_all do
    {host, port} =
      case System.get_env("KAFKA_TEST_PORT") do
        nil ->
          {~c"127.0.0.1", start_redpanda()}

        value ->
          {String.to_charlist(System.get_env("KAFKA_TEST_HOST", "127.0.0.1")),
           String.to_integer(value)}
      end

    start_supervised!({Supervisor, brokers: [{host, port}]})
    Process.sleep(8_000)
    :ok
  end

  defp start_redpanda do
    port = free_port()
    name = "gateway-it-redpanda-#{port}"

    args = [
      "run",
      "-d",
      "--name",
      name,
      "-p",
      "127.0.0.1:#{port}:#{port}",
      @image,
      "redpanda",
      "start",
      "--smp",
      "1",
      "--memory",
      "512M",
      "--overprovisioned",
      "--node-id",
      "0",
      "--check=false",
      "--kafka-addr",
      "0.0.0.0:#{port}",
      "--advertise-kafka-addr",
      "127.0.0.1:#{port}"
    ]

    {_, 0} = System.cmd("docker", args)
    on_exit(fn -> System.cmd("docker", ["rm", "-f", name], stderr_to_stdout: true) end)
    await_ready(name, port)

    for {_atom, topic} <- TopicConsumer.topics() ++ [{:telemetry, Contracts.topic(:telemetry)}] do
      {_, 0} =
        System.cmd("docker", [
          "exec",
          name,
          "rpk",
          "topic",
          "create",
          topic,
          "-p",
          "3",
          "-X",
          "brokers=127.0.0.1:#{port}"
        ])
    end

    port
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp await_ready(name, port, attempts \\ 60) do
    {out, _} =
      System.cmd(
        "docker",
        ["exec", name, "rpk", "cluster", "health", "-X", "brokers=127.0.0.1:#{port}"],
        stderr_to_stdout: true
      )

    cond do
      out =~ ~r/Healthy:\s+true/ -> :ok
      attempts == 0 -> raise "redpanda never became healthy: #{out}"
      true -> Process.sleep(1_000) && await_ready(name, port, attempts - 1)
    end
  end

  defp envelope(type, data, fields) do
    Map.merge(
      %{
        "id" => unique_id(),
        "type" => type,
        "v" => 1,
        "occurred_at" => "2026-10-18T00:00:00Z",
        "data" => data
      },
      fields
    )
  end

  test "a record on the user topic is consumed, routed and delivered to the user's session" do
    Fakes.put({:ready, "31"}, {:ok, %{"user" => %{"id" => "31"}, "guilds" => []}})
    user = online("31")

    assert :ok =
             Publisher.publish(
               Contracts.topic(:user),
               "31",
               envelope("USER_UPDATE", %{"id" => "31", "username" => "kafka"}, %{
                 "target_user_ids" => ["31"]
               })
             )

    assert %{"username" => "kafka"} = await(user, "USER_UPDATE", 20_000)
  end

  test "duplicate envelope ids are delivered once" do
    Fakes.put({:ready, "32"}, {:ok, %{"user" => %{"id" => "32"}, "guilds" => []}})
    user = online("32")

    same =
      envelope("USER_UPDATE", %{"id" => "32", "username" => "once"}, %{
        "target_user_ids" => ["32"]
      })

    assert :ok = Publisher.publish(Contracts.topic(:user), "32", same)
    assert :ok = Publisher.publish(Contracts.topic(:user), "32", same)

    assert %{"username" => "once"} = await(user, "USER_UPDATE", 20_000)
    assert await(user, "USER_UPDATE", 1_000) == :none
  end

  test "delivery from publish to the user's session stays well under the old 1 s polling floor" do
    Fakes.put({:ready, "33"}, {:ok, %{"user" => %{"id" => "33"}, "guilds" => []}})
    user = online("33")

    latencies =
      for n <- 1..20 do
        started = System.monotonic_time(:millisecond)

        :ok =
          Publisher.publish(
            Contracts.topic(:user),
            "33",
            envelope("USER_UPDATE", %{"id" => "33", "username" => "lat#{n}"}, %{
              "target_user_ids" => ["33"]
            })
          )

        assert %{"username" => "lat" <> _} = await(user, "USER_UPDATE", 5_000)
        Process.sleep(50)
        System.monotonic_time(:millisecond) - started - 50
      end

    sorted = Enum.sort(latencies)
    assert Enum.at(sorted, 9) < 100, "p50 #{Enum.at(sorted, 9)} ms: #{inspect(sorted)}"
    assert Enum.at(sorted, 18) < 250, "p95 #{Enum.at(sorted, 18)} ms: #{inspect(sorted)}"
  end
end
