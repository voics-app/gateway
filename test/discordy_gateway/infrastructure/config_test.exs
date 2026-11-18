defmodule DiscordyGateway.Infrastructure.ConfigTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Infrastructure.Config

  @spki_prefix <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>

  defp valid_env do
    {public, _private} = :crypto.generate_key(:eddsa, :ed25519)

    %{
      "INTERNAL_SERVICE_TOKEN" => "internal",
      "JWT_PUBLIC_KEY" =>
        :public_key.pem_encode([{:SubjectPublicKeyInfo, @spki_prefix <> public, :not_encrypted}]),
      "KAFKA_BROKERS" => "localhost:19092,kafka:9092",
      "VALKEY_URL" => "redis://localhost:6379",
      "ETCD_ENDPOINTS" => "http://etcd:2379",
      "TURN_STATIC_AUTH_SECRET" => "turn-secret",
      "TURN_URLS" => "turn:localhost:3478?transport=udp, turn:localhost:3478?transport=tcp",
      "API_INTERNAL_URL" => "http://api:8080",
      "MESSAGES_INTERNAL_URL" => "http://messages:8082",
      "VOICE_TOKEN_SECRET" => "dev-voice-token-secret",
      "PUBLIC_GATEWAY_URL" => "ws://localhost:8000/gateway",
      "SNOWFLAKE_WORKER_ID" => "128"
    }
  end

  test "parses a complete environment and applies defaults" do
    assert {:ok, config} = Config.parse(valid_env())
    assert config.env == "local"
    assert config.port == 4000
    assert config.default_voice_region == "nl-home"

    assert config.turn_urls == [
             "turn:localhost:3478?transport=udp",
             "turn:localhost:3478?transport=tcp"
           ]

    assert config.turn_secret == "turn-secret"
    assert config.kafka_brokers == [{~c"localhost", 19_092}, {~c"kafka", 9092}]
    assert config.etcd_endpoints == [~c"etcd:2379"]
    assert byte_size(config.jwt_public_key) == 32
    assert config.snowflake_worker_id == 128
    assert config.cluster_dns_query == nil
    assert config.mobile_background_presence_ms == 86_400_000
  end

  test "mobile background availability has a configurable positive bound" do
    assert {:ok, %{mobile_background_presence_ms: 60_000}} =
             Config.parse(Map.put(valid_env(), "MOBILE_BACKGROUND_PRESENCE_MS", "60000"))

    for value <- ["0", "-1", "forever"] do
      assert {:error, errors} =
               Config.parse(Map.put(valid_env(), "MOBILE_BACKGROUND_PRESENCE_MS", value))

      assert "MOBILE_BACKGROUND_PRESENCE_MS must be a positive integer" in errors
    end
  end

  test "E2EE_SETTINGS_UNAVAILABLE_POLICY defaults to open and accepts open, closed and wait" do
    assert {:ok, config} = Config.parse(valid_env())
    assert config.e2ee_settings_unavailable_policy == :open

    for {value, policy} <- [{"open", :open}, {"closed", :closed}, {"wait", :wait}] do
      env = Map.put(valid_env(), "E2EE_SETTINGS_UNAVAILABLE_POLICY", value)
      assert {:ok, %{e2ee_settings_unavailable_policy: ^policy}} = Config.parse(env)
    end
  end

  test "rejects an unknown E2EE_SETTINGS_UNAVAILABLE_POLICY" do
    env = Map.put(valid_env(), "E2EE_SETTINGS_UNAVAILABLE_POLICY", "maybe")

    assert {:error, errors} = Config.parse(env)
    assert "E2EE_SETTINGS_UNAVAILABLE_POLICY must be one of open, closed, wait" in errors
  end

  test "admission settings default to 0.75 of an unknown limit and are validated" do
    assert {:ok, config} = Config.parse(valid_env())
    assert config.admission_memory_fraction == 0.75
    assert config.memory_limit_bytes == nil

    env =
      Map.merge(valid_env(), %{
        "ADMISSION_MEMORY_FRACTION" => "0.6",
        "MEMORY_LIMIT_BYTES" => "1073741824"
      })

    assert {:ok, %{admission_memory_fraction: 0.6, memory_limit_bytes: 1_073_741_824}} =
             Config.parse(env)

    for bad <- ["0", "1.5", "lots"] do
      env = Map.put(valid_env(), "ADMISSION_MEMORY_FRACTION", bad)
      assert {:error, errors} = Config.parse(env)
      assert "ADMISSION_MEMORY_FRACTION must be a number above 0 and at most 1" in errors
    end

    env = Map.put(valid_env(), "MEMORY_LIMIT_BYTES", "-1")
    assert {:error, errors} = Config.parse(env)
    assert "MEMORY_LIMIT_BYTES must be a positive integer" in errors
  end

  test "TRANSPORT_BACKLOG_LIMIT defaults to 2000 and must be a positive integer" do
    assert {:ok, %{transport_backlog_limit: 2000}} = Config.parse(valid_env())

    assert {:ok, %{transport_backlog_limit: 500}} =
             Config.parse(Map.put(valid_env(), "TRANSPORT_BACKLOG_LIMIT", "500"))

    assert {:error, errors} = Config.parse(Map.put(valid_env(), "TRANSPORT_BACKLOG_LIMIT", "0"))
    assert "TRANSPORT_BACKLOG_LIMIT must be a positive integer" in errors
  end

  test "reports every problem at once" do
    assert {:error, errors} =
             Config.parse(%{
               "JWT_PUBLIC_KEY" => "junk",
               "SNOWFLAKE_WORKER_ID" => "5",
               "KAFKA_BROKERS" => "nope"
             })

    joined = Enum.join(errors, "\n")
    assert joined =~ "INTERNAL_SERVICE_TOKEN is required"
    assert joined =~ "JWT_PUBLIC_KEY must be an Ed25519"
    assert joined =~ "SNOWFLAKE_WORKER_ID must be an integer in 128..191"
    assert joined =~ "KAFKA_BROKERS must be"
    assert joined =~ "TURN_STATIC_AUTH_SECRET is required"
    assert joined =~ "TURN_URLS is required"
    assert joined =~ "VOICE_TOKEN_SECRET is required"
  end

  test "rejects a short voice secret and a bad port" do
    env = Map.merge(valid_env(), %{"VOICE_TOKEN_SECRET" => "short", "PORT" => "0"})
    assert {:error, errors} = Config.parse(env)
    assert Enum.any?(errors, &(&1 =~ "VOICE_TOKEN_SECRET"))
    assert Enum.any?(errors, &(&1 =~ "PORT"))
  end

  test "accepts PEM keys with escaped newlines, as passed through some orchestrators" do
    env = Map.update!(valid_env(), "JWT_PUBLIC_KEY", &String.replace(&1, "\n", "\\n"))
    assert {:ok, _config} = Config.parse(env)
  end
end
