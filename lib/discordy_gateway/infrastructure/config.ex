defmodule DiscordyGateway.Infrastructure.Config do
  @moduledoc """
  Reads and validates the P2 environment variables. Every problem is collected, so a
  misconfigured deployment reports all of them in one startup failure.
  """

  alias DiscordyGateway.Infrastructure.Auth.JwtVerifier

  @snowflake_range 128..191
  @log_levels ~w(debug info notice warning error)
  @e2ee_policies %{"open" => :open, "closed" => :closed, "wait" => :wait}

  @type t :: map()

  @doc "Parses `env` (a map of variable name to value) into the runtime configuration."
  @spec parse(%{String.t() => String.t()}) :: {:ok, t()} | {:error, [String.t()]}
  def parse(env) do
    fields = [
      {:env, one_of(env, "DISCORDY_ENV", ~w(local production), "local")},
      {:log_level, one_of(env, "LOG_LEVEL", @log_levels, "info")},
      {:otel_endpoint, {:ok, blank_to_nil(env["OTEL_EXPORTER_OTLP_ENDPOINT"])}},
      {:internal_token, required(env, "INTERNAL_SERVICE_TOKEN")},
      {:jwt_public_key, public_key(env)},
      {:snowflake_worker_id, snowflake(env)},
      {:kafka_brokers, kafka_brokers(env)},
      {:valkey_url, url(env, "VALKEY_URL", ~w(redis rediss valkey valkeys))},
      {:etcd_endpoints, etcd_endpoints(env)},
      {:turn_secret, required(env, "TURN_STATIC_AUTH_SECRET")},
      {:turn_urls, list(env, "TURN_URLS")},
      {:api_url, url(env, "API_INTERNAL_URL", ~w(http https))},
      {:messages_url, url(env, "MESSAGES_INTERNAL_URL", ~w(http https))},
      {:voice_token_secret, voice_secret(env)},
      {:public_gateway_url, url(env, "PUBLIC_GATEWAY_URL", ~w(ws wss http https))},
      {:default_voice_region,
       {:ok, env["DEFAULT_VOICE_REGION"] |> blank_to_nil() |> Kernel.||("nl-home")}},
      {:e2ee_settings_unavailable_policy, e2ee_settings_policy(env)},
      {:voice_detach_grace_ms, voice_detach_grace(env)},
      {:admission_memory_fraction, admission_fraction(env)},
      {:memory_limit_bytes, memory_limit(env)},
      {:cluster_dns_query, {:ok, blank_to_nil(env["CLUSTER_DNS_QUERY"])}},
      {:port, port(env)}
    ]

    case Enum.split_with(fields, fn {_key, result} -> match?({:ok, _}, result) end) do
      {ok, []} -> {:ok, Map.new(ok, fn {key, {:ok, value}} -> {key, value} end)}
      {_ok, errors} -> {:error, Enum.map(errors, fn {_key, {:error, message}} -> message end)}
    end
  end

  @doc "Parses the process environment and raises with every problem found."
  @spec load!() :: t()
  def load! do
    case parse(System.get_env()) do
      {:ok, config} -> config
      {:error, errors} -> raise "invalid gateway configuration:\n  " <> Enum.join(errors, "\n  ")
    end
  end

  defp required(env, name) do
    case blank_to_nil(env[name]) do
      nil -> {:error, "#{name} is required"}
      value -> {:ok, value}
    end
  end

  defp one_of(env, name, allowed, default) do
    value = blank_to_nil(env[name]) || default

    if value in allowed,
      do: {:ok, value},
      else: {:error, "#{name} must be one of #{Enum.join(allowed, ", ")}"}
  end

  defp e2ee_settings_policy(env) do
    with {:ok, value} <-
           one_of(env, "E2EE_SETTINGS_UNAVAILABLE_POLICY", ~w(open closed wait), "open"),
         do: {:ok, Map.fetch!(@e2ee_policies, value)}
  end

  defp public_key(env) do
    with {:ok, pem} <- required(env, "JWT_PUBLIC_KEY"),
         {:ok, key} <- JwtVerifier.parse_public_key(normalize_pem(pem)) do
      {:ok, key}
    else
      {:error, :invalid_public_key} ->
        {:error, "JWT_PUBLIC_KEY must be an Ed25519 public key in PEM"}

      {:error, message} ->
        {:error, message}
    end
  end

  # Env files and orchestrators differ in how they carry a multi-line PEM: surrounding quotes
  # and literal `\n` escapes are both accepted.
  defp normalize_pem(pem) do
    pem |> String.trim() |> String.trim("\"") |> String.trim("'") |> String.replace("\\n", "\n")
  end

  defp snowflake(env) do
    case blank_to_nil(env["SNOWFLAKE_WORKER_ID"]) do
      nil ->
        {:ok, nil}

      value ->
        case Integer.parse(value) do
          {id, ""} when id in @snowflake_range -> {:ok, id}
          _ -> {:error, "SNOWFLAKE_WORKER_ID must be an integer in #{inspect(@snowflake_range)}"}
        end
    end
  end

  defp kafka_brokers(env) do
    with {:ok, brokers} <- list(env, "KAFKA_BROKERS") do
      parsed = Enum.map(brokers, &host_port/1)

      if Enum.all?(parsed, &(&1 != :error)),
        do: {:ok, parsed},
        else: {:error, "KAFKA_BROKERS must be a comma-separated list of host:port"}
    end
  end

  defp etcd_endpoints(env) do
    with {:ok, endpoints} <- list(env, "ETCD_ENDPOINTS") do
      stripped =
        Enum.map(
          endpoints,
          &(&1 |> String.replace(~r{^https?://}, "") |> String.trim_trailing("/"))
        )

      if Enum.all?(stripped, &(host_port(&1) != :error)),
        do: {:ok, Enum.map(stripped, &String.to_charlist/1)},
        else: {:error, "ETCD_ENDPOINTS must be a comma-separated list of host:port"}
    end
  end

  defp host_port(value) do
    with [host, port] <- String.split(value, ":"),
         {port, ""} <- Integer.parse(port),
         true <- host != "" do
      {String.to_charlist(host), port}
    else
      _ -> :error
    end
  end

  defp list(env, name) do
    case env[name] |> to_string() |> String.split(",", trim: true) |> Enum.map(&String.trim/1) do
      [] -> {:error, "#{name} is required"}
      values -> {:ok, values}
    end
  end

  defp url(env, name, schemes) do
    with {:ok, value} <- required(env, name),
         %URI{scheme: scheme, host: host} when is_binary(host) <- URI.parse(value),
         true <- scheme in schemes do
      {:ok, String.trim_trailing(value, "/")}
    else
      {:error, message} -> {:error, message}
      _ -> {:error, "#{name} must be a #{Enum.join(schemes, "/")} URL"}
    end
  end

  defp voice_secret(env) do
    with {:ok, secret} <- required(env, "VOICE_TOKEN_SECRET") do
      if byte_size(secret) >= 16,
        do: {:ok, secret},
        else: {:error, "VOICE_TOKEN_SECRET must be at least 16 bytes"}
    end
  end

  defp voice_detach_grace(env) do
    case Integer.parse(blank_to_nil(env["VOICE_DETACH_GRACE_MS"]) || "15000") do
      {ms, ""} when ms >= 0 -> {:ok, ms}
      _ -> {:error, "VOICE_DETACH_GRACE_MS must be a non-negative integer"}
    end
  end

  defp admission_fraction(env) do
    case Float.parse(blank_to_nil(env["ADMISSION_MEMORY_FRACTION"]) || "0.75") do
      {fraction, ""} when fraction > 0.0 and fraction <= 1.0 -> {:ok, fraction}
      _ -> {:error, "ADMISSION_MEMORY_FRACTION must be a number above 0 and at most 1"}
    end
  end

  defp memory_limit(env) do
    case blank_to_nil(env["MEMORY_LIMIT_BYTES"]) do
      nil ->
        {:ok, nil}

      value ->
        case Integer.parse(value) do
          {bytes, ""} when bytes > 0 -> {:ok, bytes}
          _ -> {:error, "MEMORY_LIMIT_BYTES must be a positive integer"}
        end
    end
  end

  defp port(env) do
    case Integer.parse(blank_to_nil(env["PORT"]) || "4000") do
      {port, ""} when port in 1..65_535 -> {:ok, port}
      _ -> {:error, "PORT must be a valid TCP port"}
    end
  end

  defp blank_to_nil(nil), do: nil

  defp blank_to_nil(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end
end
