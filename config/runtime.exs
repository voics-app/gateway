import Config

if config_env() != :test and System.get_env("OTEL_EXPORTER_OTLP_ENDPOINT") in [nil, ""] do
  config :opentelemetry, traces_exporter: :none
end
