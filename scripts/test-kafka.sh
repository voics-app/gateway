#!/bin/sh
# Runs the Kafka integration suite (test/integration/kafka_test.exs) against a throwaway Redpanda.
# Redpanda and the Linux Elixir container share a user-defined network, so brod's CRC32C NIF
# compiles in the container, which is the only place it builds on Windows hosts.
# Containers and the network are removed on exit. Dependency and build caches live in named
# volumes (gateway-kafka-deps, gateway-kafka-build); remove them with `docker volume rm`.
set -eu

ELIXIR_IMAGE="${ELIXIR_IMAGE:-hexpm/elixir:1.20.4-erlang-29.1.2-alpine-3.24.2}"
REDPANDA_IMAGE="${REDPANDA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:v25.3.17}"
NETWORK="gateway-kafka-net"
BROKER="gateway-redpanda"
TEST_CONTAINER="gateway-kafka-test"
KAFKA_PORT=9092

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Docker needs a Windows path for bind mounts when running under Git Bash.
ROOT_MOUNT="$(cd "$ROOT" && (pwd -W 2>/dev/null || pwd))"
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cleanup() {
  docker rm -f "$BROKER" "$TEST_CONTAINER" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup
docker network create "$NETWORK" >/dev/null

docker run -d --name "$BROKER" --network "$NETWORK" --network-alias redpanda \
  -p "127.0.0.1::$KAFKA_PORT" "$REDPANDA_IMAGE" \
  redpanda start --smp 1 --memory 512M --overprovisioned --node-id 0 --check=false \
  --kafka-addr "0.0.0.0:$KAFKA_PORT" --advertise-kafka-addr "redpanda:$KAFKA_PORT" >/dev/null

attempts=0
until docker exec "$BROKER" rpk cluster health -X "brokers=redpanda:$KAFKA_PORT" 2>/dev/null | grep -q 'Healthy:[[:space:]]*true'; do
  attempts=$((attempts + 1))
  if [ "$attempts" -ge 90 ]; then
    echo "redpanda never became healthy" >&2
    docker logs "$BROKER" >&2 || true
    exit 1
  fi
  sleep 1
done

docker run --rm --name "$TEST_CONTAINER" --network "$NETWORK" \
  -e KAFKA_TEST_HOST=redpanda -e KAFKA_TEST_PORT="$KAFKA_PORT" \
  -v "$ROOT_MOUNT:/app" \
  -v gateway-kafka-deps:/app/deps \
  -v gateway-kafka-build:/app/_build \
  -w /app "$ELIXIR_IMAGE" \
  sh -c 'apk add --no-cache build-base cmake git >/dev/null \
    && mix local.hex --force >/dev/null && mix local.rebar --force >/dev/null \
    && mix deps.get >/dev/null \
    && mix test --include integration test/integration/kafka_test.exs'
