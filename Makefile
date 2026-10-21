.PHONY: dev test integration test-kafka lint build docker

IMAGE ?= ghcr.io/discordy-voics/gateway:dev

# Runs the gateway with the variables of .env (copy .env.example first).
dev:
	set -a && . ./.env && set +a && iex -S mix

test:
	mix test

# Needs Docker: starts throwaway Valkey, etcd and Redpanda containers on random ports.
integration:
	mix test --include integration

# Runs the Kafka suite against a throwaway Redpanda; the suite runs in a Linux Elixir container
# (brod's CRC32C NIF does not build on Windows). Needs Docker. See scripts/test-kafka.sh.
test-kafka:
	sh scripts/test-kafka.sh

lint:
	mix format --check-formatted
	mix compile --warnings-as-errors --force
	mix credo --strict
	mix sobelow --exit medium

build:
	MIX_ENV=prod mix release --overwrite

docker:
	docker build -t $(IMAGE) --build-arg REVISION=$$(git rev-parse --short HEAD) .

# Install the pre-push hook (lint + unit tests, see lefthook.yml).
.PHONY: hooks
hooks:
	lefthook install
