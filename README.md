# Discordy gateway

The realtime edge of Discordy, written in Elixir/Phoenix. It terminates client WebSockets
(`wss://<gateway>/gateway?v=1&encoding=json`, protocol P7), consumes the event bus (P6) and
fans events out to the right sessions, allocates voice nodes (P8), and relays end-to-end
encrypted messages, P2P signalling, telemetry and Watch Together.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the structure and the C4 diagram.

## What it does

| Area | Behaviour |
|---|---|
| Sessions | HELLO, IDENTIFY (EdDSA user JWT, or bot token via api introspection), READY from the api, heartbeat supervision, 120 sends / 60 s rate limit (close 4008), RESUME from the Valkey buffer within 120 s |
| Guild processes | One cluster-wide process per guild (Horde), loaded lazily from `/guilds/{id}/state`, kept current from `discordy.guild.v1`; fan-out filtered by membership, intents and VIEW_CHANNEL with the Discord permission algorithm |
| Event consumers | One brod consumer group per topic (`discordy-gateway.<topic>`), P6 routing, idempotent on envelope `id` (LRU of the last 10k ids per node) |
| Presence | Aggregated across a user's sessions (dnd > online > idle, invisible reads as offline), fanned out to guilds and friends |
| Voice | Least-loaded node of the region from etcd (P8), allocation in Valkey `voice:alloc:<id>`, one node per guild or DM call, reallocation when a node leaves etcd, `VOICE_SERVER_UPDATE` with an HS256 token, drain and undrain |
| E2EE relay | Op 20/21: per-device Valkey streams, ACK deletes, 7-day TTL sweeper, replay after READY |
| P2P | Op 22 relayed to online devices only; offline peers get `P2P_SIGNAL_FAILED {"error":"peer_offline"}`. READY carries short-lived TURN credentials (coturn REST scheme) built from `TURN_STATIC_AUTH_SECRET` and `TURN_URLS` |
| Telemetry | Op 23 published as `VOICE_QUALITY` and a per-node `USAGE_TICK` on `discordy.telemetry.v1`; Watch Together (op 24) stamped with the server clock |

HTTP endpoints: `GET /healthz`, `GET /readyz`, `GET /metrics`, and (with `X-Internal-Token`)
`GET /internal/v1/admin/stats`, `POST /internal/v1/voice/nodes/{id}/drain`,
`POST /internal/v1/voice/nodes/{id}/undrain`.

## Run locally

Start the infrastructure from `gitops/local` (`docker compose --profile deps up -d`), then:

```sh
cp .env.example .env     # JWT_PUBLIC_KEY must be the public half of the api's JWT key
make dev
```

or run the image: `make docker && docker run --env-file .env -p 4000:4000 ghcr.io/discordy-voics/gateway:dev`.

## Test

```sh
make test          # 248 unit and application tests, no external services
make integration   # also runs the suites that start Valkey, etcd and Redpanda in Docker
make lint          # format, warnings-as-errors, credo --strict, sobelow
```

The Kafka integration suite needs the CRC32C NIF that brod uses, which only compiles where a C
toolchain is available (Linux). On Windows run it in a container, with a broker already running
and `KAFKA_TEST_PORT` set (see the moduledoc of `test/integration/kafka_test.exs`).

## Configuration

All configuration is read from the environment and validated at startup; every problem is
reported at once. Names follow P2.

| Variable | Required | Meaning |
|---|---|---|
| `DISCORDY_ENV` | no (`local`) | `local` or `production` |
| `LOG_LEVEL` | no (`info`) | log level; logs are JSON on stdout |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | enables OpenTelemetry export |
| `INTERNAL_SERVICE_TOKEN` | yes | shared secret for `X-Internal-Token` (inbound and outbound) |
| `JWT_PUBLIC_KEY` | yes | Ed25519 public key, PEM (`\n` escapes accepted) |
| `SNOWFLAKE_WORKER_ID` | no | integer in 128-191 (gateway range, P5); validated, the gateway mints no snowflakes |
| `KAFKA_BROKERS` | yes | `host:port,...` |
| `VALKEY_URL` | yes | `redis://host:port` |
| `ETCD_ENDPOINTS` | yes | `http://host:2379,...` |
| `TURN_STATIC_AUTH_SECRET` | yes | coturn `static-auth-secret`; mints TURN credentials |
| `TURN_URLS` | yes | comma-separated `turn:` URLs handed to clients |
| `API_INTERNAL_URL` | yes | api base URL |
| `MESSAGES_INTERNAL_URL` | yes | messages base URL |
| `VOICE_TOKEN_SECRET` | yes | HS256 secret, at least 16 bytes |
| `PUBLIC_GATEWAY_URL` | yes | advertised as `resume_gateway_url` |
| `DEFAULT_VOICE_REGION` | no (`nl-home`) | region used when guild and channel set none |
| `RELEASE_COOKIE` | prod | Erlang distribution cookie |
| `CLUSTER_DNS_QUERY` | no | headless-service DNS name; enables libcluster DNS polling |
| `PORT` | no (`4000`) | HTTP port |

## Pending owner actions

- `mix hex.audit` reports GHSA-w4f7-4cxr-rv3c (CVE-2026-43966, medium) against `gun 2.7.0` and
  `cowlib 2.21.0`, and no patched release exists yet. The gateway uses gun only as the etcd gRPC
  client towards a trusted internal service. The reusable `elixir.yml` workflow runs
  `mix hex.audit`, so CI fails until upstream publishes a fix or the owner decides to accept it.
- The Kafka integration suite has not been run on Windows (see above); it runs in a Linux
  container.
