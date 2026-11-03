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
| Presence | Aggregated across a user's sessions (dnd > online > idle, invisible reads as offline), fanned out to guilds and friends; a friendship formed after READY (RELATIONSHIP_ADD, type 1) immediately hands both sides each other's current presence and tracks later changes. Client activities are sanitised (max 5, one custom status, text fields 128 characters, unknown fields dropped); an invalid activity closes the session with 4002 |
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
make test-kafka    # Kafka suite only, against a throwaway Redpanda (needs Docker, works on Windows)
make lint          # format, warnings-as-errors, credo --strict, sobelow
```

`make test-kafka` (`scripts/test-kafka.sh`) starts a single-node Redpanda container
(`gateway-redpanda`) on a dedicated network `gateway-kafka-net`, then runs
`test/integration/kafka_test.exs` in a Linux `hexpm/elixir` container on that network with
`KAFKA_TEST_HOST=redpanda` and `KAFKA_TEST_PORT=9092`. brod's CRC32C NIF needs a C toolchain, which
the container installs (`build-base`, `cmake`). The broker and network are removed on exit. Dependency
and build caches persist in the named volumes `gateway-kafka-deps` and `gateway-kafka-build`; remove
them with `docker volume rm` to start clean. Override `ELIXIR_IMAGE` or `REDPANDA_IMAGE` to pin other tags.

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
| `E2EE_SETTINGS_UNAVAILABLE_POLICY` | no (`open`) | what a voice join does when Valkey cannot tell whether the participants have `e2ee_calls_default` on: `open` joins unencrypted and logs a warning, `closed` encrypts the call, `wait` retries the read (3 attempts, about 2 s apart) and then refuses the join with `VOICE_JOIN_FAILED` `{"error": "e2ee_settings_unavailable"}` |
| `VOICE_DETACH_GRACE_MS` | no (`15000`) | how long a dropped connection keeps its voice state (guild voice or DM call) before the session leaves voice; shorter than the 120 s resume window so a closed or crashed client does not keep a ghost call open (the next call would join it and not ring) |
| `RELEASE_COOKIE` | prod | Erlang distribution cookie |
| `CLUSTER_DNS_QUERY` | no | headless-service DNS name; enables libcluster DNS polling |
| `PORT` | no (`4000`) | HTTP port |

## Security

**Accepted advisory: GHSA-w4f7-4cxr-rv3c (CVE-2026-43966, medium).** HTTP request/response splitting
via non-VCHAR bytes in `cow_http_struct_hd:escape_string/2` (cowlib), affecting `gun 2.7.0` and
`cowlib 2.21.0`. Hex has no patched release of either. Gun 2.1.0 (the version eetcd pins) is also
listed, so downgrading does not help.

- Risk assessment: the vulnerable function is reached only by cowlib's structured-field
  *serializers* (`list/dictionary/item`, used for WebTransport protocol headers). Gun calls only the
  response *parsers* (`parse_connection`, `parse_upgrade`, `parse_content_length`, ...), and the
  gateway never builds structured headers from user input. Gun's reachable surface in the gateway is
  the eetcd gRPC transport towards the internal etcd, and the WebSocket client in the tests.
- Accepted in `mix.exs` with `hex: [ignore_advisories: ["GHSA-w4f7-4cxr-rv3c"]]`, so `mix hex.audit`
  passes and only this advisory is ignored. Remove the entry as soon as Hex ships a patched gun or
  cowlib (`mix hex.outdated gun`, `mix hex.info cowlib`).

## Git hooks

Tests run locally, not in CI. Install [lefthook](https://github.com/evilmartians/lefthook) once and run `make hooks` (or `lefthook install`) to enable the pre-push hook in `lefthook.yml`: it runs this repository's lint and unit tests before every push. CI repeats only the slim gate on pull requests. Skip once with `LEFTHOOK=0 git push`.
