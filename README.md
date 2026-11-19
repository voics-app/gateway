# Voics gateway

The realtime edge of Voics, written in Elixir/Phoenix. It terminates client WebSockets
(`wss://<gateway>/gateway?v=1&encoding=json`, protocol P7), consumes the event bus (P6) and
fans events out to the right sessions, allocates voice nodes (P8), and relays end-to-end
encrypted messages, P2P signalling, telemetry and Watch Together.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the structure and the C4 diagram.

Read-state hydration follows contracts#48 / docs ADR0027. IDENTIFY registers the user session
before fetching the messages `/internal/v1/users/{id}/read-states` array, so updates received
during the fetch follow READY. `X-Read-State-Versioned: 1` certifies all entries (including an
empty array); malformed or failed reads fail identification with retryable4000 rather than
inventing zero counts. Legacy arrays hydrate without certification. Certified RESUME fetches
again, replays, emits READ_STATE_SYNC and then RESUMED; clients merge by per-channel version.
Lost certification or a failed refresh invalidates resume. This repairs missed publication on
reconnect, not failed accounting before commit. After database restore, invalidate gateway
sessions and resume buffers so a fresh READY starts the restored version history.

## What it does

| Area | Behaviour |
|---|---|
| Sessions | HELLO, IDENTIFY (EdDSA user JWT, or bot token via api introspection), READY from the api, heartbeat supervision, 120 sends / 60 s rate limit (close 4008), RESUME from the Valkey buffer within 120 s |
| Guild processes | One cluster-wide process per guild (Horde), loaded lazily from `/guilds/{id}/state`, kept current from `discordy.guild.v1`; fan-out filtered by membership, intents and VIEW_CHANNEL with the Discord permission algorithm. Private threads are visible to their creator, thread members and members with MANAGE_THREADS. |

Private thread membership is received from the internal guild seed and versioned membership events.
Internal membership lists are removed before client dispatch. Deploy gateway support before the
API starts sending those fields; older gateways can expose unknown guild-seed fields to clients.
| Event consumers | One brod consumer group per topic (`discordy-gateway.<topic>`), P6 routing, idempotent on envelope `id` (LRU of the last 10k ids per node) |
| Presence | Aggregated across a user's sessions (dnd > online > idle, invisible reads as offline), fanned out to guilds and friends; a friendship formed after READY (RELATIONSHIP_ADD, type 1) immediately hands both sides each other's current presence and tracks later changes. Client activities are sanitised (max 5, one custom status, text fields 128 characters, unknown fields dropped); an invalid activity closes the session with 4002 |
| Voice | Least-loaded node of the region from etcd (P8), allocation in Valkey `voice:alloc:<id>`, one node per guild or DM call, reallocation when a node leaves etcd, `VOICE_SERVER_UPDATE` with an HS256 token, drain and undrain |
| E2EE relay | Op 20/21: per-device Valkey streams, ACK deletes, 7-day TTL sweeper, replay after READY |
| P2P | Op 22 relayed to online devices only; offline peers get `P2P_SIGNAL_FAILED {"error":"peer_offline"}`. READY carries short-lived TURN credentials (coturn REST scheme) built from `TURN_STATIC_AUTH_SECRET` and `TURN_URLS` |
| Telemetry | Op 23 published as `VOICE_QUALITY` and a per-node `USAGE_TICK` on `discordy.telemetry.v1`; Watch Together (op 24) stamped with the server clock |

HTTP endpoints: `GET /healthz`, `GET /readyz`, `GET /metrics`, and (with `X-Internal-Token`)
`GET /internal/v1/admin/stats`, `POST /internal/v1/voice/nodes/{id}/drain`,
`POST /internal/v1/voice/nodes/{id}/undrain`.

## Passive sessions (experimental)

A certified read-state user can opt in with `passive: true` in IDENTIFY or send
SESSION_MODE (op 27) with `{"passive": true}`. Every five seconds, when there
are changes, PASSIVE_UPDATE_V2 coalesces channel latest-message markers, the
user's versioned read states and guild voice changes. Guild channel and voice
entries are rechecked against current view permission before delivery. Unread
means the existing client count of unread channels, derived from message and
read markers; it is not an exact count of unread messages. The current messages
store does not roll the latest marker back on deletion, so this has the same
limitation as the existing unread indicator.

SESSION_MODE with `{"passive": false}` fetches a fresh certified read-state
snapshot and refreshes guild snapshots before resuming guild event delivery.
A session that has used passive mode must IDENTIFY again after disconnect; it
cannot replay stale passive frames through RESUME. Missing or lost read-state
certification causes a retryable close. These options require a compatible
client and remain opt-in.

## Multiple gateway replicas

Every gateway pod needs a unique `POD_IP`, the same `RELEASE_COOKIE`, and a
`CLUSTER_DNS_QUERY` resolving the headless Service, including unready pods. EPMD and
Erlang distribution must be reachable between pods. Keep one shared Kafka consumer group
per topic: its consumer routes through the cluster-wide session and guild directories.

With discovery configured, `/readyz` includes `cluster`. Admission returns retryable close
4015 until discovered nodes are connected and session directories have exchanged membership.
A consumer that detects unavailable routing before ingestion holds its pending event until routing recovers.
`discordy_gateway_cluster_ready` exports this gate. A DNS failure also closes the gate.
This check does not make asynchronous fan-out transactional: a partition or process loss during
an already-started delivery still needs client resynchronization.

RESUME through a different replica reattaches the original session process. If that process
dies, clients must IDENTIFY again: the replay buffer alone cannot reconstruct authentication,
subscriptions and voice membership. IDs are UUIDv4/v7; this service does not use its optional
`SNOWFLAKE_WORKER_ID` setting.

Run the two-node proof, including real WebSocket reconnect, partition recovery, fan-out and
concurrent IDs:

```sh
elixir --sname gateway_cluster --erl '+S 2' -S mix test --include cluster test/integration/cluster_test.exs
```

Set `KAFKA_TEST_HOST` and `KAFKA_TEST_PORT` to a disposable broker with the P6 topics to route
the fan-out test through two real Kafka consumers; without them it enters at `Events.Ingest`.

Voice allocation excludes nodes with duplicate IDs, WebSocket URLs, or public IP/UDP-port
pairs, including cached allocations. Each voice endpoint must route to its selected signaling
process and SFU; a generic round-robin voice Service cannot provide that affinity.

Prepared active-health support also checks cached allocations against node/region blacklists
and leased probe results. A node advertising `probe_required: true` must have a healthy result
for its exact current registration lease. Missing, unknown, stale or malformed observations
exclude it. Legacy registrations default to no requirement; explicit unhealthy results and
manual blacklist markers still exclude them. Existing calls keep their registered node.
Discovery watches are backed by one linearizable snapshot per second and a two-second
monotonic freshness limit, so a stalled watch cannot preserve allocation readiness. This
does not start an observer or enable probe enforcement; see central ADR-0025.

### Mobile background presence

P7 presence accepts `background: true` from mobile user sessions. The client sends automatic
Idle or its explicitly chosen Online/Idle/DND/Invisible before OS suspension. A detached session
keeps that availability indication for `MOBILE_BACKGROUND_PRESENCE_MS` (24 hours by default),
then expires and broadcasts the remaining devices' aggregate. This is a bounded lease, not
detection of an iOS force-quit. There is no background audio or fake client heartbeat to keep it alive.

Foreground presence, successful RESUME, logout/session revocation, and a replacement IDENTIFY for
the same mobile login cancel the old lease. Desktop and bot sessions keep the ordinary resume
window. The retained process remains reachable for revocation; the resume buffer still has its
ordinary retention and a missing buffer requires a fresh IDENTIFY. After a gateway crash, its
presence entries expire within the ordinary 120-second store TTL. Each device refreshes only
its own entry, and READY reads live entries instead of a stale cached aggregate.

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

## Benchmarks

`mix test --include benchmark` runs the local capacity benchmarks in `test/benchmark/` (sessions of a
large guild: memory per session; one-guild
fan-out: deliveries per second and consumer-to-socket latency, optionally with stalled readers). They
need a Valkey reachable at `BENCH_VALKEY_URL` (for example `docker run -d -p 6379:6379
valkey/valkey:8-alpine`) so the resume-buffer writes are part of the measurement. Sizes:
`BENCH_SESSIONS` (300), `BENCH_GUILD_MEMBERS` (200), `BENCH_RECEIVERS` (50), `BENCH_MESSAGES` (6000), `BENCH_RATE` (messages per second, 0 = as fast as
possible), `BENCH_SLOW` (stalled readers, 0), `BENCH_STAGES=1` (per-stage timings; the handler slows
the run). The figures are local to the machine, not cluster figures.

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
| `MESSAGES_INTERNAL_URL` | yes | messages base URL; DM calls post and update their call system message (type 3) there |
| `VOICE_TOKEN_SECRET` | yes | HS256 secret, at least 16 bytes |
| `PUBLIC_GATEWAY_URL` | yes | advertised as `resume_gateway_url` |
| `DEFAULT_VOICE_REGION` | no (`nl-home`) | region used when guild and channel set none |
| `E2EE_SETTINGS_UNAVAILABLE_POLICY` | no (`open`) | what a voice join does when Valkey cannot tell whether the participants have `e2ee_calls_default` on: `open` joins unencrypted and logs a warning, `closed` encrypts the call, `wait` retries the read (3 attempts, about 2 s apart) and then refuses the join with `VOICE_JOIN_FAILED` `{"error": "e2ee_settings_unavailable"}` |
| `VOICE_DETACH_GRACE_MS` | no (`15000`) | how long a dropped connection keeps its voice state (guild voice or DM call) before the session leaves voice; shorter than the 120 s resume window so a closed or crashed client does not keep a ghost call open (the next call would join it and not ring) |
| `MOBILE_BACKGROUND_PRESENCE_MS` | no (`86400000`) | positive, bounded availability lease after a mobile user session reports background suspension; revoked, foreground, resumed and replaced sessions clear it |
| `ADMISSION_MEMORY_FRACTION` | no (`0.75`) | share of the memory limit at which new connections are refused with the retryable close code `4015` `SERVER_OVERLOADED`; they are accepted again 5 points below it. Connected sessions are never dropped by it |
| `MEMORY_LIMIT_BYTES` | no | memory limit admission control measures against; defaults to the container cgroup limit (v2 `memory.max`, then v1), and without any limit admission control is off |
| `TRANSPORT_BACKLOG_LIMIT` | no (`2000`) | frames a socket may have queued before its session cuts the connection as a slow consumer; the frames stay in the resume buffer and the client reconnects and resumes, so one stalled reader cannot grow gateway memory without bound |
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

### Gateway compression

Connect with `v=1&encoding=json&compress=zstd-stream` to receive binary frames in one
continuous zstd stream per WebSocket connection. Each event is flushed so it can be
decoded immediately. Client-to-server frames remain JSON text. A reconnect starts a
new stream, including when resuming a session. Without the parameter, server frames
remain JSON text. Unknown compression values are rejected. OTP 29 is required.

Run `mix run --no-start bench/compression.exs` to compare event bytes in a sample stream.
This is a small illustrative payload, not a production bandwidth measurement.
