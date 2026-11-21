# Gate gateway routing on cluster membership

Status: proposed for develop, 2026-11-18.

Gateway already uses cluster-wide `:pg` session lookup and Horde guild ownership. Its Kafka
consumer groups are deliberately shared: the replica assigned a partition routes to sessions
on every node. Giving each replica its own consumer group would duplicate dispatches.

Previously `/readyz` checked Valkey but did not check that gateway replicas could communicate.
A disconnected replica could admit sessions and acknowledge bus events against an incomplete
directory. Keep the existing topology and gate admission and consumption on its prerequisites.

`RoutingMembership` caches the DNS membership probe in ETS, checks current distribution
connections on every read, and verifies that remote `:pg` scopes have seen its marker. This
works with older gateway processes during rolling upgrades; every old replica need not run
the new monitor. A missing peer, failed DNS query, expired probe or stopped monitor closes the
gate. The DNS record must include unready pods to avoid a readiness/discovery deadlock.

Kafka callback workers wait before ingesting or acknowledging their current event; group
heartbeats run separately. `/readyz` reports `cluster: false` and new sockets get retryable
close 4015. Existing sockets remain attached and recover with the cluster. This prioritizes
correct routing over availability during a partition; DNS must reflect the intended running
replicas. It does not implement consensus or recreate a session whose owning process died.

Tests start two real BEAM nodes. They cover one shared guild dispatch, a WebSocket RESUME
through the other node, loss/recovery of membership and 20,000 same-millisecond UUIDs. With
`KAFKA_TEST_HOST` configured the fan-out test uses two consumers of a real disposable broker.
Separate regressions assert that admission closes and the Kafka callback does not acknowledge
until routing readiness returns. Voice allocation also excludes ambiguous node/endpoints.
