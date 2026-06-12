# ADR-0047: Coordinator quorum formation by ordinal join; offline snapshot restore as single-voter recovery

## Status

Accepted, 2026-06-12. Ops scope — M75 (plan §12). Companion to
ADR-0002 (single Raft group) and ADR-0008 (admin surface posture).

## Context

The production-readiness audit (docs/production-readiness-2026-06.md,
arc 5) verified that the documented 3-replica HA install bootstraps
**three independent single-node Raft clusters**: every replica got
`--bootstrap`, `coordinator.AddVoter` existed with zero callers, and
the chart had no join mechanism. Each replica elected itself leader
of its own cluster; whichever one a shard's heartbeat reached owned a
private copy of fleet state. The same audit found a snapshot export
loop that nothing could restore from — no tooling, no procedure.

Two design choices needed making beyond the conventional patterns:
where the bootstrap/join split lives, and what membership a restored
snapshot carries.

## Decision

### Quorum formation

The conventional hashicorp/raft StatefulSet pattern: **ordinal 0
bootstraps; ordinals >0 start unbootstrapped and join by asking the
leader to AddVoter them.**

- **The split lives in the binary, keyed on the ordinal in `--id`.**
  The image is distroless — there is no shell in the pod to vary args
  per ordinal — so the chart passes identical args to every replica
  (`--bootstrap` plus `--join-addr=<headless Service>:<grpc port>`)
  and `bigfleet coordinator` parses the StatefulSet ordinal from its
  own `--id` (the same parsing the shard already does for seed
  striping). Ordinal 0 honours `--bootstrap` (still gated on
  `raft.HasExistingState`, so it never re-bootstraps over data);
  every other ordinal ignores it and joins. Without `--join-addr`
  nothing changes: dev, all-in-one and test single-node runs keep
  plain `--bootstrap`.
- **Join is one leader-only admin RPC**, `JoinRaftCluster(node_id,
  raft_address)`, on the existing M15 coordinator surface — not a new
  mechanism, port, or sidecar. ADR-0008's open-trust posture is
  unchanged; M74 authenticates this RPC along with the rest of the
  surface.
- **The join loop is a membership reconciler, run on every ordinal.**
  It retries with backoff, dialing `--join-addr` fresh per attempt
  (fresh dials re-resolve DNS, so pointing the flag at the headless
  Service spreads attempts across replicas until one is the leader;
  followers answer `FailedPrecondition` like every other admin RPC),
  and exits only when this replica observes a leader *and* is a voter
  at its **current** advertise address. The address half is
  load-bearing in Kubernetes: the stock raft TCP transport advertises
  a resolved IP (`ResolveTCPAddr` on the pod DNS name), and every pod
  restart changes that IP — the cluster's configuration keeps dialing
  the dead one until an `AddVoter` rewrites it. A restarted replica
  that won an election anyway (it dials peers outbound) fixes its own
  entry locally instead of calling the leader-only RPC.
- **Re-join is idempotent.** hashicorp/raft's `AddVoter` of an
  existing voter rewrites the address in place rather than appending
  a duplicate membership entry (pinned by
  `TestAddVoter_ExistingVoterIsIdempotent`), and a replica restarting
  with an unchanged address hears the leader's heartbeat before its
  first attempt fires and exits without joining.
- **Chart consequences of leadership-gated readiness.** Readiness is
  `grpc_health_v1` on the named Coordinator service, SERVING once the
  replica observes a leader; liveness is the default service, SERVING
  for the life of the process (quorum loss must never restart-loop
  the replicas that could re-form it). That forces
  `podManagementPolicy: Parallel` (OrderedReady deadlocks a full
  restart: pod 0 can't be ready without a quorum that needs pod 1)
  and `publishNotReadyAddresses: true` on the headless Service (the
  leader must resolve a not-yet-ready joiner's DNS to replicate to
  it).

### Offline restore

`bigfleetctl snapshot save <file>` streams the leader's freshest Raft
snapshot over a new leader-only `SnapshotSave` RPC into a single-file
archive (same meta.json + state payload the export loop writes, so
exported directories restore identically). `bigfleetctl snapshot
restore` is **offline** — it rebuilds a STOPPED coordinator's data
dir; there is deliberately no online restore RPC for v1.

hashicorp/raft restore semantics: at startup, raft installs the
newest snapshot in its store — FSM state *and* the membership
configuration recorded in the snapshot's meta. Restore exploits that
by writing the snapshot back with a **single-voter configuration**
(just the restoring node) instead of the original membership, so the
restored node elects itself immediately instead of waiting for quorum
among peers whose data is gone — the same single-survivor shape as
hashicorp's `peers.json` / `RecoverCluster` recovery. The fresh
stable store is seeded with `CurrentTerm = snapshot term` so
post-restore entries don't regress below the snapshot's term. The
other replicas restart with **empty data dirs** and re-form the
quorum through the join path above. Full procedure: operator guide →
Disaster recovery.

What a restore loses is bounded by the export interval (default 5m):
shard registrations, cluster→shard bindings, and domain assignments
committed since the snapshot. All three self-heal — shards re-register
on their next heartbeat, and an unbound cluster re-binds on first
contact — but a re-bind may land on a *different* shard than the lost
binding, so machines owned by the original shard are reclaimed as
unattributed rather than recognised. Static stability bounds the blast
radius: shards and clusters keep running through the entire outage and
restore; only new cross-shard placement decisions wait.

## Consequences

- The 3-replica chart actually forms a 3-voter cluster, kills a
  leader, and keeps committing (pinned by
  `TestCoordinator_ThreeNodeQuorum_JoinAndFailover`).
- `coordinator.bootstrap=true` is now safe to leave set for the life
  of the install; the old "flip to false after first install"
  ceremony is gone.
- Known footgun, accepted: if ordinal 0 *alone* loses its PVC while
  1 and 2 keep theirs, ordinal 0 sees no existing state and mints a
  fresh single-node cluster — the standard hazard of this pattern.
  The old cluster (1, 2) retains quorum and keeps working; the
  recovery is to wipe ordinal 0's data dir again and let it join via
  the leader, not to restore. Documented in the DR runbook.
- The join RPC widens the unauthenticated admin surface by one
  membership-changing call until M74 lands. Same posture as
  `RemoveShard`, which could already evict state.
