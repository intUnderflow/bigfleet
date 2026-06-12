# ADR-0048: Opt-in file-based mTLS with bigfleet:// URI SAN identity binding

## Status

Accepted, 2026-06-12. Ops scope — M74 (plan §12). Supersedes
ADR-0008's transport posture (the leader-only read/write contract in
ADR-0008 stands; "v1 ships unauthenticated, wrap it in a sidecar" does
not).

## Context

The production-readiness audit (docs/production-readiness-2026-06.md,
arc 4) verified that every BigFleet surface is plaintext with no
caller identity: the operator→shard Session, shard→coordinator
(including M75's JoinRaftCluster), the coordinator admin RPCs, and
the M71 shard→provider dial-out. Worse than missing encryption is the
missing binding: the shard trusts the client-asserted
`Hello.cluster_id`, so any network-reachable client can impersonate
any cluster — receive its reclaim instructions, or zero its capacity
with a forged full-replacement roll-up. ADR-0046's empty-roll-up
guard mitigates the most destructive forgery; identity is the actual
fix.

ADR-0008 chose trust-the-network deliberately for the reference impl.
That stance was honest for a single-cluster dev posture; it is wrong
the moment a shard is reachable from more than one trust domain,
which is the entire premise of a fleet-level autoscaler.

## Decision

### 1. File-based mTLS, opt-in, symmetric flags

Every server (shard, coordinator) and every client (operator,
coordclient, provider grpcclient, bigfleetctl) takes the same three
flags — `--tls-cert`, `--tls-key`, `--tls-ca` — implemented once in
`pkg/grpcutil` so every binary gets identical behaviour:

- **All three set**: mTLS. Servers require and verify client
  certificates against the CA bundle; clients verify servers against
  the same bundle. TLS 1.3 minimum.
- **None set**: plaintext, exactly today's behaviour. The quickstart,
  the scaletest harness, and every existing chart keep working with
  zero changes.
- **Partial set**: startup error. A typo must not silently downgrade
  a deployment to plaintext.

One flag set covers every edge of a process: the shard's flags apply
to its Session server, its coordinator report dial, and its provider
dial. One process, one identity, one certificate.

**Certificate reload**: the `GetCertificate` / `GetClientCertificate`
callbacks stat the files on every handshake and re-read them when
either mtime changes — a stat is cheap and this is exactly how
cert-manager rotates a mounted Secret, so rotation needs no restart
and no reconnect storm. A half-written rotation (new cert, old key)
keeps serving the previous coherent pair until the files agree. The
**CA bundle is read once at startup**; rotating the CA requires a
restart (do CA rotation by trust-bundle overlap: add the new CA to
the bundle, roll certs, remove the old CA — restarting at each step).

### 2. Identity: bigfleet:// URI SANs

Caller identity is a URI SAN on the client certificate, exactly one
`bigfleet://` URI per certificate:

| SAN | Who carries it |
|-----|----------------|
| `bigfleet://cluster/<cluster_id>` | the cluster operator |
| `bigfleet://shard/<shard_id>` | the shard (presented to coordinator and provider) |
| `bigfleet://admin` | bigfleetctl and the coordinator replicas themselves |

Bindings enforced when (and only when) the transport is mTLS:

- **Shard Session**: the certificate's SAN must equal
  `bigfleet://cluster/<Hello.cluster_id>`. Mismatch — or zero or
  multiple bigfleet:// SANs — terminates the stream with
  `PermissionDenied`, a loud log, and a
  `bigfleet_shard_session_identity_rejected_total` increment (alert
  on any non-zero rate).
- **Coordinator ReportShard**: same binding for
  `bigfleet://shard/<ShardReport.shard_id>`. Admin certs do NOT pass
  — the binding is strict, not hierarchical.
- **Coordinator admin surface** — everything M15/M24/M75 expose:
  `AssignDomain`, `UnassignDomain`, `RemoveShard`, `ListShards`,
  `ListDomainAssignments`, `ListQuotas`, `JoinRaftCluster`,
  `SnapshotSave` — requires `bigfleet://admin`. Coordinator replicas
  carry the admin SAN on their own certificate: they call
  `JoinRaftCluster` on each other (ADR-0047) and are inherently the
  admin domain.
- **Provider dial-out**: the shard presents
  `bigfleet://shard/<shard_id>`; enforcement is the provider's job
  (providers are out of tree — the validation point is the provider
  boundary, ADR-0005).

**Plaintext mode skips every check.** This is deliberate and
documented rather than papered over: identity is only as strong as
the transport. A plaintext deployment is making the ADR-0008
trust-the-network choice, now opt-in instead of the only option.

### 3. What stays out

- **Raft transport TLS is out of scope.** hashicorp/raft's TCP
  transport (`raft.NewTCPTransport`) is a separate stream from the
  gRPC stack — securing it means swapping in a
  `raft.StreamLayer` that wraps the listener and outbound dials in
  TLS, with its own (non-gRPC) handshake configuration. That is
  mechanical but independent work with its own test surface; bundling
  it here would couple a follow-up to this ADR's gRPC scope. Until
  then: the Raft port carries coordinator state between replicas in
  one management cluster — keep it on a cluster-internal network
  (NetworkPolicy), exactly the ADR-0008 posture, and treat the
  follow-up as the remaining item of plan §12 security work.
- **No SPIFFE/SPIRE.** The SAN convention is SPIFFE-shaped on
  purpose (a future workload-identity integration maps cleanly), but
  v1 takes files on disk and lets cert-manager do issuance. No new
  dependencies; stdlib crypto/x509 plus the grpc credentials package
  already in the tree.
- **No in-chart certificate generation.** Charts take existing
  Secret names (`kubernetes.io/tls` layout: `tls.crt`, `tls.key`,
  `ca.crt`). cert-manager is the documented issuer; the
  operator-guide carries the Certificate manifests with the URI SAN
  convention.
- **Metrics/pprof stay HTTP-plaintext.** They already carry no
  control authority; the coordinator's kubelet probes actually move
  TO the metrics port under mTLS, because kubelet's gRPC probe
  cannot present a client certificate.

## Consequences

- Impersonating a cluster now requires the CA to misissue, not just
  network reach. The ADR-0046 roll-up guard demotes from "only
  defence" to defence-in-depth.
- Multi-shard chart installs need per-shard certificates (the SAN
  embeds the shard_id, and a StatefulSet mounts one Secret for all
  replicas). The reference chart documents the single-replica case;
  per-ordinal Secret overlays are the operator's composition.
- The scaletest harness, quickstart, and all-in-one keep running
  plaintext with zero changes — `make prevalidate` and the kind rung
  are unaffected.
- bigfleetctl against an mTLS coordinator needs the admin cert files;
  the "run it as a Job in the management cluster" pattern mounts the
  same Secret the coordinator uses.
- ADR-0008's sidecar guidance is obsolete; its leader-only RPC
  contract is untouched.
