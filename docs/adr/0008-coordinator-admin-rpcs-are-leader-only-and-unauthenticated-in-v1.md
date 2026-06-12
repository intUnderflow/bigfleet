# ADR-0008: Coordinator admin RPCs — leader-only, unauthenticated in v1, sidecar for external

**Status**: Accepted; the transport/authn posture (unauthenticated +
sidecar-for-external) is superseded by
[ADR-0048](0048-mtls-and-uri-san-identity.md), which adds opt-in
mTLS with URI SAN identity on every BigFleet transport. The
leader-only RPC contract below stands.

**Date**: 2026-05-05

## Context

M15 added a coordinator admin surface: `AssignDomain`, `UnassignDomain`, `RemoveShard`, `ListShards`, `ListDomainAssignments` (M24 added `ListQuotas`). The user-stories Mode-2 runbook ("push a config change through Raft via the coordinator's gRPC admin endpoint") was previously unimplementable — the FSM commands existed but had no externally-callable RPC.

Two open design questions when adding the surface:

1. **Where does authn / authz live?** The coordinator runs in a management cluster. It already authenticates to nothing — `ReportShard` accepts any caller that can reach `:7790`. Adding admin RPCs without authn is consistent with that posture; adding them WITH authn means picking an authenticator (mTLS? OIDC? service-account tokens?) and shipping it in v1.

2. **Reads on followers?** `ListShards` / `ListDomainAssignments` are pure reads of `State`, which is RLock-safe. We could serve them from any replica, which would offload the leader. The cost: stale reads after a leader failover. v1 doesn't have a freshness contract documented anywhere, and adding "this read may be N seconds stale" complicates the on-call mental model.

## Decision

All admin RPCs are leader-only. Followers reject with `FailedPrecondition` (mirroring `ReportShard`). Reads go through the leader's `State` RLock — fresh, consistent, no stale-read footgun, no client-side leader-cache logic needed.

v1 ships unauthenticated. The coordinator's gRPC service is a cluster-internal endpoint; protecting it is the cluster operator's responsibility:

- In-cluster callers (shards, `bigfleetctl` running as a Job): NetworkPolicy that limits ingress to `:7790` from the bigfleet-system namespace.
- External operators (a human running `bigfleetctl` from a workstation): a sidecar (envoy / linkerd / istio) that terminates mTLS and forwards plaintext to the local coordinator. The sidecar is NOT bundled with the bigfleet helm chart.

`bigfleetctl` is the canonical client. It dials with insecure transport credentials by default; if a real deployment needs mTLS, an operator wraps the binary in their own client-side TLS terminator.

## Consequences

- **One client to keep coherent.** `bigfleetctl` is the one place that wraps every admin RPC. UI consistency (tabwriter, exit codes, error wrapping) is solved once.
- **No in-tree authn means no in-tree authz drift.** The platform team's identity solution is whatever they already use for cluster-internal traffic; we don't pick winners.
- **Documented assumption in the operator-guide.** The operator-guide names the unauthenticated-by-default posture so a deployment that exposes the coordinator's port externally without a sidecar is doing so with eyes open.
- **Followers serve nothing.** After a failover, `bigfleetctl list-shards` against the (now-follower) old leader fails with `FailedPrecondition`; the client is expected to re-resolve the new leader. Pre-v1 the caller did this manually; if leader-aware client-side discovery becomes a real need a future ADR will revisit.
- **Quota writes deferred.** `SetQuota` is intentionally absent from M15. Coordinator state has `SetQuota` and there's a `MakeSetQuotaCommand` in the FSM, but no RPC. The operational story for v1 is "quota is initial-bootstrap data; if you need to change it, restart the coordinator with new bootstrap state." Adding the write RPC is straightforward when needed; we just haven't shipped it without a real request.
