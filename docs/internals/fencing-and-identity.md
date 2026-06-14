# Fencing and mTLS identity

Two distinct safety layers protect BigFleet's actuation surface, and the codebase keeps them in two tiny packages. `pkg/fencing` answers *"is the sender of this instruction or mutation still the real authority, or a stale zombie?"* — term/epoch/sequence high-water marks that let a receiver refuse a superseded sender without any coordination. `pkg/grpcutil` answers *"is the peer on the other end of this connection who it claims to be?"* — opt-in file-based mTLS with a `bigfleet://` URI-SAN identity bound to the caller-asserted protobuf id. The first defends against split-brain after a failover or restart (a process that *was* authoritative); the second against impersonation across trust domains (a process that never was). They compose: fencing assumes the sender is authenticated as *a* shard, identity assumes the network may carry a hostile peer. This page opens both packages, traces where each token is stamped and checked, and ties the fenced-mutation path to ADR-0046's safety rails. Read [`../architecture.md`](../architecture.md) for the two-tier shape and [`static-stability.md`](static-stability.md) for why the data plane refuses coordinator dependencies; this page is the detail under paper §11 ("Fencing") and [ADR-0048](../adr/0048-mtls-and-uri-san-identity.md).

## Why fencing exists at all

BigFleet is a distributed actuator with two failure modes that ordinary retries make *worse*:

- **A zombie shard.** A shard process that GC-paused, partitioned, or was replaced after a crash can come back believing it still owns its inventory and resume issuing `Drain`/`Delete` to a provider against a fleet its successor has already moved on from. Money and workloads are at stake; the mutation is not idempotent against the *intervening* successor's actions.
- **A stale coordinator.** A coordinator replica that lost leadership but hasn't noticed can keep handing shards `CoordinatorInstruction`s — domain reassignments, cross-shard drains — from a term that a newer leader has superseded.

Neither is solvable by the sender being careful: a zombie *believes* it is healthy. The fix is a receiver-side monotonic high-water mark — the receiver remembers the newest authority it has heard from and rejects anything not strictly newer. `pkg/fencing` is just enough state to make that check correct and no policy beyond it (`pkg/fencing/fencing.go:23-24`). The two directions of paper §11 get one helper each, plus a sequence counter shared by the shard→provider path.

## Direction 1 — coordinator → shard: `CoordinatorTerm`

`CoordinatorTerm` (`pkg/fencing/fencing.go:43-74`) is a single `int64` high-water mark behind an `RWMutex`. `Validate(term)` rejects `term < hwm` with `ErrStaleTerm`, advances the mark on `term > hwm`, and — critically — *accepts equal terms*: one elected coordinator legitimately issues many instructions at the same term (`fencing.go:62-63`). On rejection the mark is unchanged (`fencing_test.go:38-49`), so a stale straggler never rewrites history.

The term itself is the Raft term. The coordinator stamps it onto every `ShardReportResponse` from `c.RaftTerm()`, read straight out of `raft.Stats()["term"]` (`pkg/coordinator/coordinator.go:313-329`, `pkg/coordinator/grpc_server.go:201`). A leadership change increments the Raft term by construction, so a deposed leader's responses carry a strictly-lower term than the new leader's — the mark does the rest.

The shard checks it in two places in `pkg/shard/coordclient`:

1. **The response envelope.** After each `ReportShard`, the whole instruction batch is gated on the response's term: a stale `coordinator_term` drops *every* instruction for that cycle without processing (`coordclient.go:222-235`). They re-arrive once the term mismatch resolves.
2. **Per instruction.** `handleInstruction` re-validates `instr.GetCoordinatorTerm()`; a stale token is acked `OUTCOME_REJECTED_STALE` and not executed (`coordclient.go:258-262`). The ack tells the (possibly-new) coordinator the shard refused it, rather than silently dropping.

Note the asymmetry with the proto: `CoordinatorInstruction` carries *both* `coordinator_term` and `sequence_number` (`api/proto/bigfleet/v1alpha1/coordinator.proto:257-262`), but the shard fences only on the **term** and de-duplicates on the stable `instruction_id`, not the sequence. A `doneInstructions` set re-acks an already-executed `instruction_id` as `OUTCOME_ACCEPTED`/`duplicate-redelivery` (`coordclient.go:248-256, 288`) — the coordinator redelivers until it sees the ack, so dedup must be idempotent. The term is the split-brain guard; the id is the exactly-once guard; the sequence number is reserved headroom, not load-bearing in the current receiver. The shard also publishes its own mark back to operators on the `Hello` ack (`CoordinatorTerm: s.term.HighWaterMark()`, `pkg/shard/session.go:73-75`) so the operator's view of "which coordinator term is current" tracks the shard's.

## Direction 2 — shard → provider: `Epoch` + `Sequence`

The provider direction can't use a term — there is no consensus group between a shard and an out-of-tree provider. Instead the shard carries a **persisted restart epoch** plus a **per-process sequence**, and the provider keeps the high-water mark.

`Epoch` (`pkg/fencing/fencing.go:80-115`) is loaded once at startup by `LoadEpoch(path)`: read the file, increment, write back, hand out the new value. A missing file starts at 1; a corrupt file is a hard startup error rather than a silent reset (`fencing.go:96-111`, `fencing_test.go:69-96`). The increment-on-load is the whole point: **every restart of the same `shard_id` fences strictly higher than any prior process of that id.** A zombie that comes back is, by definition, a process that loaded an *older* epoch — its successor already incremented past it — so its mutations are stale the moment the successor makes contact. The persistence is load-bearing; an in-memory epoch would reset to 1 on restart and a crashed-then-restarted shard could collide with a slow predecessor.

`Sequence` (`pkg/fencing/fencing.go:122-133`) is a mutexed monotonic counter, always non-zero, one fresh value per `Next()`. It orders mutations *within* an epoch.

`pkg/provider/grpcclient` is the only in-tree code that talks to a provider, and it stamps `(ShardID, Epoch.Value(), Seq.Next())` onto every **mutating** call — `Create`, `Configure`, `Drain`, `Delete` — from an `Identity` injected at construction (`grpcclient.go:43-52, 101-145`). `Get` and `List` carry no token: reads don't fence (`grpcclient.go:150, 165`; proto `provider.proto:41-42`). A *fresh* sequence is minted per call **attempt**, so a transport-level retry re-stamps and is never mistaken for a replay (`grpcclient.go:7-11, 117`); idempotency is the provider's job, keyed on `(machine_id, target_state)` via `operation_id`, never on the fencing token.

The provider's obligation is the contract in `provider.proto:13-42`: track, per `shard_id`, the highest `(shard_epoch, sequence_number)` pair compared **lexicographically** — `(e1,s1)` newer than `(e2,s2)` iff `e1>e2`, or `e1==e2 ∧ s1>s2`. A token not strictly newer is rejected with `FAILED_PRECONDITION` and **must not be applied** — the fence runs *before* idempotent-retry short-circuiting. A passing token advances the mark even if the operation then fails. First contact from an unknown `shard_id` is accepted and establishes the mark; a new epoch *resets* the sequence space (any sequence is acceptable once the epoch advances). `FAILED_PRECONDITION` is **reserved on this service for fencing** so callers can detect zombie incidents mechanically (`provider.proto:32-34`).

## How a fenced/stale shard is stopped from actuating

The fence is only useful if the shard *reacts correctly* to being fenced. `pkg/provider/grpcclient` re-attaches `provider.ErrFenced` whenever a mutating call returns `FAILED_PRECONDITION`, while leaving the original gRPC status in the error chain (`grpcclient.go:206-226`). That mapping is what converts a wire code into a typed zombie signal the shard can branch on.

In the shard's execute path, `classifyExecuteError` checks `errors.Is(err, provider.ErrFenced)` **before** the message-string buckets — deliberately, because a fenced rejection's message also matches the `"provider.Create/Configure/Drain"` substrings and must *not* land in the retryable `provider_error` bucket (`pkg/shard/execute.go:121-126`). The outcome is labelled `fenced`, and execute's deferred handler treats it as an incident, not a retry:

```
s.log.Error("provider fenced this shard's mutation — zombie-shard incident;
  do not retry, investigate duplicate shard identity", ...)
```
(`pkg/shard/execute.go:56-65`). The point is that a fenced mutation is *terminal* for this process: a newer epoch of the same `shard_id` already owns the fleet, so this process's whole view is untrustworthy and retrying is wrong, not merely futile. The outcome also flows into the ADR-0046 decision audit log via `auditAction` (`execute.go:66-70`), so an operator can replay exactly which mutations were fenced.

This is the defence-in-depth seam with [ADR-0046](../adr/0046-actuation-safety-rails.md). The fence stops a *stale-identity* shard; ADR-0046's reclaim blast-radius cap, empty-roll-up quarantine, and kill switch stop a *wrong-but-current* shard (an engine defect, a forged roll-up, a fleet-drain signal). Both live at the actuation boundary, both leave `pkg/decision` pure, and both fail safe by *not* executing. A zombie that somehow passed the fence (impossible once its successor has contacted the provider) would still be bounded by the 5%/cycle reclaim cap.

## Transport identity — ADR-0048, opt-in mTLS

ADR-0048 ([`../adr/0048-mtls-and-uri-san-identity.md`](../adr/0048-mtls-and-uri-san-identity.md)) supersedes ADR-0008's *transport* posture — "v1 ships unauthenticated, wrap it in a sidecar" is gone; ADR-0008's leader-only-RPC contract stands. The motivation is sharper than encryption: before this, the shard trusted the client-asserted `Hello.cluster_id`, so any network-reachable peer could impersonate any cluster — receive its reclaim instructions, or **zero its capacity with a forged full-replacement roll-up** (ADR-0048 §Context; the empty-roll-up guard of ADR-0046 is the mitigation, identity is the fix).

### Symmetric, all-or-none flags

`pkg/grpcutil/tls.go` implements the whole layer once, so every binary behaves identically. `TLSConfig` holds three symmetric flags — `--tls-cert`, `--tls-key`, `--tls-ca` — registered with the same names on every binary (`tls.go:41-61`):

- **All three set** → mTLS, TLS 1.3 minimum, `RequireAndVerifyClientCert` on servers, server verification on clients, both against the same CA bundle (`tls.go:99-110, 133-143`).
- **None set** → plaintext, byte-identical to today; the quickstart and scaletest harness stay zero-config (`tls.go:88-90, 122-123`).
- **Partial set** → startup error from `Validate()`, surfaced by both `ServerOptions`/`DialOptions` (`tls.go:71-76`, `tls_test.go:115-135`). A typo must not silently downgrade a deployment to plaintext.

One flag set covers *every edge of a process* — the shard's flags apply to its Session server, its coordinator-report dial, and its provider dial. One process, one identity, one certificate.

**Live rotation, startup-strict CA.** Leaf certs reload without a restart: `fileCertSource.current()` stats `tls.crt`/`tls.key` on every handshake and re-reads only on an mtime change, matching how cert-manager rewrites a mounted Secret in place (`tls.go:157-215`). Two rotation hazards are handled explicitly: a mid-rotation stat blip (Kubernetes' symlink swap) keeps serving the cached pair (`tls.go:186-192`), and a half-written rotation (new cert, old key that fails `LoadX509KeyPair`) keeps the last *coherent* pair until the files agree, with mtimes left stale so the next handshake retries (`tls.go:197-206`, `tls_test.go:298-307`). The CA bundle, by contrast, is read **once at startup** (`tls.go:48-51`); CA rotation is a restart done by trust-bundle overlap (add new CA, roll certs, remove old CA). `newFileCertSource` loads the pair once at construction so bad material fails at startup, not on the first live handshake (`tls.go:171-179`).

### `bigfleet://` URI-SAN identity

Identity rides on **exactly one** `bigfleet://` URI SAN of the client certificate (`tls.go:28-37`, helpers `tls.go:217-231`):

| URI SAN | Carried by |
|---------|-----------|
| `bigfleet://cluster/<cluster_id>` | the cluster operator (`ClusterURI`) |
| `bigfleet://shard/<shard_id>` | the shard, presented to coordinator and provider (`ShardURI`) |
| `bigfleet://admin` | bigfleetctl and the coordinator replicas themselves (`AdminURI`) |

`PeerIdentity(ctx)` (`tls.go:249-272`) extracts that SAN from the *verified* client chain and returns a three-valued result that encodes ADR-0048's "identity is only as strong as the transport" rule:

- `mtls=false` — not an mTLS connection (plaintext, or TLS without a verified client chain). **Identity checks are skipped**, deliberately (`tls.go:251-257`).
- `mtls=true, err=nil, uri=...` — the single `bigfleet://` SAN.
- `mtls=true, err!=nil` — zero or multiple `bigfleet://` SANs (`ErrNoIdentity`, `tls.go:236, 263-270`); the caller rejects with `PermissionDenied`. The cert authenticated *a* peer but asserted no usable BigFleet identity, or asserted ambiguously.

### Binding the SAN to the protobuf identity

Authenticating "some cluster" is not enough; the server must bind the SAN to the *specific* id the caller asserts in-band. Three enforcement points, all guarded by `if mtls` so plaintext is unaffected:

- **Shard Session.** On the `Hello`, when the transport is mTLS, the SAN must equal `ClusterURI(Hello.cluster_id)`; mismatch, missing, or ambiguous identity terminates the stream with `PermissionDenied`, a loud error log, and a `bigfleet_shard_session_identity_rejected` increment (`pkg/shard/session.go:47-63`). This is the line that closes the forged-roll-up impersonation vector — `Hello.cluster_id` is no longer free text.
- **Coordinator `ReportShard`.** `requireShardIdentity` binds the SAN to `ShardURI(ShardReport.shard_id)` (`pkg/coordinator/grpc_server.go:36-49, 133`). The binding is **strict, not hierarchical** — an admin cert does not pass as a shard.
- **Coordinator admin surface.** `requireAdminIdentity` requires exactly `bigfleet://admin` on `AssignDomain`, `UnassignDomain`, `RemoveShard`, `ListShards`, `ListDomainAssignments`, `ListQuotas`, `JoinRaftCluster`, `SnapshotSave` (`grpc_server.go:51-63, 301-348`). Coordinator replicas carry the admin SAN because they call `JoinRaftCluster` on each other (ADR-0047) and are inherently the admin domain.
- **Provider dial-out.** The shard *presents* `ShardURI(shard_id)`; enforcement is the provider's job — providers are out of tree, so the validation point is the provider boundary (ADR-0048 §2, ADR-0005). This is the identity counterpart to the epoch/sequence fence above: the SAN says *which* shard, the fence says *which generation* of it.

What stays out (ADR-0048 §3): Raft inter-replica transport TLS (a separate `raft.StreamLayer`, keep the Raft port cluster-internal until then), SPIFFE/SPIRE (the SAN convention is SPIFFE-shaped on purpose but v1 takes files on disk), in-chart cert generation (cert-manager is the documented issuer, charts take `kubernetes.io/tls` Secret names), and metrics/pprof (HTTP-plaintext — they carry no control authority).

## The test harness — `pkg/grpcutil/tlstest`

`pkg/grpcutil/tlstest` mints throwaway CAs and leaf certs for exercising the mTLS layer. It is a **test fixture only — never deployed, never checked-in key material**, the same posture as `pkg/provider/fake`, and it deliberately imports nothing from BigFleet so both internal and external `grpcutil` tests can use it (`tlstest/tlstest.go:1-6`). `NewCA` mints an in-memory P-256 CA; `Issue`/`WriteFiles` sign a leaf with the requested URI SANs and write `tls.crt`/`tls.key`/`ca.crt` in `--tls-cert`/`--tls-key`/`--tls-ca` order (`tlstest.go:46-144`). Every leaf carries **both** ServerAuth and ClientAuth EKUs because a BigFleet process uses one certificate for every edge it serves *or* dials — the symmetric-flags design (`tlstest.go:77-79`). `LeafOpts.URIs` is verbatim, so a test can pass one SAN (the normal case), several (to exercise the exactly-one rejection), or none (an identity-free cert).

The table this drives in `tls_test.go` is the behavioural spec of the whole layer: the plaintext zero value reports `mtls=false` and skips identity (`tls_test.go:98-111`); every partial flag combination errors (`:115-135`); a same-CA handshake surfaces the client's SAN through `PeerIdentity` (`:140-157`); a wrong-CA client fails the handshake before the server sees the call (`:161-178`); a certificate-less TLS client is refused (`:183-204`); two SANs and zero SANs both produce the identity error while the *transport* still succeeds (`:209-246`); and `TestFileCertSource_ReloadOnMtime` proves the rotation contract including the half-written-rotation fallback (`:251-308`).

## Where this sits in the source-of-truth order

Paper §11 fixes the two fencing token *shapes* (`coordinator_term, sequence_number` and `shard_id, shard_epoch, sequence_number`); the code realises them with the receiver-side high-water marks above and, in the coordinator→shard direction, fences on the term while deduping on `instruction_id` — a divergence from a literal reading of "(term, sequence)" that the proto and `coordclient.go` make explicit and that is correct (the id is the stronger exactly-once key). ADR-0048 is the authority for everything in the transport-identity half; it supersedes ADR-0008's transport posture only, and the ADR's own consequences note that with identity in place the ADR-0046 roll-up guard demotes from "only defence" to defence-in-depth — which is exactly the layering this page traces.
