# Production-readiness audit — June 2026

A multi-agent audit (2026-06-11) of the distance between this
reference implementation and a deployment an operator could trust
with real machines, real workloads, and real money. Seven dimension
auditors plus a completeness critic produced 44 blocker claims, every
one of which was then handed to an independent adversarial verifier
instructed to refute it against the code. **38 sustained, 6
downgraded to hardening items, 0 refuted.** This document is the
distilled verdict and the evidence base for the production
milestone ladder (plan §12).

## Bottom line

The decision engine is a faithful, well-tested render of the papers —
the locked cost formula, victim scoring, drain-grace table, machine
state machine, penalty bucketing, full-replacement roll-ups, the
outbound-only operator, coordinator→shard fencing, and structurally
enforced static stability all verify byte-for-byte. But the system
today cannot manage a single real machine, cannot be trusted with
reclaim decisions, and has no security boundary. Nothing found
contradicts the architecture: every blocker is an unbuilt half or a
known defect, not a design flaw.

## The five blocker arcs

### 1. The provider edge does not exist

- No dial-out client: the shard hardcodes the in-tree test fake
  (`cmd/bigfleet/shard.go` provider construction; no `--provider-addr`
  flag exists), inverting the "fake is never deployed" hard rule. Two
  in-repo doc comments claim the dial-out path exists; both are false.
- No real out-of-tree provider exists anywhere, including the worked
  example the provider-author guide promises.
- Shard→provider fencing (paper §11 MUST) is absent from the wire:
  the lifecycle RPCs carry no `shard_id`/`shard_epoch`/`sequence_number`,
  `fencing.Sequence` has zero non-test callers, and the fencing
  package's doc comment claiming the edge exists is false. A zombie
  shard can issue Drain/Delete with nothing provider-side to reject.
- The wire contract cannot round-trip cluster bindings or assigned
  priorities/penalties, so a shard restart loses the state that
  protects running workloads.
- `provider.Delete` is never called from the engine: there is no
  Idle→Speculative release (paper §8 MUST), so elastic capacity is
  held — and billed — forever. Spend only ratchets upward.

### 2. The engine cannot yet be trusted with reclaims

- Consumed capacity is invisible (engine task: Phase 1 credits open
  demand against gross machine Allocatable; nothing models bound-pod
  consumption). Observed live: fills stall at ~96 % with
  `p1_unsatisfied=0` while the scheduler holds unplaceable pods, and
  Phase 3 reclaims machines under the pending demand.
- Dual supply-attribution: Phase 1 and Phase 3 independently derive
  who-keeps-what — the bootstrap≈reclaim oscillator, patched by
  mirroring four times since ADR-0027 instead of unified.
- Phase 3 Reclaims bypass the operator's cordon/PDB/evict path
  entirely and call `provider.Drain` with grace 0 — only Preempts
  route through the operator. Combined with the attribution defect,
  routine scale-down kills live workloads ungracefully. (The
  phase3 code comment and user-stories' PDB claim are both false for
  this path.)
- Shard restart zeroes every machine's AssignedPriority/penalties
  (provider List cannot return BigFleet-side state), removing
  preemption protection fleet-wide until re-learned.
- Phase 2 preemption is topology-blind and observably never fires
  for Same gangs.

### 3. No safety systems around the money path

- No reclaim circuit breaker, kill switch, or blast-radius cap
  anywhere: an empty full-replacement roll-up legally drains a
  cluster's entire fleet in one cycle.
- Provider-declared `price` and `interruption_probability` enter the
  cost formula unvalidated; `machine.Validate` exists as dead code
  while a doc comment claims it runs.
- No dry-run/shadow mode; no rollback guidance; no durable audit
  trail for lifecycle decisions that spend money (`Action.Reason` is
  documented as safe-to-drop telemetry).

### 4. Zero security

Every surface — operator→shard Session, shard→coordinator, the
coordinator admin RPCs, Raft, metrics/pprof — is plaintext and
unauthenticated. The shard trusts the client-asserted
`Hello.cluster_id`, so any party with network reach can impersonate
any cluster: receive its reclaim instructions or zero its capacity
with a forged full-replacement roll-up. ADR-0008's trust-the-network
stance was a deliberate reference-impl choice; the trust boundary
BigFleet itself defines (per-cluster identity) is enforced nowhere.
Supply chain (signing, SBOM, dependency automation) is unattended.

### 5. Operations and scale validation

- The documented 3-replica HA install bootstraps three independent
  single-node Raft clusters: `AddVoter` has no callers and the chart
  has no join mechanism. No restore tooling or DR procedure exists.
- The operator and unschedulable-pod-controller images referenced by
  the published charts are never built by CI: the documented install
  ImagePullBackOffs.
- `buf breaking` is configured but never run; v1alpha1 wire surfaces
  are actively reshaped with no skew contract (the operator's
  `protocol_version` is logged and ignored).
- Scale: the only SLO-passing runs at 500K/1M machines predate the
  ADR-0027 demand-model rewrite and do not validate the current
  engine. The current-engine baseline is 5K machines on one shard —
  stale and contested. Targets are 2+ orders of magnitude away on
  every axis (machines/shard, shard count, clusters, soak hours);
  failover has one validated scenario; longest clean soak ≈ 1 hour.

## Downgraded (real, judged hardening rather than ship-stoppers)

1. Shortfall escalation / cross-shard response being decorative —
   consciously deferred post-v1 (plan §10); only bites multi-shard
   finite-capacity fleets, since quota-unconstrained shards
   self-provision from elastic providers.
2. Coordinator admin RPC auth — subsumed by the general security arc.
3. Supply-chain signing/SBOM/dependabot.
4. Quota/provider-registry write tooling (RPCs and CLI absent).
5. Durable decision audit ledger.
6. `buf breaking` CI enforcement (configured, unwired).

## Verified strengths (what is already production-grade)

Paper §5–§16 core engine semantics; structurally enforced static
stability with passing cloud coordinator-kill runs; the machine state
machine and conformance suite; full-replacement roll-up semantics
with supersedes-key coalescing; metrics coverage and the
self-diagnosing phase-attribution probe; an unusually honest docs and
validation-ladder culture.

## Method

47 agents (7 dimension auditors + completeness critic + first verify
wave) + 35 follow-up verifiers. Every claim cited file:line evidence;
doc comments were distrusted by instruction and verified in code.
The verifier prompt instructed refutation, with credit for finding
consciously-deferred-with-rationale mitigations — the 6 downgrades
above are exactly those.
