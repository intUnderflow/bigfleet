# ADR-0058: The shard→provider fencing high-water mark is per (shard_id, machine_id), not per shard_id

## Status

**Accepted**, 2026-06-23 (author decision, Lucy Sweet).

## Context

Every mutating provider RPC carries the shard's fencing token `(shard_id, shard_epoch, sequence_number)` (paper §11). The provider keeps a high-water mark and rejects any token not strictly newer with `FAILED_PRECONDITION`, stopping a zombie shard from actuating a stale view of the fleet. The check semantics — strict-lexicographic-newer — live in the proto contract and the `providerkit` implementation; **paper §11 only fixes the token *shape*, not the check granularity.**

The `bigfleet-demo` owner reported that with a real async `providerkit` provider on `--provider-addr` and `--execute-concurrency=32`, ~30 of 120 machines landed in `FAILED` during a provisioning burst, each fenced as a "zombie" — yet there was **one live shard, one `shard_id`, one epoch.** At `--execute-concurrency=1` it never happened.

An adversarial cross-repo investigation (7 receipt-verifiers + a 3-lens verdict panel) confirmed the mechanism and the crux:

- The shard's execute pool runs `ExecuteConcurrency` workers that share one `grpcclient.Client`/`Identity.Seq`. Each mutating RPC stamps `SequenceNumber: c.id.Seq.Next()` and then sends, with **no lock across stamp→send** (`pkg/provider/grpcclient/grpcclient.go`, `pkg/fencing/fencing.go`). Workers draw monotonic seqs but **race the sends**, so tokens arrive out of order.
- The kit kept **one high-water mark per `shard_id`** and rejected anything not strictly newer (`providerkit/server.go` `checkFenceLocked`, `fence.go` `newer`). So seq 27 arriving before seq 16 advanced the mark to 27, then bricked seq 16.
- The shard maps `ErrFenced → StateFailed`, terminal, no retry, all four mutating paths (`pkg/shard/execute.go`).
- **The within-epoch sequence is not load-bearing beyond zombie detection.** A true zombie is a superseded process; `fencing.LoadEpoch` is a per-`shard_id` *persisted, increment-on-start* counter, so any successor holds a strictly **higher epoch** and is caught on epoch alone. Idempotency is keyed on `(machine_id, kind) → operation_id`, independent of the seq. A new epoch resets the seq space. The global seq's only function was to order one process's stream — and the shard's per-machine pending-action gate already guarantees **at most one in-flight mutation per machine**, so the global seq was only ordering operations across *different* machines, which have no ordering relationship at all.

**Direction 3 (serialize stamp+send in `grpcclient`) was refuted:** even with ordered sends, a gRPC server dispatches each unary RPC on its own goroutine and races to acquire the fence lock, so check-order ≠ arrival-order. It only "works" by serializing the entire mutating dispatch (≈ `conc=1`).

## Decision

Key the provider's fencing high-water mark per **(shard_id, machine_id)**, not per `shard_id`. The machine id rides in every mutating RPC (`Create`/`Configure`/`Drain`/`Delete` all carry `machine_id`), so the check needs no proto field change. `FenceMark.newer` is unchanged (epoch then sequence, lexicographic); only the map key changes.

Because the shard serializes transitions per machine (one in-flight mutation per machine), a per-(shard, machine) mark stays monotonic for real traffic and **never false-positives** for a single live shard, while concurrent ops on different machines stop fencing each other. A **true zombie** is still caught: it carries a strictly lower epoch, rejected per machine.

This is **Direction 2** of the hand-off's candidates. Rejected:

- **Direction 1 (epoch-only within shard):** correct for zombie detection but loses all within-epoch resolution and the same-epoch duplicate-identity tripwire (which becomes silent), and is no simpler in blast radius than Dir 2.
- **Direction 3 (serialize stamp+send):** refuted (server-side goroutine race; collapses to `conc=1`).
- **Direction 4 (`conc=1` for out-of-tree, made loud):** leaves a real concurrency defect in the contract and caps **every** `providerkit` deployment at serial provisioning — unacceptable for a system built around out-of-tree providers.

## Consequences

- **Concurrent provisioning works for out-of-tree providers.** `--execute-concurrency > 1` no longer bricks machines on the async provider path; the documented ramp-burst remedy is now safe.
- **`ErrFenced → terminal` stays correct.** Under per-machine keying the fence fires only on a true zombie (lower epoch) or a genuine same-(shard, machine) conflict; a benign cross-machine reorder is no longer a rejection, so no core `execute.go` change is needed.
- **Contract + conformance change.** `provider.proto`'s obligation moves to per (shard_id, machine_id). The conformance suites broaden the *isolation* behavior (kit `B302`; core `test/conformance` adds a per-machine isolation test) and tighten the *ordering* behavior (`B303`) to "single shard **and machine**". Existing fence tests were all single-machine, so none flipped — the cross-machine case was simply **never covered** (the same blind spot as ADR-0057: the in-process fake doesn't fence). All `providerkit`-based providers inherit the fix centrally and re-certify against the broadened suite; the behavior-catalog count is unchanged (B302 extended, not added).
- **Snapshot format change.** The persisted `Snapshot.Fences` moves from `map[string]FenceMark` (keyed by shard_id) to `[]FenceRecord{ShardID, MachineID, Epoch, Sequence}`, mirroring `OpRecord`. A provider upgraded in place drops its pre-upgrade marks; this is benign (the first post-upgrade op per (shard, machine) re-establishes the mark, and the shard's epoch is higher than any pre-upgrade process anyway).
- **No change to the coordinator→shard fence.** That path is single-threaded (`coordclient` runs one goroutine, sequential) and validates only the term, so it has no analogous race; the fix is deliberately asymmetric.

This is the provider-side fencing counterpart to the async-provider work in [ADR-0056] (when a node is `Configured`) and [ADR-0057] (does the operator hear it). Surfaced, like both, by the `bigfleet-demo` real async provider path.

[ADR-0056]: ./0056-node-join-readiness-gates-coverage-credit.md
[ADR-0057]: ./0057-node-state-emitted-on-reconcile-and-resynced-on-reconnect.md
