# ADR-0021: Persistent execute pool — decouple action execution from cycle barrier

**Status**: Accepted

**Date**: 2026-05-08

## Context

The shard's cycle (`pkg/shard/shard.go`'s `cycle` function) computes a snapshot, runs Phase 1/2/3 to emit actions, then dispatches those actions across N goroutines (`executeConcurrency`) and **waits for all of them to finish via `wg.Wait()`** before moving on. The cycle's context (`cycleCtx`, deadlined at `s.cfg.CycleInterval`) is the parent of every action's context, so when the deadline fires, in-flight RPCs are cancelled mid-call.

Three things follow from that single barrier:

1. **Cycle wall-clock = max(action_latency).** With 256 workers and operator handler mean ~3 s, p99 ~7 s, the cycle's slowest worker drags the whole cycle to ~7 s. Throughput becomes `executeConcurrency / max(action_latency)` rather than `executeConcurrency / mean(action_latency)`.

2. **Cycle ctx cancellation cascades into machine state.** Until M44.4 we marked machines `Failed` (terminal) on `context.Canceled`; we softened it to a `Configuring → Idle` rollback, but the cancellation still wastes the in-flight RPC and the action gets re-emitted next cycle. At burst, this thrashes.

3. **Phase emissions are coupled to drain rate, not arrival rate.** A cycle can't emit more actions until the previous batch completes. If the operator side is briefly slow (apiserver latency spike), the entire shard pauses.

The scaleway-50k cloud runs in this thread surfaced this hard. With every operator-side and harness-side optimisation in place — coalescer, recv-pump async, dispatch sem tuned, sand smoothing — we still hit a ~25 binds/sec ceiling on the chain because each cycle waits for the slow tail of its 256-action batch. That's below the steady-state churn rate of ~41 CRs/sec. The chain converges (Failed cascade is dead), but it can't sustain steady state.

## Decision

Replace the per-cycle dispatch + wg.Wait pattern with a **shard-scoped persistent worker pool** that:

- Spawns `executeConcurrency` goroutines once, at shard start.
- Workers loop forever, draining a single shard-scoped buffered channel `actionQueue`.
- Each worker derives **its own per-action context** (`shardCtx + ExecuteTimeout`) — independent of any cycle deadline.
- The cycle now just *enqueues* actions and returns immediately. No `wg.Wait`.
- The queue is bounded; if it's full, the cycle drops new emissions and increments a `dropped` counter. Phase emissions are idempotent against an unchanged snapshot, so the next cycle re-derives.

Concretely:

```go
type Shard struct {
    // ... existing fields ...
    actionQueue chan decision.Action  // size: executeConcurrency * 2
}

func (s *Shard) Run(ctx context.Context) error {
    s.actionQueue = make(chan decision.Action, s.cfg.ExecuteConcurrency*2)
    for i := 0; i < s.cfg.ExecuteConcurrency; i++ {
        go s.executeWorker(ctx)
    }
    // ... existing cycle loop ...
}

func (s *Shard) executeWorker(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        case a := <-s.actionQueue:
            execCtx, cancel := context.WithTimeout(ctx, s.cfg.ExecuteTimeout)
            _ = s.execute(execCtx, a)  // err already logged in classifyExecuteError
            cancel()
        }
    }
}

// in cycle, replace the dispatch+wait with:
for _, a := range all {
    select {
    case s.actionQueue <- a:
    default:
        metrics.ShardActionsDropped.Inc()
    }
}
// no wg.Wait
```

`ExecuteTimeout` is a new config field (default 30 s; matches the existing `BootstrapTimeout`). Each action is independent; cycle deadlines do not gate them.

## Consequences

### What this changes (correctness)

- **Stale-snapshot risk increases.** An action emitted at cycle T might run during cycle T+5 (handler-mean × queue-depth seconds later). By then the world may have moved. Mitigation already in place: every executor (`executeBootstrap`, `executeProvision`, `executeDrain`) starts with a state-machine validation against the *current* inventory (via `s.inv.Get`), and `applyTransition` rejects invalid transitions. A stale action gets a soft-fail and the worker logs and moves on. Phase 1 re-emits next cycle.

- **Duplicate emissions across cycles increase.** If cycle T emits Bootstrap for machine M, and cycle T+1 fires before M has been transitioned to Configuring, T+1's Phase 1 sees M as Idle and emits Bootstrap again. Both queue. The first worker to win the state-machine race transitions M; the second sees M is no longer Idle and soft-fails. Same mitigation as above. We could also add a per-machine in-flight set to prevent the duplicate emission, but it adds complexity without changing the outcome.

- **Cycle ctx cancellation no longer affects in-flight executes.** The Configuring → Idle rollback transition added in M44.4 stays, but its trigger (cycle ctx fire) becomes rare — only an operator RPC that takes longer than `ExecuteTimeout` (30 s default) cancels.

### What this changes (observability)

- **`ShardCyclePhaseDuration{phase="execute"}` becomes near-instant** — it now measures only the time to enqueue actions. The wall-clock of action execution moves to a new histogram `ShardActionDuration{kind=...}`.

- **`ShardActionsDeferred` semantics change.** Previously: actions truncated by `MaxActionsPerCycle` cap. Now: actions dropped because the queue is full. Same name, same intent (something the cycle wanted to emit but couldn't), different mechanism. Documented in the metric's help text.

- **`ShardActionQueueDepth` (new gauge)**: current depth of the action queue. Useful for spotting back-pressure: queue depth steadily climbing means workers can't keep up with cycle emissions.

- **`ShardExecuteInflight` (existing gauge) keeps working** — it's now bounded by `executeConcurrency` regardless of cycle phase.

### What this doesn't change

- The shard's overall API and the CapacityProvider contract are untouched.
- `executeConcurrency` and `MaxActionsPerCycle` profile knobs keep their meanings.
- `BootstrapTimeout` keeps its meaning (per-RPC blob fetch deadline).
- All existing executors (`executeBootstrap`, `executeProvision`, `executeDrain`) are unmodified — they already handle the "world has moved" case. The change is upstream, in how they're invoked.
- The static-stability invariant: clusters keep running with BigFleet entirely down. (Workers exit cleanly on `shardCtx.Done()`; in-flight actions abort; nothing is in an undefined state.)

### Throughput implication

Pre-change:
```
throughput = executeConcurrency / max(action_latency_p99)
           = 256 / 7 s = 36 actions/sec
```

Post-change:
```
throughput = executeConcurrency / mean(action_latency)
           = 256 / 3 s = 85 actions/sec
```

That's roughly a 2.4× lift assuming today's operator handler latency. With the steady-state churn rate of 41 CRs/sec on `scaleway-50k`, that comfortably clears.

## Alternatives considered

1. **Just bump cycle ctx to 60 s.** Reduces ctx_canceled but doesn't fix the wg.Wait barrier — cycle still waits for slowest worker, just takes longer to do it. Throughput stays max-bound. Plus the cycle is now sluggish to react to new demand.

2. **Bump executeConcurrency to 1024.** With handler mean 3 s, queueing on the apiserver-side QPS limit grows. Each handler waits longer in the rate-limit queue. Saw this empirically when we bumped to 256 — handler p99 went 4 s → 32 s. More concurrency makes individual handlers slower under contention.

3. **Hand-rolled priority queue per cluster.** Doesn't address the cycle barrier. Adds complexity.

4. **Remove `wg.Wait` but keep per-cycle workers.** Each cycle still creates and destroys 256 goroutines. Avoidable scheduler / GC overhead. Persistent pool is strictly better.

## Validation

- Local `go test ./pkg/shard/...` and `make scale` pass.
- `dev-5k` kind run reaches steady-state SLO (single-cluster local check that the queue/idempotency path works at small scale).
- `scaleway-50k` cloud re-run: expect chain throughput ≥ 80 binds/sec sustained, steady-state internal binding latency p99 ≤ 15 s (ADR-0020 budget).
