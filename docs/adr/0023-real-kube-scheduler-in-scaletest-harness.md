# ADR-0023: Real kube-scheduler in the scaletest harness, retire pod-shim's binding role

**Status**: Proposed

**Date**: 2026-05-13

## Context

The scale-test harness ships a custom binary, `bigfleet-scaletest-pod-shim` (at `test/scaletest/cmd/pod-shim/`), which performs three jobs inside each kwok-faked Kubernetes cluster:

1. Watches `UpcomingNode` CRs, creates fake Nodes with `Allocatable` set per ADR-0022's density model.
2. Watches Pods, marks them `PodScheduled=False reason=Unschedulable` when no Node fits (so the unschedulable-pod-controller picks them up and creates a `CapacityRequest`).
3. Binds Pods to fake Nodes via the `/binding` subresource, bin-packing to density.

Jobs 2 and 3 are **kube-scheduler's job in production**. The harness wraps them inside pod-shim only because the original M43 sequence wanted a minimum-viable Pod → bound loop without taking on the integration of a real scheduler against kwok-Nodes. Pod-shim is a harness shortcut, not a design statement.

The shortcut has caught up with us. In the 2026-05-13 devpod-5k run on Uber infrastructure (5 clusters × 100K Pods), the diagnostic on issue #1 (private `bigfleet-uber` repo) showed:

- The BigFleet shard, operator, and rollup pipelines all sit comfortably inside their SLOs (cycle p99 ~127ms-1s, operator rollup p99 ~330ms-1.2s, zero shortfalls).
- The runner's user-facing binding latency SLO (`internalBindingLatencyP99Seconds ≤ 20 s`) is failed at **102.4 s p99**.
- The 102 s is entirely on pod-shim's bind path: 256 binder workers saturated at >30K objects per cluster, individual `/binding` writes slowing because pod-shim competes for apiserver writes with the load-driver on the same in-cluster apiserver.

Cluster-shape iteration (10×50K split — issue #2) confirmed: the bottleneck **is** per-cluster, and we can move the inflection point by sharding clusters smaller. But every variant just verifies the harness's own scheduling stand-in is slower than a real scheduler would be, while telling us nothing useful about BigFleet's behaviour at production-realistic scale.

**The harness's choice of scheduler is now the dominant variable in the published numbers.** That's the wrong shape for a credible scale-test result.

## Decision

Replace pod-shim's scheduling role (jobs 2 and 3 above) with a real `kube-scheduler` process running against each per-cluster kwok apiserver. Keep only job 1 (UpcomingNode → fake-Node creation), as a new small binary `bigfleet-scaletest-node-creator`.

Concretely:

1. **Add `cmd/scaletest-node-creator`**, ~100-line binary. Watches `UpcomingNode` CRs via controller-runtime, creates fake-Nodes with `Allocatable` set per ADR-0022. Zero responsibility for Pods.

2. **Run real `kube-scheduler` inside each per-cluster apiserver container** (`entrypoint-apiserver.sh`). Already running `kube-apiserver`, `kube-controller-manager`, `kwok-controller`; adding the scheduler is one more binary download and one more supervised process. Configure with a `KubeSchedulerConfiguration` that uses `NodeResourcesFit` plugin with `scoringStrategy: { type: MostAllocated }` so it bin-packs to `Allocatable` rather than spreading — preserves ADR-0022's density-100 model without explicit pod-shim placement code.

3. **Retire pod-shim** (`test/scaletest/cmd/pod-shim/`) once the new flow is validated end-to-end on kind + cloud. Until then keep it parallel, gated on a `harness.scheduler` profile field with values `pod-shim` (legacy) and `kube-scheduler` (new). Default flips to `kube-scheduler` once a devpod-5k run on the new flow lands a verdict.

## Consequences

### What we gain

- **Published binding-latency numbers are credible.** What we measure is what a production fleet would measure: real kube-scheduler against the cluster the operator manages. If the soak SLO passes, it's a true claim. If it fails, the failure is meaningful (a real-world per-cluster scheduler ceiling, not a harness limitation).
- **One less custom binary to maintain.** Pod-shim has been a source of bugs across M43, M44, M45 (SetupSignalHandler double-call, `IndexField` cache-startup race, WatchList interaction, binder thundering herd). Removing it removes a class of failure mode from future scale-test work.
- **Density-100 placement comes from a configurable scheduler scoring plugin** rather than hand-rolled bin-pack code. Tunable from outside without modifying harness binaries.
- **Future scale-test harnesses (e.g. multi-tenancy, NodeAffinity-heavy workloads) work without extending pod-shim** — they extend the scheduler config, the established Kubernetes path.

### What we lose

- **Explicit control over Pod-to-Node placement.** Pod-shim's `tryBind` deterministically picks the first fitting fake-Node. Real kube-scheduler scores all Nodes and may make different choices. For verifying ADR-0022's density model holds, we need to confirm the resulting Pod distribution still pile-packs to `Allocatable` capacity per Node. Probably fine with `MostAllocated` scoring; needs explicit validation on kind before cloud.
- **One more process per kwok apiserver container.** The container already runs apiserver + kine/etcd + kube-controller-manager + kwok-controller. Adding kube-scheduler is one more binary, ~50MB RAM idle. Should fit comfortably in current per-pod budgets.
- **The transition window** during which pod-shim and node-creator both exist. Two code paths, slight maintenance overhead. Limited by retiring pod-shim once new flow is proven.

### What stays the same

- BigFleet itself: unchanged. `pkg/shard`, `pkg/coordinator`, `pkg/decision`, `pkg/operator` — none affected. This is purely a harness change.
- The chain shape: load-driver → Pods → (now-real-)scheduler-marks-Unschedulable → unschedulable-pod-controller → CR → operator → shard → Bootstrap → operator publishes UpcomingNode → node-creator creates fake-Node → real scheduler binds Pod. Same five stages between BigFleet and the test inputs, just with the scheduler stage now being the real thing.
- The published `scaletest-results` page: continues to report BigFleet-internal SLOs (shard cycle p99, operator rollup p99, shortfalls) and the now-credible user-facing binding latency.

## Implementation sequence

To stay reversible and reviewable:

1. Add `cmd/scaletest-node-creator` parallel to pod-shim. No behaviour change yet.
2. Add `kube-scheduler` binary download + `KubeSchedulerConfiguration` template + launch logic to `entrypoint-apiserver.sh`. Gated behind an env var so legacy runs are unaffected.
3. Add `harness.scheduler` chart value, plumbed through `entrypoint-workload.sh` to either launch pod-shim (legacy) or skip it (new).
4. Update `dev-500` profile with `harness.scheduler: kube-scheduler`. Validate ramp + soak both pass on kind under the new flow. Density-100 must hold — verify by inspecting `bigfleet_scaletest_pod_shim_*_total` / equivalent node-creator metrics + actual Pod-per-Node distribution.
5. File a `devpod-5k` brief on `bigfleet-uber` to validate the new flow on Uber infrastructure.
6. Flip default `harness.scheduler` to `kube-scheduler` for all profiles.
7. Delete `test/scaletest/cmd/pod-shim/`. Update chart templates and profiles to drop `podShim.*` config.

This ADR moves to **Accepted** once step 5 returns a clean verdict.

## Alternatives considered

- **Tune pod-shim's knobs** (raise `binderConcurrency`, raise `OPERATOR_QPS`, etc.). Cheap, but only moves the bottleneck — doesn't change the fundamental property that we're measuring a custom binder, not a production-realistic one.
- **Shard clusters smaller** (10×50K, 20×25K, etc.). Confirmed in issue #2 that this scales ramp throughput, but soak still fails on the harness's binder. Same critique: the test's outcome depends on a harness implementation detail.
- **Optimise pod-shim into something approximating kube-scheduler.** Significant engineering effort that ends with a worse copy of an existing well-tested binary.
- **Raise the soak SLO to a regime where pod-shim doesn't gate it.** Hides the issue rather than fixing it. Published numbers would silently include pod-shim's overhead. Rejected on grounds of result credibility.

## References

- ADR-0017 (per-CR binding latency vs fingerprint fan-out latency) — establishes the chain stages we measure.
- ADR-0018 (internal vs user-facing binding latency) — the binding-latency dichotomy this ADR resolves more honestly.
- ADR-0022 (`Need.Count` is Pod count) — the density model the new scheduler-config must preserve.
- `bigfleet-uber` issues #1 and #2 (private) — the empirical data driving this decision.
