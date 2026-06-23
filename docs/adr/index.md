# Architecture Decision Records

<!-- This file is synced from docs/adr/index.md at build time. Edit the canonical source. -->

## ADR index

| # | Status | Title |
|---|--------|-------|
| [1](/adr/0001-record-architecture-decisions/) | Accepted | Record architecture decisions |
| [2](/adr/0002-coordinator-topology-single-region/) | Accepted | Coordinator topology: single region |
| [3](/adr/0003-shard-snapshot-eventual-consistency-on-the-cycle-hot-path/) | Superseded | Shard snapshot: eventual consistency on the cycle hot path |
| [4](/adr/0004-incremental-reconcile-via-since-revision/) | Accepted | Incremental reconcile via since-revision |
| [5](/adr/0005-provider-boundary-is-the-validation-point/) | Accepted | Provider boundary is the validation point |
| [6](/adr/0006-shard-self-registers-via-heartbeat/) | Accepted | Shard self-registers via heartbeat |
| [7](/adr/0007-cluster-to-shard-binding-is-operator-chosen-at-deploy-time/) | Accepted | Cluster-to-shard binding is operator-chosen at deploy time |
| [8](/adr/0008-coordinator-admin-rpcs-are-leader-only-and-unauthenticated-in-v1/) | Amended by ADR-0048 | Coordinator admin RPCs are leader-only and unauthenticated in v1 |
| [9](/adr/0009-reclaim-uses-policy-v1-eviction-and-async-drain/) | Accepted | Reclaim uses policy/v1 eviction and async drain |
| [10](/adr/0010-minimum-kubernetes-version-1-31/) | Accepted | Minimum Kubernetes version 1.31 |
| [11](/adr/0011-bootstrap-template-is-helm-values-text-template/) | Accepted | Bootstrap template is Helm values text template |
| [12](/adr/0012-helm-charts-published-to-ghcr-as-oci-artefacts/) | Accepted | Helm charts published to GHCR as OCI artefacts |
| [13](/adr/0013-demand-to-inventory-regimes-and-slos/) | Accepted | Demand-to-inventory regimes and SLOs |
| [14](/adr/0014-slo-posture-binding-latency-not-cycle-wall-clock/) | Accepted | SLO posture: binding latency, not cycle wall-clock |
| [15](/adr/0015-realistic-archetype-improvements/) | Accepted | Realistic archetype improvements |
| [16](/adr/0016-nodestateupdate-carries-node-identity/) | Accepted | NodeStateUpdate carries node identity |
| [17](/adr/0017-per-cr-binding-latency-vs-fingerprint-fanout/) | Accepted | Per-CR binding latency vs fingerprint fanout |
| [18](/adr/0018-internal-vs-user-facing-binding-latency/) | Accepted | Internal vs user-facing binding latency |
| [19](/adr/0019-phase1-cloud-vs-bench-discrepancy/) | Accepted | Phase 1 cloud vs bench discrepancy |
| [20](/adr/0020-internal-binding-latency-slo-respects-rollup-interval/) | Accepted | Internal binding latency SLO respects rollup interval |
| [21](/adr/0021-persistent-execute-pool/) | Accepted | Persistent execute pool |
| [22](/adr/0022-need-count-semantics-pod-vs-machine/) | Accepted | `Need.Count` semantics — Pod count vs machine count, and where packing lives |
| [23](/adr/0023-real-kube-scheduler-in-scaletest-harness/) | Accepted | Real kube-scheduler in the scaletest harness, retire pod-shim's binding role |
| [24](/adr/0024-co-location-via-podaffinity/) | Accepted | Co-location via podAffinity — the `CoLocation` CR field, roll-up aggregates |
| [25](/adr/0025-harness-anchors-samerack-groups/) | Accepted | The load-driver anchors sameRack groups — a gang-scheduler stand-in |
| [26](/adr/0026-scaletest-models-speculative-tier/) | Accepted | The scaletest harness must model the Speculative tier |
| [27](/adr/0027-rollup-demand-is-a-constrained-resource-request/) | Accepted | Roll-up demand is a constrained aggregate resource request, not `(per-pod-shape, count)` |
| [28](/adr/0028-cycle-p99-is-regime-parametric/) | Accepted | Cycle-p99 SLO is regime-parametric; the realistic catalog scales with Need cardinality |
| [29](/adr/0029-phase1-omega-style-occ/) | Accepted | Phase 1 Omega-style OCC — shared-state, commit-broker priority, dual-mode commits |
| [30](/adr/0030-incremental-phase1/) | Proposed | Incremental Phase 1 — delta-only processing as a layered optimization |
| [31](/adr/0031-parsync-partitioned-sync/) | Proposed | ParSync-style partitioned synchronization — conditional follow-on for raised per-shard ceilings |
| [32](/adr/0032-realistic-catalog-production-calibration/) | Accepted | Realistic catalog production-calibrated workload distribution |
| [33](/adr/0033-phase1-supply-credit-respects-bind-readiness/) | Rejected | Phase 1 supply-credit must respect bind readiness, not just provider state — superseded by ADR-0035 |
| [34](/adr/0034-scaletest-byo-substrate/) | Accepted | Scaletest is bring-your-own-substrate |
| [35](/adr/0035-scaletest-slos-at-steady-state/) | Accepted | Scaletest SLOs are measured at steady state under churn, not at ramp |
| [36](/adr/0036-phase3-gated-by-first-rollup/) | Accepted | Phase 3 reclaim must not fire before a cluster's first rollup has arrived |
| [37](/adr/0037-scaletest-node-affinity-dimensions-are-realistic/) | Accepted | Scaletest catalog node-affinity dimensions must be realistic — drop synthetic team/app label axes |
| [38](/adr/0038-scaletest-workloads-are-controller-managed/) | Accepted | Scaletest workloads are controller-managed objects (Deployment / StatefulSet), not bare Pods |
| [39](/adr/0039-capacityrequest-per-pod-not-per-unschedulable-pod/) | Accepted | One CapacityRequest per Pod — not per *unschedulable* Pod; the demand signal must be total, not unmet |
| [40](/adr/0040-same-domain-attribution-unified/) | Accepted | `Same`-domain attribution is unified — every supply-crediting site is domain-aware |
| [41](/adr/0041-sub-machine-same-needs-fold-into-atomic-aggregates/) | Accepted | Sub-machine `Same`-Needs fold into atomic aggregates — `Same` is for cross-machine topology |
| [42](/adr/0042-unsatisfiable-domain-choice-is-sticky-at-equal-coverage/) | Accepted | Unsatisfiable-regime `Same`-domain choice is sticky at equal coverage — switch only for strictly greater |
| [42a](/adr/0042-addendum-aged-acquisition-parking/) | Accepted | ADR-0042 Addendum: aged acquisition parking — group identity on the wire, park after 8 unsatisfiable cycles, re-probe every 32 |
| [43](/adr/0043-demand-realism-check-before-mechanism/) | Accepted | Harness-observed triggers get a demand-realism check before mechanism ships |
| [44](/adr/0044-machine-count-aware-seed-sizing/) | Accepted | Seed machine pools are sized by machine demand (pod share ÷ packing density, gang-aware per-zone floors), not workload weight |
| [45](/adr/0045-consumed-capacity-in-the-attribution-model/) | Accepted | Capacity counts for a cluster iff bound — Phase 3 reclaims on demand shrinkage only; BigFleet never models packing (author decision; supersedes its own first draft) |
| [46](/adr/0046-actuation-safety-rails/) | Accepted | Actuation safety rails — per-cluster reclaim blast-radius cap, empty-roll-up quarantine, global kill switch |
| [47](/adr/0047-coordinator-raft-join-and-offline-restore/) | Accepted | Coordinator quorum formation by ordinal join; offline snapshot restore as single-voter recovery |
| [48](/adr/0048-mtls-and-uri-san-identity/) | Accepted | Opt-in file-based mTLS with bigfleet:// URI SAN identity binding — supersedes ADR-0008's transport posture |
| [49](/adr/0049-idle-speculative-release-hold-window/) | Accepted | Idle→Speculative release (paper §8's other half) — per-CapacityType idle holds inside Phase 3; the hold window is the rail, not a cap |
| [50](/adr/0050-realism-catalog-is-machine-calibrated/) | Accepted | Realism catalog (realistic.yaml) calibrated to a realistic MACHINE fleet via per-archetype node-packing density; GPU inference densified (8/node), training whole-machine (1); amends M66.2 + ADR-0044 (author decision) |
| [51](/adr/0051-gang-granular-domain-attribution/) | Accepted | Same-domain choice follows THIS gang's bindings (gang-granular attribution) — record Need.Group on the binding, break capped-coverage ties on gang-own coverage; refines ADR-0045, fixes M77g (author decision) |
| [52](/adr/0052-count-in-flight-provision-commitment/) | Accepted | The shard counts its own in-flight provision commitment against the deficit — credit attributed Creating machines in the coverage walk; amends ADR-0045's "no in-flight discounting" one state earlier, fixes the #66/#74 pre-Configuring runway over-acquire (author decision) |
| [53](/adr/0053-two-axis-machine-state-deferred/) | Deferred | Two-axis machine-state model (provisioned × bound + op annotation) — scouted as an alternative to ADR-0052 and judged worse for the over-acquire (doesn't fix it; 149-ref blast; raises correctness surface); deferred as a standalone future ergonomics initiative, wire-frozen, post-ladder (author decision) |
| [54](/adr/0054-steady-bind-slo-reframe-for-uncapped-scheduler/) | Accepted | Steady pod-bind SLO reframe under an uncapped real scheduler — release gate moves off the end-to-end pod-bind p99 (uncapped-scheduler / reprovision-bound, not BigFleet's deliverable) onto BigFleet's capacity-delivery hops (configure-phase p99, Bootstrap success ratio, node-state-update p99, shortfalls==0) plus a loose end-to-end p50 liveness floor; the end-to-end p99 becomes informational (author decision) |
| [55](/adr/0055-cross-shard-rebalancing/) | Proposed | Coordinator-driven cross-shard rebalancing (realises bigfleet.md §9: transfer idle → reassign quota → cross-shard preempt) — a leader-only tiered rebalancer + the three stub handlers made real, reusing the M20/M69 drain path; anti-oscillation via cooldown + demand-pull invariant; machine-ids donor-resolved, ownership via shard-local persisted owned-set (author decided to BUILD not remove, 2026-06-19; Proposed pending staged-build greenlight) |
| [56](/adr/0056-node-join-readiness-gates-coverage-credit/) | Accepted | Coverage credit gated on observed node readiness — **Option A (provider-contract obligation)**: Configure must not report Configured until the node is observed Ready, enforced by a new conformance cluster-join scenario (no shard change); closes the S1 silent false-Configured → phantom-capacity hole that bootstrapSuccessRatio (reported failures) and ADR-0033 (ramp throughput) do not cover (author decision) |
| [57](/adr/0057-node-state-emitted-on-reconcile-and-resynced-on-reconnect/) | Accepted | P0: shard emits NodeStateUpdate on reconcile-observed transitions + resyncs node state on operator (re)connect — notifyNodeState fired only from the worker/applyTransition path, so async (providerkit) providers, which reach terminal Configured via reconcile, were invisible to the operator (workload never schedules); the in-process fake masked it and the assumed reconnect resync was never built. Shard→operator only, static stability preserved (author decision) |
| [58](/adr/0058-fence-high-water-mark-per-shard-machine/) | Accepted | Shard→provider fencing high-water mark is per (shard_id, machine_id), not per shard_id — a single live shard's concurrent execute pool draws monotonic sequence numbers but races the sends, so a per-shard mark fenced the shard against its own out-of-order arrivals on different machines (false zombie → ~30/120 machines bricked at execute-concurrency 32). Per-machine keying stays monotonic (shard serializes per machine) while letting concurrent cross-machine ops proceed; a true zombie is still caught on epoch. Dir 3 (serialize stamp+send) refuted (server-side goroutine race). Contract + conformance (B302 broadened) + snapshot-format change; surfaced by bigfleet-demo (author decision) |
