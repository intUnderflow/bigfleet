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
| [45](/adr/0045-consumed-capacity-in-the-attribution-model/) | Proposed | Consumed capacity enters the attribution model — operator-reported per-machine consumption + bound/open demand split (author decision pending) |
| [46](/adr/0046-actuation-safety-rails/) | Accepted | Actuation safety rails — per-cluster reclaim blast-radius cap, empty-roll-up quarantine, global kill switch |
| [47](/adr/0047-coordinator-raft-join-and-offline-restore/) | Accepted | Coordinator quorum formation by ordinal join; offline snapshot restore as single-voter recovery |
| [48](/adr/0048-mtls-and-uri-san-identity/) | Accepted | Opt-in file-based mTLS with bigfleet:// URI SAN identity binding — supersedes ADR-0008's transport posture |
