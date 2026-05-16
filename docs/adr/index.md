# Architecture Decision Records

<!-- This file is synced from docs/adr/index.md at build time. Edit the canonical source. -->

## ADR index

| # | Title |
|---|-------|
| [1](/adr/0001-record-architecture-decisions/) | Record architecture decisions |
| [2](/adr/0002-coordinator-topology-single-region/) | Coordinator topology: single region |
| [3](/adr/0003-shard-snapshot-eventual-consistency-on-the-cycle-hot-path/) | Shard snapshot: eventual consistency on the cycle hot path |
| [4](/adr/0004-incremental-reconcile-via-since-revision/) | Incremental reconcile via since-revision |
| [5](/adr/0005-provider-boundary-is-the-validation-point/) | Provider boundary is the validation point |
| [6](/adr/0006-shard-self-registers-via-heartbeat/) | Shard self-registers via heartbeat |
| [7](/adr/0007-cluster-to-shard-binding-is-operator-chosen-at-deploy-time/) | Cluster-to-shard binding is operator-chosen at deploy time |
| [8](/adr/0008-coordinator-admin-rpcs-are-leader-only-and-unauthenticated-in-v1/) | Coordinator admin RPCs are leader-only and unauthenticated in v1 |
| [9](/adr/0009-reclaim-uses-policy-v1-eviction-and-async-drain/) | Reclaim uses policy/v1 eviction and async drain |
| [10](/adr/0010-minimum-kubernetes-version-1-31/) | Minimum Kubernetes version 1.31 |
| [11](/adr/0011-bootstrap-template-is-helm-values-text-template/) | Bootstrap template is Helm values text template |
| [12](/adr/0012-helm-charts-published-to-ghcr-as-oci-artefacts/) | Helm charts published to GHCR as OCI artefacts |
| [13](/adr/0013-demand-to-inventory-regimes-and-slos/) | Demand-to-inventory regimes and SLOs |
| [14](/adr/0014-slo-posture-binding-latency-not-cycle-wall-clock/) | SLO posture: binding latency, not cycle wall-clock |
| [15](/adr/0015-realistic-archetype-improvements/) | Realistic archetype improvements |
| [16](/adr/0016-nodestateupdate-carries-node-identity/) | NodeStateUpdate carries node identity |
| [17](/adr/0017-per-cr-binding-latency-vs-fingerprint-fanout/) | Per-CR binding latency vs fingerprint fanout |
| [18](/adr/0018-internal-vs-user-facing-binding-latency/) | Internal vs user-facing binding latency |
| [19](/adr/0019-phase1-cloud-vs-bench-discrepancy/) | Phase 1 cloud vs bench discrepancy |
| [20](/adr/0020-internal-binding-latency-slo-respects-rollup-interval/) | Internal binding latency SLO respects rollup interval |
| [21](/adr/0021-persistent-execute-pool/) | Persistent execute pool |
| [22](/adr/0022-need-count-semantics-pod-vs-machine/) | `Need.Count` semantics — Pod count vs machine count, and where packing lives |
| [23](/adr/0023-real-kube-scheduler-in-scaletest-harness/) | Real kube-scheduler in the scaletest harness, retire pod-shim's binding role |
| [24](/adr/0024-co-location-via-podaffinity/) | Co-location via podAffinity — the `CoLocation` CR field, roll-up aggregates |
| [25](/adr/0025-harness-anchors-samerack-groups/) | The load-driver anchors sameRack groups — a gang-scheduler stand-in |
| [26](/adr/0026-scaletest-models-speculative-tier/) | The scaletest harness must model the Speculative tier |
| [27](/adr/0027-rollup-demand-is-a-constrained-resource-request/) | Roll-up demand is a constrained aggregate resource request, not `(per-pod-shape, count)` |
| [28](/adr/0028-cycle-p99-is-regime-parametric/) | Cycle-p99 SLO is regime-parametric; the realistic catalog scales with Need cardinality |
