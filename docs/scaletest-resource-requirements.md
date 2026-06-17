# Scale-test resource requirements

An honest, provider-agnostic estimate of the compute needed to run each BigFleet scale-test rung under realistic conditions, and where the practical ceilings are. Units are abstract — a **"host" is one ~96 vCPU / ~384 GiB cloud VM** — with no provider- or region-specific naming, so the arithmetic holds independent of where you run it.

The lower-rung numbers are measured (see [scale-test results](./scaletest-results.md)); the higher rungs are extrapolated from those plus one dedicated per-host density experiment. The caveats section is explicit about which is which.

## The rungs

Each rung simulates ~10× the Pods of the previous one (a rung's name is its machine count; Pod demand is roughly machine-count × per-machine density).

| rung  | machines | total simulated Pods |
|-------|----------|----------------------|
| 5k    | ~5,000   | 500,000              |
| 50k   | ~50,000  | 5,000,000            |
| 500k  | ~500,000 | 50,000,000           |
| 5m    | ~5,000,000 | 500,000,000        |

## How the harness uses hosts

The fleet is simulated with [kwok](https://kwok.sigs.k8s.io/) (Kubernetes WithOut Kubelet — simulated nodes/Pods, no real containers):

- **1 engine/hub host** runs the BigFleet shard (decision engine) + coordinator + metrics. It hosts **no** simulated Pods and stays light at small scale.
- **N satellite hosts**, each running **M kwok clusters**; each kwok cluster simulates **~25,000 Pods** and carries its own kube-apiserver + kube-scheduler + the BigFleet operator. The satellite host's **CPU is the binding resource** — it pays for apiserver / scheduler / etcd / operator control-plane work, *not* container runtime (kwok has none).
- Each satellite's operators hold a control session back to the hub.

So the host count for a rung is driven by **how many Pods one satellite host can carry cleanly**, plus one hub.

## Per-host clean density (measured)

"Clean" = no sustained host oversubscription (load below core count), all capacity-delivery SLOs green ([SLOs](./slos.md)), no scheduler runaway.

| scheduler config        | clean Pods/host | evidence |
|-------------------------|-----------------|----------|
| default (uncapped)      | **~125K** (proven), ~150–160K ceiling | the 5k baseline ran 5 clusters × 25K = 125K/host at ~50–65 % CPU on a 96-vCPU host |
| **pod-backoff capped**  | **~225K** | density probe: 200K clean (~45–60 % CPU); 250K saturates (~95 % CPU); **uncapped 250K runs away** (load compounds past the core count) |

The capped figure is the important one. At high density the **kube-scheduler's retry/backoff churn dominates host CPU** and, uncapped, compounds into a load runaway the scheduler never recovers from. Capping the pod backoff removes that amplification — the same host then carries ~1.8× the Pods at lower CPU.

The cap is a harness *scheduler* setting, not a BigFleet setting, and it's a **density/cost lever, not an SLO lever**: BigFleet's capacity-delivery gates pass with the scheduler uncapped (the published 5k baseline runs uncapped — see [ADR-0054](./adr/0054-steady-bind-slo-reframe-for-uncapped-scheduler.md)), and end-to-end Pod-bind latency is informational under the release gate precisely because it's scheduler-bound. The cap removes a scheduler CPU artifact to fit more simulated Pods per host; it does not change what BigFleet is graded on.

Density scales (roughly inversely) with host size; with a fixed VM size, host **count** — not host **size** — is the lever.

## Resource estimates per rung

`sat hosts = ceil(Pods / density)`, plus **1 hub**. The **VM count ≈ distinct-host count when VMs are spread across regions** (a cross-region VM lands ~1:1 on its own physical host). Within a single busy region the platform tends to **pack ~1.25–2 VMs per physical host**, so reaching a target distinct-host count there needs proportionally more VMs (or churn) — spreading across regions is the efficient way to add distinct hosts.

| rung | Pods | sat hosts @125K (uncapped) | sat hosts @225K (capped) | total hosts (capped, +hub) | feasibility on a cloud-VM fleet |
|------|------|-----------------------------|---------------------------|-----------------------------|----------------------------------|
| 5k   | 500K | 4    | 3    | **~4**     | Trivial — a handful of VMs in one region. (Published baseline: 1 hub + 4 sats.) |
| 50k  | 5M   | 40   | 23   | **~24**    | Reachable. A single region packs well below 24 distinct hosts, so spread across 2–3 regions (cross-region VMs = 1:1 distinct hosts) and use the scheduler cap. Lands within a modest VM budget (a few dozen). |
| 500k | 50M  | 400  | 223  | **~224**   | Beyond a modest VM budget — hundreds of VMs. Two harness limits also bind: the hub control plane must **shard** (multiple shard replicas, unvalidated at this inventory), and the **single-relay control-session transport does not scale** to hundreds of cross-region sessions (needs an in-cloud or in-cluster relay). Requires a dedicated cluster/cloud allocation. |
| 5m   | 500M | 4000 | 2223 | **~2,200** | Not a cloud-VM-fleet task. Thousands of VMs, a multi-shard engine control plane, and a non-relay transport — effectively a purpose-built test cluster and harness redesign. |

## Binding constraint per rung (what stops you, in order)

- **5k** — nothing; fits comfortably in one region.
- **50k** — distinct-host count vs. VM budget. Solved by **(i) capped density** (~halves hosts, 40 → ~24) and **(ii) cross-region VMs** (1:1 distinct hosts, sidestepping single-region packing). ~24 hosts / ~24–30 VMs.
- **500k** — VM count (hundreds) **+** control-session transport (single relay → in-cloud / in-cluster) **+** hub sharding. Needs dedicated infrastructure.
- **5m** — all of the above at 10×: a dedicated large test cluster, a sharded engine, and an in-cluster transport. A distinct engineering effort, not a fleet-assembly task.

## Caveats & confidence

- **Density is empirical but single-host.** One satellite host was pushed to saturation (uncapped 250K = runaway; capped 200K clean / 250K saturated → ceiling ~225K capped, ~150K uncapped). Multi-host fleets add cross-host variance and shared-host (noisy-neighbor) contention — budget headroom; don't provision at the saturation edge.
- **Hub validated only to ~500K-Pod inventory.** The single hub host sat at ~4 % CPU at the 5k rung. At 50M–500M the hub must shard; that scaling is **unvalidated** and is a separate axis from satellite host count.
- **Lever assumptions:** ~25K Pods per kwok cluster and ~96-vCPU hosts. Both change the arithmetic if changed.
- **Cross-region latency is mostly fine.** The latency-sensitive operator operations are host-local; only the hub control session crosses regions, and it has ample latency headroom. The scaling limit at hundreds+ of sessions is the **single relay**, not the latency.

## Summary

| rung  | clean hosts (capped) | verdict |
|-------|----------------------|---------|
| 5k    | ~4                   | routine — published baseline |
| 50k   | ~24                  | reachable: capped density + cross-region spread, ~24–30 VMs |
| 500k  | ~224                 | needs dedicated cluster allocation + transport / hub-sharding work |
| 5m    | ~2,200               | a purpose-built effort (cluster + sharded engine + in-cluster transport) |

The headline: **scheduler-capping roughly halves the host count, and cross-region VMs remove the single-region packing wall** — together they bring 50k within a modest VM budget. 500k and 5m are gated not by BigFleet's engine (which stays light per-decision) but by raw VM count and two harness components — control-session transport and hub sharding — that would need dedicated work.
