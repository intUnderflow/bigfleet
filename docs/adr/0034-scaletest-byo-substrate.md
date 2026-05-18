# ADR-0034: Scaletest is bring-your-own-substrate

## Status

Accepted, 2026-05-19.

## Context

`test/scaletest/profiles/*.yaml` today bundles two unrelated things
into one file: **the test definition** (scale, catalog, density,
ramp, churn, soak) and **the substrate config** (per-host
capacity, per-cluster apiserver Pod ceiling, storage backend,
kwok-pod resource requests, cost). The bundle is encoded in the
filename itself: `scaleway-50k` and `uber-50k` describe the same
scale of test but with different cluster geometries, kwok-pod
resources, and storage backends because the substrates have
different per-host capacity.

This entanglement has three observable costs:

1. **Filename leakage.** `uber-*` and `scaleway-*` name a runtime
   choice as if it were a property of the system-under-test. A
   public BigFleet user picks up the repo and sees five "scaleway"
   profiles plus five "uber" profiles and has to read both to
   figure out which (if any) matches their cloud. The substrate
   prefix doesn't belong in the filename.
2. **Profile × substrate combinatorial growth.** Today's 5 scales ×
   2 substrates = 10 files. Adding a third substrate (kind-laptop
   above dev-50/dev-500, GKE Autopilot, EKS spot) would push to
   15, then 20. Each new file is mostly a copy of the same scale
   description with substrate-specific tuning swapped in.
3. **No clean reuse for outside users.** A public-BigFleet user
   running their first scaletest writes a profile that conflates
   "what test" with "where to run" — and then has to re-derive
   that for every scale they want to validate against.

Inspecting `scaleway-50k.yaml` and `uber-50k.yaml` side-by-side
makes the structural point: the `scale`, catalog, density, ramp,
soak, churn, seed-fraction, priority distribution, and runner-
action fields are identical or trivially-equivalent across the
two files. The only fields that differ are the substrate-specific
ones — cluster count, per-cluster Pods, kwok-pod resources, storage
backend, cost estimate. The framework is *already* substrate-
agnostic in its test-definition surface; the file layout just
doesn't reflect it.

## Goals

1. **Scaletest profiles describe the test, not the runtime.** A
   profile YAML carries scale, catalog, density, ramp/soak/churn,
   priority distribution, failure injection — everything the
   system-under-test sees. It carries no substrate-specific tuning.
2. **Substrates are user-supplied and orthogonal.** A substrate
   YAML carries per-host capacity, per-cluster Pod ceiling, storage
   backend, kwok-pod resource budgets, and cost. BigFleet ships
   three example substrates as a starting point; users describe
   their own infra in the same format.
3. **The runner derives geometry from the cross product.**
   `clusterCount = ceil(profile.machines × profile.density /
   substrate.cluster.podsPerCluster)`. Cost = `hosts ×
   perHostUsdPerHour × duration`. Ramp budget validation against
   the substrate's declared bind throughput.
4. **No public-facing substrate names.** The example substrates
   are named by *shape* (e.g. `example-fat-host`,
   `example-mid-host`, `example-kind-laptop`), not by provider.
5. **Behaviour-neutral migration.** The split is a refactor, not
   a semantic change. Pre-migration scale-test results must
   reproduce post-migration when the same scale and substrate
   parameters are supplied as `--profile=<scale>.yaml
   --substrate=<substrate>.yaml`.

## Non-goals

- **Auto-selecting a substrate** based on cluster context. The user
  always passes `--substrate=` explicitly. Implicit defaults are
  the wrong direction for "where am I about to spend money".
- **A substrate registry / marketplace.** Substrates are local
  files. There's no remote catalogue, no online lookup, no
  "official" substrate beyond the three examples we ship.
- **Multi-substrate composition.** A run uses one substrate. A
  test that wants to validate multi-region / multi-substrate
  behaviour belongs in a dedicated failover-* profile that
  embeds the cross-substrate scenario.
- **Substrate-side parameter sweeps.** The runner takes one
  profile + one substrate. Sweeping is a shell-loop concern, not
  a runner feature.
- **Changes to ADR-0029 / ADR-0033 / ADR-0032 semantics.** This
  is a file-layout refactor; algorithmic content is unchanged.

## Decision

**Split each existing `*-Nk.yaml` profile into a substrate-agnostic
test definition + a separately-named substrate file.** The runner
takes both and merges them into the Helm values it installs.

### Substrate schema (canonical)

```yaml
# test/scaletest/substrates/<name>.yaml
apiVersion: bigfleet.io/scaletest/v1
kind: Substrate
metadata:
  name: example-fat-host
  description: "80vCPU / 160GiB hosts with etcd-backed kwok apiservers."

host:
  # Per-host resource budget. The runner packs cluster.clustersPerHost
  # kwok pods per host plus the BigFleet system-under-test pods
  # (shard, coordinator, prometheus) onto the first host.
  vCPU: 80
  memoryGiB: 160

cluster:
  # The per-kwok-apiserver operating point. podsPerCluster is the
  # substrate's "comfortable Pod ceiling" — past this point bind
  # throughput tails off (kine sqlite WAL pressure, etcd watch
  # latency, kube-scheduler list-watch cost, etc.).
  podsPerCluster: 25000
  clustersPerHost: 10
  storage: etcd               # or "kine"
  # Empirical per-cluster bind throughput. The runner uses this to
  # validate that profile.rampSeconds × bindThroughputPodsPerSec
  # covers profile's total Pod ramp; emits a warning if not.
  bindThroughputPodsPerSec: 30

kwokPod:
  # The kwok pod (apiserver + kwok-controller + operator + load-driver)
  # resource budget. Tuned to the substrate's per-host capacity and
  # podsPerCluster.
  requests: { cpu: 2, memory: 4Gi }
  limits:   { cpu: 8, memory: 32Gi }
  # Optional. Default 1Gi. Past 25K Pods/cluster, kine's sqlite WAL
  # needs more headroom; etcd-backed substrates can leave this at
  # default.
  sharedVolumeSizeLimit: "2Gi"

apiserver:
  # Optional. Substrate-specific apiserver tuning (max-mutating-req,
  # max-readonly-req, watch-cache-size). Helm chart applies as
  # --extra-flags. Default: chart defaults (sane for kine on 25K Pods).
  extraFlags: []

# Cost model. Free / on-prem substrates set perHostUsdPerHour: 0.
costEstimate:
  perHostUsdPerHour: 0
  notes: ""

# Optional: provisioning hints. Free-form Markdown the runner prints
# during --dry-run; not validated.
provisioning: |
  See substrate docs for how to spin up matching hosts.
```

### Profile schema (what stays)

```yaml
# test/scaletest/profiles/<name>.yaml
apiVersion: bigfleet.io/scaletest/v1
kind: Profile
metadata:
  name: 50k
  description: "50K machines, 5M aggregated Pods at density=100."

scale:
  machines: 50000
  density: 100              # Pods per machine
  # Total Pods = machines × density = 5_000_000

catalog:
  # ADR-0032: realistic six-archetype workload distribution.
  archetypes: realistic     # or "uniform" for synthetic-bench shape

seed:
  # Pre-seeded inventory at runner install time.
  configuredFraction: 0.0   # 0 = cold start; 1 = fully pre-seeded
  speculativeMultiplier: 3  # ADR-0026: elastic tier
  idleHeadroomFraction: 0.2 # 20% Idle headroom for Phase 1

loadProfile:
  rampSeconds: 1800
  soakSeconds: 1800
  churnPerMinute: 0.02

# Optional. Failure-injection hooks for failover-* profiles.
runnerActions: []
```

### Runner merge semantics

```
clusterCount = ceil(profile.scale.machines × profile.scale.density /
                    substrate.cluster.podsPerCluster)

hostsNeeded = ceil(clusterCount / substrate.cluster.clustersPerHost) +
              1   # +1 for BigFleet system-under-test pods

estimatedCost = hostsNeeded × substrate.costEstimate.perHostUsdPerHour ×
                (rampSeconds + soakSeconds + 600) / 3600   # +10min teardown
```

Validation (runs at start, before any helm install):

1. **Ramp feasibility.** `profile.scale.machines × profile.scale.density
   / profile.loadProfile.rampSeconds ≤ clusterCount ×
   substrate.cluster.bindThroughputPodsPerSec`. If not, warn —
   the ramp will tail off past the budget. (Not fatal; some tests
   *want* to exercise that regime.)
2. **Resource fit.** `hostsNeeded × substrate.host.vCPU` is sane
   (i.e. ≥ what the chart will request). The chart's resource
   requests are derived from substrate.kwokPod.requests × clusterCount
   + system-under-test resources.
3. **Cost ceiling.** `estimatedCost ≤ --max-cost-usd` (default $50).
   Prompt for confirmation if cloud-context regex matches, per
   the existing cost-guardrails surface.

### File layout (post-migration)

```
test/scaletest/
  profiles/
    dev-50.yaml           # laptop integration gate
    dev-500.yaml          # laptop rehearsal
    5k.yaml
    50k.yaml
    500k.yaml
    1m.yaml
    5m.yaml
    failover-leader-kill.yaml
    failover-shard-kill.yaml
    failover-partition.yaml
    failover-soak.yaml
  substrates/
    README.md             # schema doc + how to write your own
    example-fat-host.yaml      # 80/160 GiB, 25K Pods/cluster, etcd
    example-mid-host.yaml      # 32/128 GiB, 100K Pods/cluster, kine
    example-kind-laptop.yaml   # M5 Max kind, 2.5–10K Pods/cluster, tmpfs kine
```

Failover profiles embed their own cluster count today (50 × 1K)
because the test point is static-stability behaviour at a fixed
disturbance, not a scale-derived geometry. They stay as-is with
a substrate-derived per-host packing.

### Canonical invocation

```sh
# A 5K-machine scale-test on the fat-host example substrate:
scaletest-runner \
    --profile=test/scaletest/profiles/5k.yaml \
    --substrate=test/scaletest/substrates/example-fat-host.yaml \
    --duration=30m
```

`make scale-5k` etc. shortcuts continue to exist; they default to
`example-fat-host` as the canonical validation substrate.

## Migration plan

1. **Stage 0**: ADR sign-off (this document).
2. **Stage 1**: Substrate schema + Go types in `pkg/scaletest/config`.
   `make generate`-tested round-trip.
3. **Stage 2**: Runner merge logic. Unit-tested geometry derivation,
   ramp-feasibility validation, cost estimation.
4. **Stage 3**: Helm chart parameterization. The chart already
   takes a values file; the runner's job is to assemble the merged
   values from profile + substrate. Minimal chart change beyond
   exposing the geometry knobs that are currently per-profile.
5. **Stage 4**: Migrate existing profiles. Each `*-Nk.yaml` is
   split into a substrate-agnostic `<scale>.yaml` plus the
   matching example substrate file. dev-* and failover-* profiles
   pair with `example-kind-laptop.yaml` and an appropriate
   substrate respectively, without test-definition reshape.
6. **Stage 5**: Canonical reproduce. Run each new
   `<scale>.yaml + <substrate>.yaml` combination and compare bind
   ramp, cycle p99, and operator rollup p99 to the corresponding
   pre-migration baseline. Verdict must be "no measurable delta"
   for every (scale, substrate) pair before the legacy files are
   deleted.
7. **Stage 6**: Runbook update. The scaletest runbook reframes
   the profile table as "scales × your substrate".
8. **Stage 7**: Delete the legacy `scaleway-*` and `uber-*`
   profile files. Site sync picks up the new layout.

Roughly 4–6 hours of careful work. The in-flight OC1/OC2/OC3
disambiguation experiment from [ADR-0033] can complete on the
legacy naming and its verdict applies unchanged to the new
layout — the SHA and behaviour are what matter, not the file
path.

## Alternatives considered

### Option B: rename `uber-*` → bare scale names, drop `scaleway-*`

Simplest move. Half a commit. Loses the substrate-aware framing
entirely and bakes today's substrate-specific tuning into the
"canonical" profile shape. If a public user shows up wanting to
run on a different substrate, they're back to forking + tuning
each profile individually.

Rejected because the entanglement is the actual problem, not just
the prefix. Doing the rename without the split is moving the wart,
not fixing it.

### Status quo: keep `scaleway-*` + `uber-*` profiles

Cheapest in lines-of-code. Costs us a substrate prefix in every
filename, a confidentiality wart that needs scrubbing per public
artifact, and an N × M file growth as new substrates appear.

Rejected because the framing was wrong from the start; the recent
runbook update is the third or fourth round of working around it.

### Parametric substrate (`--substrate-host-vcpu=80 ...` CLI flags)

Substrate-as-flags instead of substrate-as-file. Equivalent
information; worse ergonomics. Three example substrate YAMLs are
self-documenting in a way `man scaletest-runner` isn't, and a
user who's iterating on a substrate description benefits from
file-based version control.

Rejected for ergonomic reasons; the data model is the same either
way.

## Hard rules touched

None of the load-bearing BigFleet hard rules (CLAUDE.md §"Hard
rules"). This ADR is purely about the scaletest harness file
layout; the system-under-test contract is unchanged. Specifically:

- Provider RPC surface: unchanged.
- Coordinator / shard / operator wire format: unchanged.
- Cost formula: unchanged.
- Static stability: unchanged. (Failover profiles continue to
  exercise it; they just get their per-host packing from a
  substrate file.)

Under this contract the test-vs-runtime separation becomes
explicit in the file layout: the **profile** is the canonical
test definition; the **substrate** is the user's runtime choice.

## References

- [ADR-0026] Scaletest models the Speculative tier.
- [ADR-0028] Cycle-p99 is regime-parametric.
- [ADR-0032] Realistic catalog production-calibrated workload distribution.
- [ADR-0033] Phase 1 supply-credit must respect bind readiness
  (the active design discussion; its in-flight validation
  experiment applies unchanged to the post-migration layout —
  the SHA and behaviour matter, not the file path).
- `docs/scaletest.md` (the runbook; reshapes under Stage 6).

[ADR-0026]: ./0026-scaletest-models-speculative-tier.md
[ADR-0028]: ./0028-cycle-p99-is-regime-parametric.md
[ADR-0032]: ./0032-realistic-catalog-production-calibration.md
[ADR-0033]: ./0033-phase1-supply-credit-respects-bind-readiness.md
