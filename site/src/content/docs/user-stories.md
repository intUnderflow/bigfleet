---
title: "BigFleet user stories"
description: "How BigFleet shows up in the day-to-day of the people who depend on it. Each story walks from \"what life was like before\" to \"what they actually do now\" to \"what they gain.\" Names are fictional; th…"
---

How BigFleet shows up in the day-to-day of the people who depend on it. Each story walks from "what life was like before" to "what they actually do now" to "what they gain." Names are fictional; the workflows are concrete.

## 1. Maya submits a training job and her pod schedules in 90 seconds

**Role**: ML researcher at a mid-sized AI lab. She runs ~5 fine-tuning jobs a week, each needing 8×H100s for 4 hours.

**Before BigFleet** — Maya's lab ran a single autoscaler per cluster. When she submitted a 8-GPU pod and the cluster's GPU pool was at capacity, the autoscaler eventually noticed (every 60s), eventually called AWS to ask for a node (90s API call), and eventually the node joined and the kubelet became Ready (4–8 min). Total: 6–10 minutes of pod-pending while she stared at `kubectl get pods`. If the cluster had no a3-highgpu-8g quota left in `us-west-2`, the pod sat pending forever and Maya had to manually file a ticket to the platform team to switch regions.

**With BigFleet** — Maya creates a `Pod` with `priorityClass: ml-research` and `nvidia.com/gpu: 8` like she always has. She doesn't know BigFleet exists. The cluster's `bigfleet-unschedulable-pod-controller` notices her pending pod and creates a `CapacityRequest` CR. The operator includes that request in the next 10s rollup to its shard. The shard's Phase 1 either:

- finds an existing Idle a3-highgpu-8g machine and configures it for her cluster (~30 seconds), or
- asks the AWS provider to provision a fresh one from a *pre-warmed Speculative pool* the platform team maintains (45 seconds for AWS to ack + 30 seconds for kubelet join = ~90 seconds), or
- preempts a low-priority `priorityClass: nightly-eval` job from another team's job whose `interruptionPenalty` is lower than her workload's value to BigFleet (~20 seconds — Phase 2).

**The win**: Maya's median time-to-pod-running went from 7 minutes to 90 seconds. When the cluster's GPU pool is saturated, BigFleet quietly preempts lower-priority workloads instead of failing — so she stops needing the platform team's intervention. Maya never has to learn what BigFleet *is*; the experience is just "kubectl apply, pod runs."

```yaml
# What Maya writes
apiVersion: v1
kind: Pod
metadata:
  name: finetune-llama-7b-2026-05-01
spec:
  priorityClassName: ml-research          # 1,000,000
  containers: [...]
  nodeSelector:
    node.kubernetes.io/instance-type: a3-highgpu-8g
  resources: { limits: { nvidia.com/gpu: 8 } }
```

```yaml
# What the unschedulable-pod-controller creates (auto, not Maya's job)
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: CapacityRequest
metadata:
  name: pod-finetune-llama-7b-2026-05-01
  ownerReferences: [{ kind: Pod, name: finetune-llama-7b-2026-05-01 }]
spec:
  count: 1
  profile:
    requirements:
      - { key: node.kubernetes.io/instance-type, operator: In, values: [a3-highgpu-8g] }
    resources: { nvidia.com/gpu: "8" }
    priority: 1000000
    interruptionPenalty: 8192
    reclamationPenalty: 65536
```

## 2. Sven launches a 256-GPU job at 9am Tuesday and BigFleet rebalances 3 clusters around it

**Role**: ML platform engineer running a 12-cluster fleet. He owns the team's 256-GPU training reservations.

**Before BigFleet** — Sven's 256-GPU jobs were launched once a week, and the routine was: at 8:55am, manually drain low-priority workloads from `cluster-prod-us-west-2a` and `cluster-prod-us-west-2b` to free capacity. Coordinate with two on-call engineers. Watch the autoscaler crawl. Ten percent of the time, capacity wasn't fully available because an unrelated pod had grabbed an a3 node in the last hour and Sven had to file a one-off cordon request. The 256-GPU job started 20–40 minutes after he hit submit, and his 5-engineer team waited.

**With BigFleet** — Sven's 256-GPU `Job` has `priorityClass: ml-research-flagship` (priority 100M) and `interruptionPenalty: 1048576` ($1M, signalling "do not interrupt this once started"). The unschedulable-pod-controller creates 32 CRs (one per pod for the 8-GPU per-pod shape). His shard sees 32 high-priority requests against its NeedsTable's existing population.

Phase 1 finds 156 GPUs idle. Phase 2 walks the shortfall and finds 100 candidate victims across 4 different clusters running `priorityClass: nightly-eval` (priority 1000), all with `interruptionPenalty: 256` — these workloads' owners explicitly *signed up* to be preemptable in exchange for cheaper effective cost. Phase 2 issues drain instructions to the 4 cluster operators in parallel; each operator gracefully drains its victim pods (PDBs respected, drain grace 10 min) and acknowledges the reclaim. The 4 clusters' freed nodes are reassigned to Sven's flagship cluster across the next two cycles.

**The win**: Sven submits at 8:59am and his 32 pods are all running by 9:08am — a 9-minute spinup with no human coordination. The nightly-eval team's owners get a Slack message (via their team's monitoring on `bigfleet_shard_actions_total{kind="Reclaim"}`) and adjust their job timestamps for tomorrow. Sven's team stops losing 30 minutes a week to capacity orchestration. Annual savings: ~25 hours of senior-engineer time per quarter, plus ~$2K of GPU-hours that used to be reserved-but-idle in the "drain and prep" window.

## 3. Priya runs `cluster-prod-eu` and stops thinking about autoscaler tuning

**Role**: SRE owning two production clusters in EU regions.

**Before BigFleet** — Priya owned `cluster-autoscaler` configuration for her two clusters: per-node-group `min`/`max`/`scale-down-delay-after-add`/`balance-similar-node-groups`. Each cluster had ~15 instance types across 3 zones, each with its own `MachineSet`. Tuning was per-cluster and the autoscaler couldn't see what was happening in `cluster-prod-us`. She'd often see `cluster-prod-eu-1` over-provision while `cluster-prod-us-1` was at capacity. She maintained per-cluster runbooks, per-cluster Grafana dashboards, per-cluster on-call.

**With BigFleet** — Priya runs the `bigfleet-operator` Helm chart in each of her clusters. The operator has 3 settings: `clusterID`, `shardAddress`, and a kubeconfig. She doesn't tune anything else per-cluster. BigFleet's shard owns the decision-making, including preferring to satisfy demand from a freshly-Idle US node over creating a new EU one when EU spot capacity is constrained.

Priya's runbook is now one page: "If `bigfleet_operator_session_reconnects_total` is rising, check the operator's stream connectivity to the shard." Everything else delegates to the platform team that owns BigFleet.

**The win**: Priya stops being an autoscaler expert. She owns less code, has fewer alerts, and her clusters' fleet-level utilisation is consistently 5–10 percentage points higher because BigFleet rebalances across her clusters and the US/APAC regions she doesn't even own. When a new instance type ships (say, GH200), the platform team adds it to BigFleet's provider config once, and her cluster gets it for free with no MachineSet edit on her end.

## 4. Marcus owns BigFleet for a 200-cluster company and watches utilisation climb 33 percentage points

**Role**: Principal platform engineer at a large AI/SaaS company. Marcus owns BigFleet's deployment, the provider implementations, and the fleet-level capacity model.

**Before BigFleet** — Marcus's 200 production clusters each ran their own cluster-autoscaler. Average GPU utilisation was 45% — meaning 55% of the company's $20M/yr GPU spend was idle capacity sitting in clusters that didn't currently need it. Per-cluster autoscalers couldn't share. Marcus had spent 18 months on a homegrown "cross-cluster scheduler" that mostly didn't work.

**With BigFleet** — Marcus deploys BigFleet's coordinator (3 replicas) and 4 shards (one per region). Each cluster runs an operator. Three weeks of adoption later, he can show an executive dashboard:

- Fleet-level GPU utilisation: **78%** (up from 45%).
- Mean time to schedule a high-priority pod: **84 seconds** (down from 8 min).
- Number of clusters where humans manually drain workloads to make room for high-priority work: **0** (down from ~50/week).
- Cost of capacity that's idle but-could-have-served-demand-elsewhere: **$340K/yr** (down from $9M/yr).

The 33-percentage-point utilisation lift translates to **$6.6M/yr saved** — roughly 30× the 5-engineer cost of running BigFleet itself. Marcus's metrics dashboard, [`docs/operator-guide.md` runbook](/operator-guide/), and the [scaling guide](/scaling-guide/) are now the company's standard references for "how the fleet works."

**The win**: Marcus retired his homegrown cross-cluster scheduler. BigFleet's static-stability guarantee meant the migration was incremental — clusters could opt in one at a time without ripping out their existing CAs first. The CFO's quarterly review now includes a specific BigFleet line item.

## 5. Elena uses penalty buckets to identify $400K of overprovisioned interruption-penalty

**Role**: FinOps analyst. Owns the quarterly cost review.

**Before BigFleet** — Elena could see GPU spend by cluster, but not by *workload*. She knew the company spent $20M/yr on compute, but couldn't tell which 30% of jobs were paying premium for "must not be interrupted" guarantees they didn't actually need. The cluster-autoscaler model had no concept of "cost of interrupting this workload."

**With BigFleet** — Every `CapacityRequest` carries an `interruptionPenalty: $X`. The cost is bucketed (powers of 2 from $0.50 to $8.4M) and exported as a Prometheus metric: `bigfleet_shard_inventory_machines{interruption_penalty_bucket=...}`. Elena queries:

```promql
sum by (interruption_penalty_bucket) (
  bigfleet_shard_inventory_machines{state="Configured"}
)
```

She finds: 14% of configured machines belong to workloads with `interruptionPenalty: $1,048,576` (bucket 20) — but on inspection, half of those workloads are *batch nightly evaluations* whose engineers blindly copied their `interruptionPenalty` from the production training jobs' values.yaml.

Elena raises a tracking ticket. Three teams audit and drop their `interruptionPenalty` to $1024 (bucket 10), which moves them to a cheaper effective-cost tier where BigFleet is willing to schedule them on spot capacity. Q-on-Q the company's spot-vs-on-demand mix shifts from 18% spot to 41% spot.

**The win**: $400K/yr of compute reallocated from on-demand to spot, with no impact on the workloads (because they were always preemptable; their owners just hadn't expressed it). Elena's quarterly review is the first time the company has had per-workload cost-of-interruption visibility.

## 6. Devon gets paged at 3am — capacity stockout, fleet-wide

**Role**: SRE on call for the platform team.

**Before BigFleet** — A 3am page about "GPU capacity stockout" used to mean Devon manually checking 12 cluster autoscalers' status, identifying which clouds/regions had no quota left, deciding whether to file an emergency quota increase, and manually evicting low-priority work. Average resolution: 90 minutes. Average mean-time-to-page-someone-else: 25 minutes.

**With BigFleet** — The page fires off a single Prometheus alert: `bigfleet_shard_shortfalls > 0 for 5m`. Devon opens the runbook ([operator-guide §runbook](/operator-guide/#runbook)) and follows the playbook:

1. `kubectl get capacityrequests --all-namespaces -o jsonpath='{.items[?(@.status.phase=="Shortfall")]...}'` to see which workloads are unsatisfied.
2. Check `bigfleet_shard_inventory_machines{state="Idle"}` to confirm the fleet *can't* satisfy without new provisioning.
3. Check the AWS provider's Get RPCs in Prometheus to see if cloud quota is the actual bottleneck.

Within 8 minutes Devon identifies that `nvidia.com/gpu` quota in `us-west-2` is exhausted and `eu-west-1`'s spot price has spiked. He files a one-line quota request for us-west-2 and lets the eu-west-1 spot constraint resolve naturally over the next hour as workloads finish. He goes back to bed.

**The win**: The 90-minute incident becomes an 8-minute one. Devon doesn't have to know per-cluster autoscaler internals — there's one fleet-level metric that tells him "is BigFleet currently failing to satisfy demand, and if so, why." Annual on-call burden drops by ~40% for capacity-related pages.

## 7. Lin implements an `IronicProvider` and ships it the same week

**Role**: Engineer on a private-cloud team. They run thousands of bare-metal nodes via Ironic and want them to participate in the BigFleet fleet alongside the AWS/GCP nodes.

**Before BigFleet** — A "new cluster autoscaler provider" used to mean forking `cluster-autoscaler`, adding their backend, and shipping a custom build per cluster. The fork was a maintenance burden the team carried for years. They couldn't merge upstream because the provider was internal.

**With BigFleet** — Lin reads [`provider-author-guide.md`](/provider-author-guide/), runs `make conformance` against a stub on her laptop to confirm she understands the protocol, then implements `Create`/`Configure`/`Drain`/`Delete`/`Get`/`List` in a new repo (`github.com/initial-orange/bigfleet-ironic-provider`). The proto contract is fixed; her code never touches BigFleet itself.

She points the BigFleet conformance suite at her provider's gRPC endpoint. It runs 47 test scenarios (idempotency, transitional-state recovery, since_revision cursor correctness, drain interruption handling). Three pass on day one; she fixes the failures in the rest over two days. By Friday she's running her provider in staging alongside the AWS provider — both registered in BigFleet's coordinator, both serving capacity.

**The win**: Two developer-weeks instead of months. Her provider repo is independent — she ships releases on her cadence, BigFleet ships on its own. When Ironic adds a new feature, she lands it in her provider without an upstream BigFleet PR. The conformance suite gives her ongoing confidence that her provider stays compatible as BigFleet evolves.

## 8. Theo plans next quarter's capacity and trusts the projection

**Role**: Capacity planner / FinOps. Owns the quarterly hardware plan that the procurement team uses.

**Before BigFleet** — Theo used per-cluster utilisation metrics to project quarterly capacity. The output was always too conservative because each cluster's autoscaler was unaware of cross-cluster demand. He'd reserve 30% headroom per cluster — totalling 30% of fleet — but only ~12% headroom was ever needed in practice (because peaks didn't align across clusters). The over-reservation cost the company ~$1.8M/yr.

**With BigFleet** — Theo queries the BigFleet shard-level history:

```promql
quantile_over_time(0.99,
  sum(bigfleet_shard_inventory_machines{state=~"Configured|Configuring"})[90d]
)
```

He sees the *fleet-level* p99 demand, not per-cluster. From that, he applies a 12% headroom buffer (informed by [`scaling-guide.md`](/scaling-guide/)'s tier-by-tier sizing tables). He shares the projection with Marcus's team and the procurement team in a single Confluence page.

**The win**: $1.8M/yr saved by reserving headroom against the *aggregate* demand peak rather than the sum of per-cluster peaks. The plan is auditable — each cluster's contribution is visible — and rolls forward by quarter. A fight that used to take a week of Theo + 3 cluster owners arguing now takes 2 hours.

## 9. Aisha runs the failover-soak before every release and sleeps better

**Role**: Reliability engineer on the platform team. Owns BigFleet's release process.

**Before BigFleet** — Pre-release validation for the cluster-autoscaler stack used to involve manually killing the running CA pod in staging, watching the cluster, and "feeling out" whether anything broke. There was no quantitative answer to "does the system survive this failure mode."

**With BigFleet** — Aisha runs the `failover-soak` profile from the [scale-test harness](/scaletest/) before every BigFleet release:

```sh
scaletest-runner \
  --profile=test/scaletest/profiles/failover-soak.yaml \
  --duration=60m \
  --output=./results/$(date +%Y%m%d)-failover/
```

The profile spins up 50 simulated clusters (KWOK), holds steady at 50K demand for 10 minutes, then kills the coordinator's Raft leader. It does this twice during the run. The runner's `summary.json` shows whether the cluster operators reconnected within the SLO and whether *any* configured machine flipped state during the coordinator outage.

The expected outcome — and the one that's confirmed every release — is that **zero cluster-side errors occur during 60s of leader churn**. Static stability holds. If it didn't, the release blocks.

**The win**: Every release is gated by an automatic, quantitative "does static stability still hold" check. Aisha can answer "what happens if Raft is wedged for 30 seconds" with a number, not a hand wave. Trust in the deploy process goes from "we hope" to "we measured 32 times this quarter and it held."

## 10. Hiroshi never learns BigFleet exists

**Role**: Backend engineer on the company's core product team. Submits ~20 pods a week as part of a long-running stateful service.

**Before BigFleet** — Hiroshi's experience was "submit pod, wait, sometimes file a ticket if it doesn't schedule." He learned the names of the cluster-autoscaler's failure modes the hard way.

**With BigFleet** — Hiroshi's pods specify `priorityClassName: prod-stateful` (priority 10000), `nvidia.com/gpu: 0`, plenty of CPU and memory, no special signals. The unschedulable-pod-controller creates a CR for him. The operator includes it in the rollup. The shard's Phase 1 satisfies his demand from existing Idle inventory in his cluster every single time, in under a second.

Hiroshi never reads BigFleet docs. He never sees a BigFleet alert. He never gets paged about capacity. The system is invisible to him — which is the highest possible compliment.

**The win**: For the 95% of the company who isn't a platform engineer, BigFleet is *not noticed*. The other 5% (Maya, Sven, Marcus, Devon, Elena, Theo, Aisha, Lin, Priya) get the leverage that makes the 95% experience seamless.

---

## Common patterns across these stories

- **Static stability is felt as "the system never bites me when it has problems."** Priya, Hiroshi, and Maya never see BigFleet outages because clusters keep running. Devon's 3am page is about *the fleet being out of capacity*, not about *BigFleet being broken*.
- **Priority + interruption-penalty is the language everyone speaks.** Sven uses it to express "don't interrupt my flagship job." Elena uses it to find overprovisioning. The nightly-eval team uses it to opt into spot-pricing in exchange for preemptability. The same two numbers carry signal across roles.
- **Out-of-tree providers mean the platform team can ship fast.** Lin's two-week Ironic provider would have been a multi-quarter fork in the cluster-autoscaler world.
- **Fleet-level visibility unlocks decisions per-cluster autoscalers can't see.** Marcus's 33-point utilisation lift, Theo's $1.8M reservation reduction, and Elena's $400K spot-shift all come from looking at the fleet, not summing the clusters.

## Where to go next

- New to BigFleet? Start with the [quickstart](quickstart/).
- Curious how the engine works? [Architecture](architecture/).
- Ready to operate it? [Operator guide](operator-guide/).
- Writing a provider? [Provider author guide](provider-author-guide/).
