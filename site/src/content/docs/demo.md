---
title: "The demo — a hands-on tour"
description: "Run a real, miniature BigFleet on your own machine and drive it from the terminal: provision, reclaim, preempt, and watch capacity move across a finite fleet."
---

The best way to understand BigFleet is to watch it move capacity. This page is a **hands-on tour you run
on your own hardware** — a real, miniature BigFleet against a simulated substrate, driven entirely from
your terminal. It takes a few minutes and tears down cleanly.

(A hosted, browser-based version sometimes runs at [bigfleet-demo.lucy.sh](https://bigfleet-demo.lucy.sh),
but it lives on a small shared server and is often offline — so this tour doesn't depend on it.)

## What you'll run

The demo is a real, miniature BigFleet deployment:

- **Three real Kubernetes clusters** — each a genuine `kube-apiserver` plus the **stock upstream
  `kube-scheduler`** (native priority, preemption, and `MostAllocated` bin-packing), stood up with
  [`kwokctl`](https://kwok.sigs.k8s.io/). Their nodes are [kwok](https://kwok.sigs.k8s.io/) fakes.
- **One BigFleet shard** driving a per-cluster **operator**, **unschedulable-pod controller**, and a small
  **node-creator** that turns each `UpcomingNode` into a kwok `Node`.
- **Three real `CapacityProvider` backends** — on-prem, AWS, GCP — implementing the genuine
  [six-RPC provider contract](/provider-author-guide/) and passing the conformance suite (`core,cloud,spot`).
  They mint kwok nodes instead of booting VMs, but everything above them is the real engine.
- **A finite fleet of ~120 nodes** across three cost tiers (below). It cannot grow past its ceiling —
  scarcity is the point, because that is when BigFleet has to make decisions.

### What's real, what's simulated

BigFleet's demos follow one rule: **never oversell**.

- **Real** — the engine and all three phases (assign / preempt / reclaim) with the effective-cost and
  victim-score math; the operator↔shard stream and the `CapacityRequest` / `AvailableCapacity` /
  `UpcomingNode` CRDs; the three conformance-certified providers and the capacity types, prices, and
  interruption probabilities they declare; and real Kubernetes scheduling (stock `kube-scheduler` per
  cluster — priority, preemption, bin-packing).
- **Simulated** — the cloud itself (`on-prem`/`AWS`/`GCP` are labels on kwok fakes, not VMs); the dollar
  prices and interruption probabilities (illustrative author-chosen constants, *not* cloud quotes — the
  engine reasons about them, but no *measured* saving is claimed); and provisioning latency (a configured
  dwell, so a node takes tens of seconds to appear — labelled *"real decision; transfer speed simulated"*).

Every node and pod carries `bigfleet.demo/simulated: "true"`; nodes keep a `kwok://` providerID.

### The three cost tiers

BigFleet ranks capacity by **`effective_cost = price + interruption_probability × penalty`**:

| Tier | What | Marginal price | Interruptible | BigFleet uses it… |
|---|---|---|---|---|
| **Committed** | owned on-prem + reserved cloud | `$0` (already paid) | no | **first** |
| **On-demand** | elastic GCP / AWS | ~`$0.38`/node·hr (illustrative) | no | for interruption-**sensitive** demand once committed is full |
| **Spot** | the same SKUs, cheaper | ~`$0.11`/node·hr (illustrative) | yes (~30%) | for interruption-**tolerant** demand |

## Start it

On a Docker-capable machine with Go installed:

```sh
git clone https://github.com/intUnderflow/bigfleet-demo
cd bigfleet-demo
# the sibling engine + providers checkouts must be present alongside it:
#   ../bigfleet  ../bigfleet-providers   (pinned in ci/sibling-pins.env)
hack/demo-up.sh
```

`demo-up.sh` builds the binaries, stands up three `kwokctl` clusters, starts the shard + the three
providers + per-cluster controllers, and serves a browser UI on `http://localhost:8090`. We'll drive
everything from the terminal below; the UI is an optional richer view of the same state.

> **Provisioning is deliberately gradual.** Nodes appear over tens of seconds (the simulated dwell, plus
> the demo runs the shard's executor serially). Give each step a moment and re-run `demo-observe.sh`.

## Watch the fleet

`demo-observe.sh` is your window — per-cluster node counts broken down by tier (from the provider-declared
`bigfleet.demo/billing` label), plus the shard's **real** inventory and action counters:

```sh
hack/demo-observe.sh
```

```
  cluster-a  nodes=10  [ owned:7 reserved:3 ] pending=0
  cluster-b  nodes=10  [ owned:6 reserved:4 ] pending=0
  cluster-c  nodes=10  [ owned:7 reserved:3 ] pending=0
  --- shard inventory (REAL: BigFleet's declared capacity, by type+state) ---
    Configured BareMetal = 20
    Configured Reserved = 10
    Idle BareMetal = 8
    Idle Reserved = 10
    Speculative OnDemand = 48
    Speculative Spot = 24
  --- shard actions (REAL: Bootstrap/Provision/Reclaim/Preempt) ---
    bigfleet_shard_actions_total{kind="Bootstrap"} 30
```

At rest, the baseline workloads run entirely on **committed** capacity (`owned` + `reserved`) — the
on-demand and spot tiers sit as `Speculative` quota at `$0`. Run it under `watch` to see changes live:

```sh
watch -n2 hack/demo-observe.sh
```

## Drive it

`demo-workload.sh <cluster> <level> [demand|critical]` sets a cluster's demand — the same `/api/demand`
the UI buttons use. `level` is node-equivalents (0–40); `demand` is everyday production (interruption-
tolerant), `critical` is high-priority (interruption-sensitive).

### 1. Fill committed, then burst to cloud

```sh
hack/demo-workload.sh cluster-a 20      # add production demand
```

Re-run `demo-observe.sh`: cluster-a's pending pods drive BigFleet to provision nodes — it fills the
**already-paid-for committed** pool first (the `owned`/`reserved` counts climb, cost stays `$0`), then
bursts onto metered cloud. Committed-before-cloud is real Phase-1 cheapest-first logic.

### 2. Spot vs on-demand — the cost choice

Push past committed so the cloud tiers open, and look at *which* cloud tier each kind of demand provisions:

```sh
hack/demo-workload.sh cluster-a 30          # tolerant → cheap SPOT
hack/demo-workload.sh cluster-b 8 critical  # sensitive → stable ON-DEMAND
```

After they provision, list nodes by tier:

```sh
kubectl --kubeconfig run/cluster-a.kubeconfig get nodes -L bigfleet.demo/billing
```

BigFleet provisions **spot** for the tolerant demand (cheapest effective cost) and **on-demand** for the
critical demand (spot's interruption risk outweighs the saving). That routing is real engine math — the
provider declares each tier's price and interruption probability, and Phase 1 sorts by effective cost.
(Which *pod* lands on which node is then the stock kube-scheduler's bin-packing — a separate layer.)

### 3. Saturate the finite fleet

Drive demand past the fleet's ceiling:

```sh
for c in cluster-a cluster-b cluster-c; do hack/demo-workload.sh $c 40; done
```

Committed fills, then spot, then on-demand — and the remaining pods stay **Pending** (`demo-observe.sh`
shows non-zero `pending`). There is nowhere left: a real fleet is finite, and this is BigFleet at the edge
of it.

### 4. Critical demand → native preemption

With the fleet full, send high-priority demand into a cluster:

```sh
hack/demo-workload.sh cluster-a 10 critical
```

The stock `kube-scheduler` **preempts** lower-priority batch pods to make room, and BigFleet's Phase 2
frees committed machines for the critical work. The preemptions are real Kubernetes events:

```sh
kubectl --kubeconfig run/cluster-a.kubeconfig get events -A \
  --field-selector reason=Preempted | head
```

### 5. Move capacity between clusters

Clear cluster-a's demand and raise cluster-b's:

```sh
hack/demo-workload.sh cluster-a 0       # drop demand on A
hack/demo-workload.sh cluster-b 30      # raise demand on B
```

Watch with `demo-observe.sh`: cluster-a's committed nodes drain back to the shared idle hub, and cluster-b
draws from that freed capacity — capacity **moves** across the fleet, reused, with no new spend. This is
the whole reason BigFleet exists: it is a fleet-level autoscaler, not a per-cluster one.

> The orchestrated versions of saturate / preempt / move are also one call each —
> `curl -XPOST localhost:8090/api/scenario -d '{"name":"saturate"}'` (or `critical`, or `move`) — which
> sequence the beats and narrate them in the on-screen decision feed.

## Reset and tear down

```sh
curl -XPOST localhost:8090/api/reset    # back to the at-rest baseline ($0, committed-only)
hack/demo-down.sh                       # delete the clusters + stop everything
```

## How it maps to BigFleet's architecture

Every step above is a real path through the engine:

1. You add demand → pods go Pending → the **unschedulable-pod controller** writes a `CapacityRequest` CR.
2. The **operator** rolls those into a `Need` and streams it to the **shard** over `Shard.Session`.
3. The shard diffs needs vs provisioned inventory and runs the **three phases**, provisioning, reclaiming,
   and rebalancing **nodes** (not pods).
4. It actuates through a **`CapacityProvider`** over the six RPCs; the provider's declared capacity
   (type / price / interruption) is what the cost math reasons over.
5. Provisioned machines surface as `UpcomingNode`s, node-creator mints the kwok `Node`, and the stock
   `kube-scheduler` binds the pods.

See [Concepts](/concepts/) for the vocabulary, [Architecture](/architecture/) for the full shape, and the
[Provider author guide](/provider-author-guide/) for the contract the demo's three providers implement.
