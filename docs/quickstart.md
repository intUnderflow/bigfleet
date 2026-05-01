# BigFleet quickstart

Bring up a single-node BigFleet (one coordinator, one shard) on a kind cluster, attach a fake provider, and watch a `CapacityRequest` flow through.

## Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop/) (or any Docker-compatible runtime)
- [kind](https://kind.sigs.k8s.io/)
- [helm](https://helm.sh/) ≥ 3.10
- `kubectl`
- This repo, cloned, with Go ≥ 1.22

## 1. Create a kind cluster

```sh
kind create cluster --name bigfleet-quickstart
```

## 2. Install the CRDs

```sh
kubectl apply -f api/crd/bigfleet.lucy.sh_capacityrequests.yaml
kubectl apply -f api/crd/bigfleet.lucy.sh_availablecapacities.yaml
kubectl apply -f api/crd/bigfleet.lucy.sh_upcomingnodes.yaml
```

## 3. Run BigFleet locally (all-in-one)

For the quickstart we run BigFleet on the host, not in the cluster — easier to read logs and inject test data.

```sh
go run ./cmd/bigfleet all-in-one \
  --shard-listen=:7780 \
  --coordinator-listen=:7790 \
  --metrics-addr=:8780 \
  --data-dir=$(mktemp -d -t bigfleet-quickstart-XXXX)
```

This launches:

- A coordinator listening on `:7790`
- A single shard listening on `:7780`
- An in-process fake provider seeded with a small idle inventory (see `cmd/bigfleet/all_in_one.go`)
- `/metrics` on `:8780`

Leave it running. Open a new terminal for the next steps.

## 4. Run the operator against your kind cluster

```sh
go run ./cmd/operator \
  --cluster-id=cluster-quickstart \
  --shard-addr=localhost:7780 \
  --kubeconfig=$HOME/.kube/config \
  --metrics-addr=:8770
```

The operator dials the shard, opens a `Shard.Session` stream, and starts emitting rollups every 10 s.

## 5. Create a CapacityRequest

One CR represents one pod's worth of capacity. To ask for two nodes, apply two CRs.

```sh
for i in 1 2; do
cat <<EOF | kubectl apply -f -
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: CapacityRequest
metadata:
  name: training-job-$i
spec:
  requirements:
    - key: node.kubernetes.io/instance-type
      operator: In
      values: [a3-highgpu-8g]
  resources:
    nvidia.com/gpu: "8"
  priority: 1000000
  interruptionPenalty: 8192
  reclamationPenalty: 65536
EOF
done
```

## 6. Watch it flow through

```sh
# CR transitions Pending → Acknowledged once the shard accepts it.
kubectl get capacityrequest -w

# UpcomingNode CRs appear as the shard provisions machines.
kubectl get upcomingnode -w

# AvailableCapacity CRs reflect what's idle.
kubectl get availablecapacity
```

In the BigFleet log you'll see Phase 1 emit `Bootstrap` actions and the fake provider walk machines through `Idle → Configuring → Configured`. After ~1 cycle (1 s) the CR transitions to `Acknowledged` and 2 `UpcomingNode` CRs appear in the cluster.

## 7. Watch metrics

```sh
curl -s localhost:8780/metrics | grep -E '^bigfleet_(shard|coordinator)_' | head -20
```

Key metrics to watch:

- `bigfleet_shard_cycle_duration_seconds` — should be well under 100 ms p99.
- `bigfleet_shard_actions_total{kind="Bootstrap"}` — increments per Phase 1 assignment.
- `bigfleet_shard_inventory_machines{state="Configured"}` — should be 2 after the CR is acknowledged.
- `bigfleet_shard_shortfalls` — should be 0.

## 8. Tear down

```sh
# Stop the BigFleet and operator processes (Ctrl-C).
kind delete cluster --name bigfleet-quickstart
```

## What you just demonstrated

- The full rollup → decision → provision → acknowledgement loop.
- Static stability: kill the BigFleet process between steps 6 and 7 and the kind cluster keeps running.
- The provider abstraction: the fake provider is interchangeable with a real provider implementing the same six RPCs.

## Next steps

- Real install (multi-cluster, three coordinator replicas, in-cluster shard): [`operator-guide.md`](operator-guide.md).
- Sizing for production: [`scaling-guide.md`](scaling-guide.md).
- Writing your own provider: [`provider-author-guide.md`](provider-author-guide.md).
- The full design rationale: the [BigFleet paper](https://lucy.sh/bigfleet) (vendored at [`papers/bigfleet.md`](papers/bigfleet.md)).
