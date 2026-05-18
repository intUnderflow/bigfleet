# Scale-test substrates (ADR-0034)

A **substrate** describes where you'll run a BigFleet scale test: per-host capacity, per-cluster apiserver operating point, kwok-pod resources, storage backend, and cost. The runner pairs one substrate with one profile (under `../profiles/`) to compute geometry and emit Helm values.

```sh
scaletest-runner \
    --profile=../profiles/50k.yaml \
    --substrate=./example-fat-host.yaml \
    --output=./out
```

The three example files in this directory are starting points — copy one, adjust to your actual hardware, and check it into your own repo. The substrate is *yours*; the profile is canonical.

## What to fill in

| Field | What it is |
|---|---|
| `host.vCPU` / `host.memoryGiB` | The hardware shape you're packing kwok pods onto. |
| `cluster.podsPerCluster` | Your per-apiserver "comfortable Pod ceiling" — past this point bind throughput tails off (kine WAL pressure, etcd watch latency, kube-scheduler list-watch cost). |
| `cluster.clustersPerHost` | How many kwok pods you pack per host. Must satisfy `2 × kwokPod.requests × clustersPerHost ≤ host.vCPU/memoryGiB` with headroom for the system-under-test (shard, coordinator, prometheus). |
| `cluster.storage` | `etcd` or `kine`. Real etcd handles bigger objects/cluster; kine is fine up to ~25K and is cheaper to run. |
| `cluster.bindThroughputPodsPerSec` | Empirical from a short run on your hardware. Used for ramp-feasibility validation only — wrong values warn but don't block. |
| `kwokPod.requests` / `kwokPod.limits` | Per-container budget. The chart applies these identically to the kwok pod's apiserver and workload containers. Per-Pod totals are 2×. |
| `costEstimate.perHostUsdPerHour` | Your provider's per-hour rate for this host shape. `0` for free / on-prem. |

## Examples

- **`example-fat-host.yaml`** — 64 vCPU / 128 GiB hosts, 10 KWOK clusters per host, etcd, 25K Pods/cluster. Roughly AWS `c6i.16xlarge` / GCP `n2-standard-64`.
- **`example-mid-host.yaml`** — 32 vCPU / 128 GiB hosts, one large cluster per host, kine, 100K Pods/cluster. Roughly Scaleway PRO2-L / AWS `c6i.8xlarge`.
- **`example-kind-laptop.yaml`** — Docker Desktop on a developer laptop, 5 small clusters, kine on tmpfs. For dev/integration rehearsals.

## Compatibility

The `5k` / `50k` / `500k` / `1m` / `5m` profiles work on any substrate that fits the geometry. The `dev-*` and `failover-*` profiles (legacy bundled shapes) work best on `example-kind-laptop`.
