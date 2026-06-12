# BigFleet operator guide

For the human running BigFleet — installing it, monitoring it, responding when something is wrong.

## Kubernetes version support

**Minimum: Kubernetes 1.31.** The helm charts (`bigfleet`, `bigfleet-operator`, `bigfleet-unschedulable-pod-controller`) declare `kubeVersion: ">= 1.31.0-0"` and helm will refuse to install on older clusters.

The binding constraint is the `CapacityRequest` CRD's `spec.versions[].selectableFields` block (used by the on-call runbook's `kubectl --field-selector=status.phase=Pending` query). SelectableFields went GA in Kubernetes 1.31; in 1.30 they're behind the `CustomResourceFieldSelectors` feature gate and require explicit opt-in. Older clusters reject the CRD at install time.

Other features BigFleet uses, all from earlier releases (listed for completeness, not as additional requirements):

| Feature | Used by | Stable since |
|---|---|---|
| CRD `subresource:status` | All `bigfleet.lucy.sh/v1alpha1` resources | 1.16 |
| `policy/v1` Eviction | M20 ReclaimInstruction handler (PDB-respecting drain) | 1.22 |
| `coordination.k8s.io/v1` Leases | controller-runtime managers | 1.14 |
| `NetworkPolicy` egress | M17.x `partition-coordinator-from-shard-N` failover-test action | 1.8 |
| `statefulset.kubernetes.io/pod-name` label | M17.x partition NetworkPolicy podSelector | 1.13 |

If you need to run BigFleet on a 1.30 cluster, the workaround is to drop the `selectableFields` block from the CRD before applying it and use the user-stories runbook's pre-M21 `jq` command for status-phase filtering. Not officially supported; we don't run CI against it.

## Architecture in one paragraph

BigFleet is two tiers. The **coordinator** (Tier 1) owns Raft-replicated fleet state — shard membership, cluster→shard map, topology-domain→shard assignments, quota allocations, provider registry. The **shards** (Tier 2) own machines and make provisioning decisions on their hot path. Per-cluster **operators** dial a shard over a long-lived bidirectional gRPC stream. The coordinator does *not* make provisioning decisions; shards do. Static stability is the load-bearing safety property: clusters keep running with BigFleet entirely down. (See the [BigFleet paper](https://lucy.sh/bigfleet) for the full architecture, also vendored at `docs/papers/bigfleet.md`.)

## Components

| Component | Where it runs | What it owns |
|---|---|---|
| Coordinator | Standalone (3 replicas, Raft) — typically a dedicated namespace | Membership, cluster→shard, domain→shard, quotas, provider registry |
| Shard | Standalone (~200 replicas at 100M-node scale; start with 1) | Per-shard machine inventory, decision engine, operator session terminations |
| Operator | One per Kubernetes cluster you want BigFleet to manage | CapacityRequest informer + roll-up; bootstrap blob renderer; reclaim handler |
| `bigfleet-unschedulable-pod-controller` | Optional, per cluster | Watches Pods → creates CapacityRequests for unschedulable ones |
| Provider | One process per real backend (cloud / bare metal). Lives in **separate repos**. | The actual machine lifecycle |

## Install

The coordinator and shards live in your management cluster (or a dedicated cluster). Each managed cluster runs its own operator. Charts are published to GHCR as OCI artefacts on every push to main; pin to the chart version (`Chart.yaml`'s `version` field) for reproducibility.

```sh
# 1. Install the BigFleet CRDs into every cluster you want managed.
kubectl apply -f https://github.com/intUnderflow/bigfleet/raw/main/api/crd/bigfleet.lucy.sh_capacityrequests.yaml
kubectl apply -f https://github.com/intUnderflow/bigfleet/raw/main/api/crd/bigfleet.lucy.sh_upcomingnodes.yaml
kubectl apply -f https://github.com/intUnderflow/bigfleet/raw/main/api/crd/bigfleet.lucy.sh_availablecapacities.yaml

# 2. Deploy the coordinator + shard control plane on the management cluster.
helm install bigfleet oci://ghcr.io/intunderflow/charts/bigfleet \
  --version 0.1.0 \
  --namespace bigfleet-system --create-namespace \
  --set coordinator.replicas=3
# Quorum forms automatically (ADR-0047): ordinal 0 bootstraps, ordinals
# 1..N join via the leader. coordinator.bootstrap=true (the default) is
# safe to leave set for the life of the install — only ordinal 0
# honours it, and only when its data dir is empty.

# 3. On each managed cluster, install the per-cluster operator.
helm install bigfleet-operator oci://ghcr.io/intunderflow/charts/bigfleet-operator \
  --version 0.1.0 \
  --namespace bigfleet-system --create-namespace \
  --set clusterID=cluster-prod-eu \
  --set shardAddress=bigfleet-shard.bigfleet-system:7780

# 4. (Optional) Install the unschedulable-pod controller on clusters
#    where you want BigFleet to react to Pod scheduling failures.
helm install bigfleet-unschedulable-pod-controller \
  oci://ghcr.io/intunderflow/charts/bigfleet-unschedulable-pod-controller \
  --version 0.1.0 \
  --namespace bigfleet-system

# 5. Register your providers with the coordinator (one CLI per provider).
#    Provider authoring is documented in docs/provider-author-guide.md.
```

Equivalent install commands using a git checkout (useful for development or air-gapped environments) replace `oci://ghcr.io/intunderflow/charts/<chart> --version <V>` with `./deploy/helm/<chart>` and drop the version flag.

## Transport security (mTLS + identity)

BigFleet ships plaintext by default — the quickstart and the scaletest harness stay zero-config, and a plaintext install is making the ADR-0008 trust-the-network choice with eyes open. For any deployment where a shard or coordinator is reachable from more than one trust domain, enable [ADR-0048](adr/0048-mtls-and-uri-san-identity.md) mTLS: it encrypts every gRPC edge **and** binds the protobuf-asserted identities (`Hello.cluster_id`, `ShardReport.shard_id`) to the client certificate, which is what actually stops one cluster impersonating another (stealing its reclaim instructions, or zeroing its capacity with a forged full-replacement roll-up).

### How it works

Every binary takes the same three flags; the charts wire them when you set a `tls.secretName` value:

```
--tls-cert=/etc/bigfleet/tls/tls.crt
--tls-key=/etc/bigfleet/tls/tls.key
--tls-ca=/etc/bigfleet/tls/ca.crt
```

All three set = mTLS (servers require and verify client certs; clients verify servers — both against the same CA bundle). None = plaintext. A partial set is a startup error. One flag set covers every edge of a process: the shard's flags apply to its Session server, its coordinator dial, and its provider dial.

### URI SAN conventions

Identity is a URI SAN on the certificate — **exactly one `bigfleet://` URI per certificate**:

| Component | Required URI SAN | Also needs |
|---|---|---|
| Cluster operator | `bigfleet://cluster/<cluster_id>` | — |
| Shard | `bigfleet://shard/<shard_id>` | DNS SAN for its per-pod headless-Service name |
| Coordinator | `bigfleet://admin` | DNS SANs for the `bigfleet-coordinator` Service names |
| `bigfleetctl` | `bigfleet://admin` | — |

The shard terminates a Session whose certificate doesn't carry the asserted `cluster_id` with `PermissionDenied` and increments `bigfleet_shard_session_identity_rejected_total` — alert on any non-zero rate. The coordinator applies the same binding to `ReportShard.shard_id` and gates the whole admin surface (`AssignDomain`, `UnassignDomain`, `RemoveShard`, `ListShards`, `ListDomainAssignments`, `ListQuotas`, `JoinRaftCluster`, `SnapshotSave`) on `bigfleet://admin`.

### Issuing certificates with cert-manager

The charts never generate certificates; they mount existing `kubernetes.io/tls` Secrets (`tls.crt` / `tls.key` / `ca.crt` — exactly what a cert-manager `Certificate` produces). One private CA for the whole BigFleet trust domain is the expected shape:

```yaml
# Management cluster: coordinator certificate (shared by all replicas).
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: bigfleet-coordinator-tls
  namespace: bigfleet-system
spec:
  secretName: bigfleet-coordinator-tls
  issuerRef: {name: bigfleet-ca, kind: ClusterIssuer}
  uris:
    - bigfleet://admin
  dnsNames:
    - bigfleet-coordinator.bigfleet-system.svc
    - "*.bigfleet-coordinator.bigfleet-system.svc"
---
# Management cluster: shard certificate. The URI SAN embeds the
# shard_id, so issue one Certificate per shard ordinal; the reference
# chart's single tls.secretName fits replicas=1 (per-ordinal Secret
# overlays are your composition for multi-shard installs).
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: bigfleet-shard-0-tls
  namespace: bigfleet-system
spec:
  secretName: bigfleet-shard-0-tls
  issuerRef: {name: bigfleet-ca, kind: ClusterIssuer}
  uris:
    - bigfleet://shard/bigfleet-shard-0
  dnsNames:
    - bigfleet-shard-0.bigfleet-shard-headless.bigfleet-system.svc
---
# Each managed cluster: operator client certificate. The URI must
# match the chart's clusterID value exactly.
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: bigfleet-operator-tls
  namespace: bigfleet-system
spec:
  secretName: bigfleet-operator-tls
  issuerRef: {name: bigfleet-ca, kind: ClusterIssuer}
  uris:
    - bigfleet://cluster/cluster-prod-eu
```

Then point the charts at the Secrets:

```sh
helm upgrade bigfleet ./deploy/helm/bigfleet \
  --set coordinator.tls.secretName=bigfleet-coordinator-tls \
  --set shard.tls.secretName=bigfleet-shard-0-tls
helm upgrade bigfleet-operator ./deploy/helm/bigfleet-operator \
  --set tls.secretName=bigfleet-operator-tls ...
```

`bigfleetctl` against an mTLS coordinator needs the admin cert files (`--tls-cert/--tls-key/--tls-ca`); running it as a Job that mounts the coordinator's Secret is the simplest pattern.

### Rotation

Leaf certificates rotate **live**: every TLS handshake stats the cert/key files and re-reads them when an mtime changes, which is exactly what happens when cert-manager renews a mounted Secret. No restart, no reconnect storm; a half-written rotation keeps serving the previous coherent pair until both files agree. The **CA bundle is read once at startup** — rotate the CA by trust-bundle overlap (append the new CA to the bundle, restart, roll all leaf certs, remove the old CA, restart).

### What mTLS does not cover

The Raft transport between coordinator replicas (`:7791`) stays plaintext — hashicorp/raft's TCP transport is a separate stream from the gRPC stack and securing it is tracked follow-up work (ADR-0048 has the rationale). Keep the Raft port cluster-internal with a NetworkPolicy. Metrics/pprof endpoints are also plaintext HTTP; under mTLS the coordinator's kubelet probes move to HTTP twins on the metrics port automatically (kubelet's gRPC probe cannot present a client certificate).

## Day-2 observability

Each binary exposes Prometheus metrics on a configurable port:

| Binary | Default port | Path |
|---|---|---|
| Coordinator | `:8790` | `/metrics` |
| Shard | `:8780` | `/metrics` |
| Operator | `:8770` | `/metrics` |
| `bigfleet-unschedulable-pod-controller` | `:8080` | `/metrics` |

### Key metrics

**Health**
- `bigfleet_coordinator_raft_term` — increases on leader elections. Rapidly increasing = network partition or stepdown loop.
- `bigfleet_coordinator_apply_total{outcome=...}` — Apply outcomes. Spike in `error` or `fsm_error` = state-machine issue.
- `bigfleet_shard_cycle_duration_seconds` — histogram. p95 should stay below ~50 ms; p99 below 100 ms at 5K-machine inventory.
- `bigfleet_operator_session_reconnects_total` — should be near zero in steady state. Bursts = shard unhealthy or network blip.

**Throughput**
- `bigfleet_shard_actions_total{kind=...}` — Bootstrap / Provision / Reclaim / Preempt counts. Sustained high Preempt = priority-inversion churn; investigate the workloads' priorities.
- `bigfleet_operator_acknowledged_total` — CRs transitioning Pending → Acknowledged. Should track the rate of unschedulable-pod arrivals.
- `bigfleet_shard_inventory_machines{state=...}` — current machine counts by state. Stable in steady state; spikes through transitional states (Creating / Configuring / Draining / Deleting) during scale events.

**Pressure**
- `bigfleet_shard_shortfalls` — unresolved demand the shard couldn't satisfy locally. Persistent non-zero = under-provisioned fleet *or* over-aggressive workload priorities.
- `bigfleet_coordinator_pending_instructions{shard=...}` — coordinator-issued instructions awaiting ack. Should drain to zero between rebalance cycles.

### Suggested dashboard layout

Two panels per group:

1. **Coordinator**: Raft term (single-stat), Apply rate by outcome (timeseries), pending instructions per shard (timeseries).
2. **Shard**: cycle duration p50/p95/p99 (heatmap), action rate by kind (timeseries), inventory by state (stacked timeseries), shortfalls (single-stat with alert).
3. **Operator (per cluster)**: rollup duration p50/p95 (heatmap), CR acknowledgement rate (timeseries), session reconnects (single-stat with alert).

## Runbook

| Alert | What's happening | What to do |
|---|---|---|
| Shard `up==0` | The shard process is down | Restart. Existing pods/CRs in managed clusters are unaffected (static stability). New provisioning pauses until the shard returns. |
| Coordinator leader stepdown | Network blip or replica restart. New leader within ~1s normally. | Investigate if it loops. Check `bigfleet_coordinator_raft_term` rate. |
| Sustained `bigfleet_shard_shortfalls > 0` | Demand exceeds available capacity in the shard's slice | (a) Check the donor-shard summaries: is there free capacity elsewhere? Cross-shard rebalance should be active. (b) If fleet-wide saturated, procurement needs to add capacity. |
| `bigfleet_operator_session_reconnects_total` rising | Shard ↔ operator network unhealthy | Check shard health; check kubelet/CNI on the cluster running the operator. Operators reconnect with backoff automatically. |
| `bigfleet_shard_cycle_duration_seconds` p99 > 1s | Hot path overloaded | (a) Is the inventory huge? Check `bigfleet_shard_inventory_machines`. (b) Provider RPCs slow? Check provider's own metrics. (c) Consider sharding more aggressively. |

## Disaster recovery (coordinator state)

Static stability is the first thing to know: **shards and managed clusters keep running with the coordinator entirely down**, through the whole loss-and-restore procedure below. The data plane makes provisioning decisions autonomously; what pauses is coordinator-mediated work — new shard registrations, new cluster→shard bindings, domain assignments, cross-shard rebalancing instructions. There is no provisioning fire drill; do the restore calmly.

### Backups

Two mechanisms, use both:

- **Continuous export (recommended baseline).** Run the coordinator with `--snapshot-export-dir` pointed at a path mounted from durable object storage. The leader exports a snapshot every `--snapshot-export-interval` (default 5m) as a `<timestamp>-<id>/` directory containing `meta.json` + `state`, with a `latest` symlink. Coordinator state is small; 5 minutes is cheap. Your exposure window is this interval.
- **On-demand save.** `bigfleetctl --coordinator=<addr> snapshot save backup.snap` streams the leader's freshest snapshot to a single file. Take one before every chart upgrade and before any planned management-cluster maintenance.

### Total-loss recovery

When all coordinator replicas (or their PVCs) are gone:

```sh
# 0. Scale the coordinator StatefulSet to zero. Restore is OFFLINE —
#    it rewrites a stopped coordinator's data dir.
kubectl -n bigfleet-system scale statefulset bigfleet-coordinator --replicas=0

# 1. Restore the newest snapshot into ordinal 0's (recreated) PVC.
#    bigfleetctl ships in the bigfleet image; run it from a pod that
#    mounts the PVC at /var/lib/bigfleet. The archive can be a
#    `snapshot save` file or an exported snapshot directory (use
#    <export-dir>/latest).
bigfleetctl snapshot restore \
  --data-dir=/var/lib/bigfleet \
  --node-id=bigfleet-coordinator-0 \
  --raft-advertise=bigfleet-coordinator-0.bigfleet-coordinator.bigfleet-system.svc:7791 \
  backup.snap

# 2. Make sure ordinals 1 and 2 have EMPTY data dirs (delete their
#    PVCs). They must re-join the restored cluster, not vote with
#    stale state.

# 3. Scale back to 3. Ordinal 0 starts with the restored snapshot and
#    elects itself (the restore writes a single-voter configuration —
#    hashicorp/raft installs the membership recorded in the snapshot's
#    meta, the same single-survivor shape as hashicorp's peers.json
#    recovery); ordinals 1 and 2 join via ADR-0047 and the quorum
#    re-forms by itself.
kubectl -n bigfleet-system scale statefulset bigfleet-coordinator --replicas=3
```

### What a restore loses

Everything committed between the snapshot and the failure — bounded by the export interval (or the age of your last `snapshot save`):

- **Cluster→shard bindings made since the snapshot.** These re-create themselves: clusters are bound on first contact, so the next roll-up from an unbound cluster re-binds it. The consequence to expect: the re-bind may land on a *different* shard than the lost binding, in which case machines the original shard held for that cluster are no longer attributed to it and get reclaimed/re-provisioned rather than recognised. Transient over-provisioning, not an outage.
- **Shard registrations and domain assignments since the snapshot.** Shards re-register on their next heartbeat (~10s). Domain assignments made via `bigfleetctl assign-domain` since the snapshot must be re-applied by hand — check your change log.
- **Nothing on the data plane.** Running workloads, operator sessions, shard inventories, and in-flight provisioning are untouched (shard state lives on the shards).

One non-restore failure worth knowing (ADR-0047): if ordinal 0 *alone* loses its PVC while 1 and 2 keep theirs, do **not** restore — the survivors still have quorum and the live state. Wipe ordinal 0's data dir and let it re-join as an empty replica.

## Security

- **Operator → shard**: enable ADR-0048 mTLS (see "Transport security" above). The shard verifies the client certificate's `bigfleet://cluster/<cluster_id>` URI SAN against `Hello.cluster_id` — the impersonation check is built in, not a deployment add-on.
- **Shard → coordinator**: the same mTLS layer binds `ShardReport.shard_id` to `bigfleet://shard/<shard_id>` and gates the admin surface on `bigfleet://admin`. A network-policy fence inside the management cluster is still worthwhile (it's the only protection the plaintext Raft port has).
- **Shard → provider**: the shard presents its `bigfleet://shard/<shard_id>` certificate when mTLS is on; verifying it is the provider's job (the provider boundary is the validation point, ADR-0005). Cloud providers typically also use cloud-IAM for the underlying API; if the provider runs process-local on the shard host, a Unix socket or localhost-only TCP is reasonable.

### Supply chain

Images pushed from main are cosign-signed (keyless, GitHub OIDC) and carry a BuildKit SPDX SBOM attestation. Verify before deploying:

```sh
cosign verify ghcr.io/intunderflow/bigfleet:main \
  --certificate-identity-regexp='^https://github.com/intUnderflow/bigfleet/\.github/workflows/images\.yml@.*$' \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com
```

Same command for `bigfleet-operator` and `bigfleet-unschedulable-pod-controller`. Inspect the SBOM with `docker buildx imagetools inspect <ref> --format '{{ json .SBOM }}'`.

## Upgrades

### CRD upgrades
v1alpha1 is in flux until v1beta1. Use `kubectl apply` for in-place CRD upgrades; the operator informers re-list on schema bumps. Never delete and recreate a CRD with existing CapacityRequest objects (you'll lose state).

### Coordinator upgrades
Helm upgrade is rolling — one replica at a time, paced by the readiness probe (a restarted replica reports ready once it observes a Raft leader again). The Raft cluster tolerates one replica down out of three; leader election handles stepdown automatically, and the PodDisruptionBudget (`minAvailable: 2` at 3 replicas) keeps node drains from taking quorum. `coordinator.bootstrap=true` is safe to leave set (ADR-0047): only ordinal 0 honours it, and only on an empty data dir.

### Shard upgrades
Rolling. Each shard's existing assignments are persisted by the coordinator (cluster → shard, domain → shard); a fresh shard process re-reads them on startup and re-establishes provider connections.

### Operator upgrades
Rolling. The Shard.Session stream is reconnect-safe; in-flight bootstrap requests are reissued by the shard on the new connection.

## Cross-references

- Architecture: [BigFleet paper](https://lucy.sh/bigfleet) (vendored at `docs/papers/bigfleet.md`)
- Operating model: [Fleet-Scale Kubernetes paper](https://lucy.sh/fleet-scale-kubernetes) (vendored at `docs/papers/fleet-scale-kubernetes.md`)
- Implementation plan: `docs/plan.md`
- Provider authoring: `docs/provider-author-guide.md`
- Scaling sizing: `docs/scaling-guide.md`
