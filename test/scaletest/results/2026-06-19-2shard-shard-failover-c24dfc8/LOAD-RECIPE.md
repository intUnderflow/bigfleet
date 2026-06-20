# Load this receipt in Grafana — one command

This bundle's `tsdb-snapshot.tar.gz ([download](https://github.com/intUnderflow/bigfleet-uber/releases/download/failover-shard-kill-2shard-receipt-20260620/tsdb-snapshot.tar.gz))` is a full Prometheus TSDB (the combined hub+satellite
snapshot of a genuine BigFleet uber-5k run). To open it and explore every metric:

```bash
# 1. unpack the TSDB next to this file
mkdir -p data && tar -xzf tsdb-snapshot.tar.gz -C /tmp/_recv && \
  find /tmp/_recv -mindepth 2 -maxdepth 2 -type d -exec cp -r {} data/ \;   # flatten all hub+sat blocks into one TSDB dir

# 2. bring up Prometheus (serving the snapshot) + Grafana (pre-provisioned with the dashboard)
docker compose up -d

# 3. open Grafana
open http://localhost:3000          # anonymous, Admin; dashboard: "BigFleet uber-5k receipt"
# (Prometheus UI is also at http://localhost:9090 for raw PromQL)
```

`docker compose down -v` when done.

## What you're looking at
- **Combined TSDB**: every block from the hub shard/coordinator Prometheus + all satellite
  Prometheis, loaded into one instance. Series are disambiguated by their `instance`
  (ephemeral pod IP) + `pod`/`component` labels — see `tsdb-label-enumeration.txt` for the
  complete distinct label-value set (657 values, human-eyeballed; no host/region/employee
  identifiers — only metric names, k8s object names, and ephemeral 10.42/100.64 pod IPs).
- **Dashboard** (`grafana-dashboard.json`): the ADR-0054 capacity-delivery gates
  (configure-phase p99, shard-cycle p99, bootstrap success ratio, node-state/rollup/ack p99),
  inventory-by-state, active sessions, and Phase-3 reclaim — the same signals the run was
  gated on.
- Raw PromQL: e.g. `histogram_quantile(0.99, sum by(le)(rate(bigfleet_shard_cycle_duration_seconds_bucket[5m])))`.

## Provenance
See `config/` for the rendered values, image digests, git SHA, and fleet geometry of the run
this snapshot came from. Logs (`logs/`, scrubbed) and time-series CSVs (`csv/`) accompany.
