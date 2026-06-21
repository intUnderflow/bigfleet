# Load this receipt in Grafana — one command

This bundle's `tsdb-snapshot.tar.gz ([download](https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-uber50k-20260621/tsdb-uber-50k-cee793e.tar.gz))` is a full Prometheus TSDB: the **hub** shard/coordinator
Prometheus + a **representative one-satellite-per-region sample** (one per region (5 regions)) of the 40-satellite, 5,000,000-Pod uber-50k fleet. (The published canonical
`2026-06-18-uber-50k-cee793e` committed only its hub `blocks` dir, 86M; this bundle additionally
carries 5 regional satellite TSDBs — it *exceeds* the published committed evidence. The full
40-satellite TSDB is ~6GB and is not committed; the fleet-aggregate operator gates in `summary.json`
are measured live, summed-by-le across all 40 satellites, exactly as the published run measured them.)

To open it and explore every metric:

```bash
# 1. unpack the TSDB next to this file
mkdir -p data && tar -xzf tsdb-snapshot.tar.gz -C /tmp/_recv && \
  find /tmp/_recv -mindepth 2 -maxdepth 2 -type d -exec cp -r {} data/ \;   # flatten all hub+sat blocks into one TSDB dir

# 2. bring up Prometheus (serving the snapshot) + Grafana (pre-provisioned with the dashboard)
docker compose up -d

# 3. open Grafana
open http://localhost:3000          # anonymous, Admin; dashboard: "BigFleet uber-50k receipt"
# (Prometheus UI is also at http://localhost:9090 for raw PromQL)
```

`docker compose down -v` when done.

## What you're looking at
- **Combined TSDB**: every block from the hub shard/coordinator Prometheus + the 5 regional
  satellite Prometheis, loaded into one instance. Series are disambiguated by their `instance`
  (ephemeral pod IP) + `pod`/`component` labels — see `tsdb-label-enumeration.txt` for the
  complete distinct label-value set (no host/region/employee identifiers — only metric names,
  k8s object names, and ephemeral 10.42/100.64 pod IPs).
- **Dashboard** (`grafana-dashboard.json`): the ADR-0054 capacity-delivery gates
  (configure-phase p99, shard-cycle p99, bootstrap success ratio, node-state/rollup/ack p99),
  inventory-by-state, active sessions — the same signals the run was gated on (50k profile:
  configure ≤15s, cycle ≤25s, bootstrap ≥0.99, node-state ≤1.5s, shortfalls ==0, bind p50 ≤10s info).
- Raw PromQL: e.g. `histogram_quantile(0.99, sum by(le)(rate(bigfleet_shard_cycle_duration_seconds_bucket[5m])))`.

## Provenance
See `config/` for the rendered values, image digests, git SHA, and fleet geometry of the run
this snapshot came from (all-cee793e: chart `git checkout cee793e` + images `bigfleet:cee793e` /
`bigfleet-scaletest:cee793e`). Logs (`logs/`, scrubbed) and time-series CSVs (`csv/`) accompany.
