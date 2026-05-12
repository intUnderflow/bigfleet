#!/bin/bash
# entrypoint-apiserver.sh — runs the apiserver-side stack of the
# scaletest harness in its own container, isolated from the operator
# and load-driver via separate cgroup CPU budgets.
#
# What this container runs:
#   1. kine                — sqlite-backed etcd shim (localhost:2379)
#   2. kube-apiserver      — talks to kine, serves on localhost:6443
#   3. kwok-controller     — fakes node lifecycle in the apiserver
#
# Coordination with the workload container (operator + load-driver):
#   * Both containers share an emptyDir at /var/scaletest.
#   * This script writes the kubeconfig + CA + admin certs there and
#     then touches /var/scaletest/apiserver-ready once readyz passes.
#   * The workload container waits on that file before starting.
#
# Pre-M-reshape this script + entrypoint-workload.sh ran in one
# process tree under entrypoint.sh; that bundled all five components
# under a single 1-core CPU limit and the apiserver+kine starved the
# workload (and vice versa) on every run. Splitting into two
# containers gives each side its own CPU budget.

set -euo pipefail

: "${POD_NAME:?POD_NAME required}"

WORK="${WORK:-/var/scaletest}"
mkdir -p "$WORK"/{certs,kine,manifests,logs}

log() { echo "[$(date -u +%H:%M:%S) $1] ${*:2}"; }

# ---- 1. mint a self-signed CA + serving cert + admin client cert ----
log entrypoint "minting certs"
cat > "$WORK/openssl.cnf" <<EOF
[req]
distinguished_name = req
[v3_ca]
basicConstraints = CA:TRUE
keyUsage = critical, digitalSignature, keyCertSign
[v3_server]
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt
[v3_client]
keyUsage = critical, digitalSignature
extendedKeyUsage = clientAuth
[alt]
DNS.1 = localhost
DNS.2 = $POD_NAME
IP.1  = 127.0.0.1
EOF

(
  cd "$WORK/certs"
  openssl genrsa -out ca.key 2048 >/dev/null 2>&1
  openssl req -x509 -new -key ca.key -days 365 -subj "/CN=scaletest-ca" \
    -extensions v3_ca -config "$WORK/openssl.cnf" -out ca.crt >/dev/null 2>&1

  openssl genrsa -out apiserver.key 2048 >/dev/null 2>&1
  openssl req -new -key apiserver.key -subj "/CN=kube-apiserver" \
    -out apiserver.csr >/dev/null 2>&1
  openssl x509 -req -in apiserver.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 365 -extensions v3_server -extfile "$WORK/openssl.cnf" \
    -out apiserver.crt >/dev/null 2>&1

  openssl genrsa -out admin.key 2048 >/dev/null 2>&1
  openssl req -new -key admin.key -subj "/CN=admin/O=system:masters" \
    -out admin.csr >/dev/null 2>&1
  openssl x509 -req -in admin.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 365 -extensions v3_client -extfile "$WORK/openssl.cnf" \
    -out admin.crt >/dev/null 2>&1

  openssl genrsa -out sa.key 2048 >/dev/null 2>&1
  openssl rsa -in sa.key -pubout -out sa.pub >/dev/null 2>&1
)

# ---- 2. write an admin kubeconfig pointing at localhost:6443 ----
KCFG="$WORK/admin.kubeconfig"
cat > "$KCFG" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: local
  cluster:
    server: https://127.0.0.1:6443
    certificate-authority: $WORK/certs/ca.crt
users:
- name: admin
  user:
    client-certificate: $WORK/certs/admin.crt
    client-key: $WORK/certs/admin.key
contexts:
- name: local
  context:
    cluster: local
    user: admin
current-context: local
EOF
export KUBECONFIG="$KCFG"

# ---- 3. start storage backend (kine sqlite or etcd) ----
# M45.5: KWOK_STORAGE selects the apiserver's backing store.
# - kine (default): kine + sqlite. Lightweight, no separate process to
#   lifecycle, but sqlite WAL fsyncs serialise writes inside one
#   connection. Under the 5-cluster-apiserver-writes-per-Pod chain
#   load, this was the dominant CPU consumer (kine at 729 % fleet on
#   dev-500) and the bind-tail latency cause.
# - etcd: real etcd. Parallel writes via BoltDB MVCC. ~15 MB binary
#   added to the image; kwok apiserver already speaks etcd v3.
#   Recommended for throughput-heavy laptop scaletests.
: "${KWOK_STORAGE:=kine}"
if [[ "$KWOK_STORAGE" == "etcd" ]]; then
  mkdir -p "$WORK/etcd"
  log etcd "starting"
  etcd --name=node \
       --data-dir="$WORK/etcd" \
       --listen-client-urls=http://127.0.0.1:2379 \
       --advertise-client-urls=http://127.0.0.1:2379 \
       --listen-peer-urls=http://127.0.0.1:2380 \
       --initial-cluster=node=http://127.0.0.1:2380 \
       --initial-advertise-peer-urls=http://127.0.0.1:2380 \
       --initial-cluster-state=new \
       --logger=zap --log-level=warn \
       --unsafe-no-fsync=true \
       >"$WORK/logs/etcd.log" 2>&1 &
  KINE_PID=$!
else
  # KINE_SQLITE_PARAMS is overridable for kine perf tuning.
  # Default _sync=OFF skips the per-WAL-commit fsync.
  # Override to KINE_SQLITE_PARAMS='_journal=WAL&cache=shared' to
  # restore default fsync behaviour for crash-safety testing.
  : "${KINE_SQLITE_PARAMS:=_journal=WAL&cache=shared&_sync=OFF}"
  log kine "starting (params=$KINE_SQLITE_PARAMS)"
  kine --listen-address=tcp://127.0.0.1:2379 \
       --endpoint="sqlite://$WORK/kine/state.db?$KINE_SQLITE_PARAMS" \
       >"$WORK/logs/kine.log" 2>&1 &
  KINE_PID=$!
fi

for i in {1..30}; do
  if (echo > /dev/tcp/127.0.0.1/2379) 2>/dev/null; then break; fi
  sleep 0.5
done

# ---- 4. start kube-apiserver ----
log apiserver "starting"
kube-apiserver \
  --etcd-servers=http://127.0.0.1:2379 \
  --secure-port=6443 \
  --bind-address=127.0.0.1 \
  --advertise-address=127.0.0.1 \
  --tls-cert-file="$WORK/certs/apiserver.crt" \
  --tls-private-key-file="$WORK/certs/apiserver.key" \
  --client-ca-file="$WORK/certs/ca.crt" \
  --service-account-key-file="$WORK/certs/sa.pub" \
  --service-account-signing-key-file="$WORK/certs/sa.key" \
  --service-account-issuer=https://localhost:6443 \
  --authorization-mode=AlwaysAllow \
  --service-cluster-ip-range=10.0.0.0/24 \
  --allow-privileged=true \
  --disable-admission-plugins=ServiceAccount,Priority \
  --feature-gates=WatchList=true \
  --max-requests-inflight=2000 \
  --max-mutating-requests-inflight=1000 \
  >"$WORK/logs/apiserver.log" 2>&1 &
APISERVER_PID=$!

for i in {1..120}; do
  if kubectl --kubeconfig="$KCFG" get --raw=/readyz >/dev/null 2>&1; then break; fi
  sleep 0.5
done
log apiserver "ready"

# ---- 5. apply CRDs ----
log crds "applying"
kubectl --kubeconfig="$KCFG" apply -f /etc/bigfleet/crd/ >/dev/null

# ---- 6. start kwok-controller ----
log kwok "starting"
kwok \
  --kubeconfig="$KCFG" \
  --manage-all-nodes=true \
  --cidr=10.244.0.0/16 \
  --node-ip=10.244.0.1 \
  >"$WORK/logs/kwok.log" 2>&1 &
KWOK_PID=$!

# ---- 7. start kube-controller-manager (garbage-collector only) ----
#
# Stock Kubernetes runs many controllers (replication, deployment, GC,
# TTL, etc.). For the harness we only need garbage-collector — it's the
# controller that walks ownerRef chains and cascade-deletes owned
# objects when their owner is deleted. UPC creates each CR with the
# owning Pod as its sole ownerRef; without GC the CRs accumulate as
# orphans on every churn cycle.
#
# --controllers=garbage-collector,namespace disables every other
# controller, so we don't pay CPU for replicaset / endpoint / etc.
# work the harness has no use for. (namespace stays on so the empty
# default namespace continues to exist.)
log kcm "starting (garbage-collector only)"
kube-controller-manager \
  --kubeconfig="$KCFG" \
  --authentication-kubeconfig="$KCFG" \
  --authorization-kubeconfig="$KCFG" \
  --leader-elect=false \
  --controllers=garbage-collector-controller,namespace-controller \
  --root-ca-file="$WORK/certs/ca.crt" \
  --service-account-private-key-file="$WORK/certs/sa.key" \
  --use-service-account-credentials=false \
  --bind-address=127.0.0.1 \
  --secure-port=10257 \
  --concurrent-gc-syncs=20 \
  >"$WORK/logs/kcm.log" 2>&1 &
KCM_PID=$!

# Signal the workload container that the apiserver is ready and the
# kubeconfig is written. The workload script waits on this file.
touch "$WORK/apiserver-ready"
log entrypoint "apiserver-side ready; signalled workload container"

# ---- 8. supervise: if any process dies, take the container down ----
trap 'kill $KINE_PID $APISERVER_PID $KWOK_PID $KCM_PID 2>/dev/null || true' SIGTERM SIGINT

while true; do
  for name in KINE APISERVER KWOK KCM; do
    pidvar="${name}_PID"
    if ! kill -0 "${!pidvar}" 2>/dev/null; then
      log entrypoint "$name (pid ${!pidvar}) exited; tailing logs and exiting"
      tail -n 50 "$WORK/logs/$(echo "$name" | tr '[:upper:]' '[:lower:]').log" || true
      exit 1
    fi
  done
  sleep 2
done
