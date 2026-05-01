#!/bin/bash
# entrypoint.sh — bring up an isolated KWOK cluster inside one Pod, then
# start the BigFleet operator + load-driver against it.
#
# What runs here:
#   1. kine                — sqlite-backed etcd shim (localhost:2379)
#   2. kube-apiserver      — talks to kine, serves on localhost:6443
#   3. kwok-controller     — fakes node lifecycle in the apiserver
#   4. bigfleet-operator   — dials BIGFLEET_SHARD_ADDR with cluster ID
#   5. load-driver         — emits CapacityRequest churn per profile
#
# Each KWOK cluster is a self-contained replica unit. N pods deployed
# from this image = N independent simulated clusters dialling into the
# same BigFleet shard.

set -euo pipefail

: "${POD_NAME:?POD_NAME required}"
: "${BIGFLEET_SHARD_ADDR:?BIGFLEET_SHARD_ADDR required}"
: "${LOAD_PROFILE:?LOAD_PROFILE required (path to YAML)}"

# Cluster ID is derived from the pod name so it's stable across
# operator restarts within the same Pod and unique across replicas.
CLUSTER_ID="${POD_NAME}"

WORK="${WORK:-/tmp/scaletest}"
mkdir -p "$WORK"/{certs,kine,manifests,logs}

# ---- 1. mint a self-signed CA + serving cert + admin client cert ----
log()  { echo "[$(date -u +%H:%M:%S) $1] ${*:2}"; }

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

# ---- 3. start kine ----
log kine "starting"
kine --listen-address=tcp://127.0.0.1:2379 \
     --endpoint="sqlite://$WORK/kine/state.db?_journal=WAL&cache=shared" \
     >"$WORK/logs/kine.log" 2>&1 &
KINE_PID=$!

# Wait for kine to accept connections.
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
  --disable-admission-plugins=ServiceAccount \
  --feature-gates=WatchList=true \
  >"$WORK/logs/apiserver.log" 2>&1 &
APISERVER_PID=$!

# Wait for the apiserver to be ready.
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

# ---- 7. start bigfleet-operator ----
# Per-profile tunables come from chart values via env vars. Empty /
# unset values are fine — bigfleet-operator's flag defaults take over.
op_args=(
  --cluster-id="$CLUSTER_ID"
  --shard-addr="$BIGFLEET_SHARD_ADDR"
  --kubeconfig="$KCFG"
  --metrics-addr="0.0.0.0:8770"
)
[[ -n "${OPERATOR_QPS:-}"             ]] && op_args+=(--qps="$OPERATOR_QPS")
[[ -n "${OPERATOR_BURST:-}"           ]] && op_args+=(--burst="$OPERATOR_BURST")
[[ -n "${OPERATOR_ACK_CONCURRENCY:-}" ]] && op_args+=(--ack-concurrency="$OPERATOR_ACK_CONCURRENCY")
[[ -n "${OPERATOR_ROLLUP_INTERVAL:-}" && "$OPERATOR_ROLLUP_INTERVAL" != "0s" ]] && op_args+=(--rollup-interval="$OPERATOR_ROLLUP_INTERVAL")
log operator "starting (cluster=$CLUSTER_ID shard=$BIGFLEET_SHARD_ADDR qps=${OPERATOR_QPS:-default} ack=${OPERATOR_ACK_CONCURRENCY:-default})"
bigfleet-operator "${op_args[@]}" >"$WORK/logs/operator.log" 2>&1 &
OPERATOR_PID=$!

# ---- 8. start the load-driver ----
log loadgen "starting (profile=$LOAD_PROFILE)"
load-driver \
  --kubeconfig="$KCFG" \
  --cluster-id="$CLUSTER_ID" \
  --profile="$LOAD_PROFILE" \
  --metrics-addr="0.0.0.0:8771" \
  >"$WORK/logs/loadgen.log" 2>&1 &
LOADGEN_PID=$!

log entrypoint "all components up"

# ---- 9. supervise: if any process dies, take the pod down ----
trap 'kill $KINE_PID $APISERVER_PID $KWOK_PID $OPERATOR_PID $LOADGEN_PID 2>/dev/null || true' SIGTERM SIGINT

while true; do
  for name in KINE APISERVER KWOK OPERATOR LOADGEN; do
    pidvar="${name}_PID"
    if ! kill -0 "${!pidvar}" 2>/dev/null; then
      log entrypoint "$name (pid ${!pidvar}) exited; tailing logs and exiting"
      tail -n 50 "$WORK/logs/$(echo "$name" | tr '[:upper:]' '[:lower:]').log" || true
      exit 1
    fi
  done
  sleep 2
done
