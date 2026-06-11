# syntax=docker/dockerfile:1.7
#
# scaletest harness image: one Pod = one self-contained KWOK cluster +
# the bigfleet-operator + a load-driver. Tested at v1.30 of K8s, v0.6 of
# kwok, v0.13 of kine.
#
# Build from the repo root so the COPY --from=builder picks up our
# go.mod and full source tree:
#   docker build -t scaletest:dev -f test/scaletest/image/Dockerfile .

ARG GO_VERSION=1.26
ARG K8S_VERSION=v1.30.0
ARG KWOK_VERSION=v0.7.0
ARG KINE_VERSION=v0.14.16
ARG ETCD_VERSION=v3.5.16
ARG TARGETARCH

# ---------------- Stage 1: build the Go components ----------------
FROM golang:${GO_VERSION}-bookworm AS builder
WORKDIR /src

# Cache go mod download separately from the source tree.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=linux GOARCH=${TARGETARCH:-amd64} CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" \
      -o /out/bigfleet-operator ./cmd/operator && \
    GOOS=linux GOARCH=${TARGETARCH:-amd64} CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" \
      -o /out/bigfleet-unschedulable-pod-controller ./cmd/bigfleet-unschedulable-pod-controller && \
    GOOS=linux GOARCH=${TARGETARCH:-amd64} CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" \
      -o /out/bigfleet-scaletest-pod-shim ./test/scaletest/cmd/pod-shim && \
    GOOS=linux GOARCH=${TARGETARCH:-amd64} CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" \
      -o /out/bigfleet-scaletest-node-creator ./test/scaletest/cmd/node-creator && \
    GOOS=linux GOARCH=${TARGETARCH:-amd64} CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" \
      -o /out/load-driver ./test/scaletest/cmd/load-driver

# ---------------- Stage 2: assemble the runtime image ----------------
FROM debian:bookworm-slim AS runtime

ARG K8S_VERSION
ARG KWOK_VERSION
ARG KINE_VERSION
ARG TARGETARCH

ENV DEBIAN_FRONTEND=noninteractive \
    KUBE_VERSION=${K8S_VERSION} \
    KWOK_VERSION=${KWOK_VERSION} \
    KINE_VERSION=${KINE_VERSION}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates curl openssl tini bash && \
    rm -rf /var/lib/apt/lists/*

# All seven third-party binaries, fetched in ONE layer, in PARALLEL,
# through a persistent BuildKit cache mount keyed on (name, version,
# arch). Two reasons this isn't seven tidy RUN layers:
#   - Serial downloads dominated cold builds: 337.6s for the four
#     dl.k8s.io binaries alone at ~1MB/s (2026-06-11 cold rebuild).
#     Parallel curls bound the wall time at the slowest single fetch.
#   - Cache mounts survive Docker-daemon restarts that wipe the layer
#     cache (that same rebuild re-curled everything while the go-mod
#     cache mount was intact), so a cold event re-fetches nothing it
#     has ever seen.
# Downloads land under a temp name and are renamed only on success, so
# a partial download can never poison the cache; the install step fails
# loudly if any fetch died.
#
# Why each binary is here:
#   kube-controller-manager — the garbage-collector controller that
#     cascade-deletes ownerRef-owned objects (CRs owned by churned
#     Pods). Without it kwok apiservers accumulate orphan CRs and the
#     rollup carries stale demand (scaleway-50k Drop F: 2810 CRs vs
#     1000 live Pods, Phase 2 thrash).
#   kube-scheduler — ADR-0023: gated by KWOK_SCHEDULER=kube-scheduler;
#     replaces pod-shim's binding role so published binding-latency
#     numbers reflect a production fleet.
#   kubectl — entrypoint CRD apply + readiness probes.
#   kine — sqlite-backed etcd shim (release artefact is linux-only,
#     named kine-${ARCH}, no OS infix).
#   etcd — M45.5: optional KINE_BACKEND=etcd; kine-sqlite serialises
#     writes through one WAL fsync stream, etcd writes in parallel.
#   kwok — the node/pod lifecycle controller.
ARG ETCD_VERSION
RUN --mount=type=cache,target=/dl \
    set -eu; ARCH="${TARGETARCH:-amd64}"; \
    fetch() { \
      [ -s "/dl/$1" ] || { curl -fsSL "$2" -o "/dl/$1.tmp" && mv "/dl/$1.tmp" "/dl/$1"; }; \
    }; \
    fetch "kube-apiserver-${KUBE_VERSION}-${ARCH}" "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kube-apiserver" & p1=$!; \
    fetch "kube-controller-manager-${KUBE_VERSION}-${ARCH}" "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kube-controller-manager" & p2=$!; \
    fetch "kube-scheduler-${KUBE_VERSION}-${ARCH}" "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kube-scheduler" & p3=$!; \
    fetch "kubectl-${KUBE_VERSION}-${ARCH}" "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kubectl" & p4=$!; \
    fetch "kine-${KINE_VERSION}-${ARCH}" "https://github.com/k3s-io/kine/releases/download/${KINE_VERSION}/kine-${ARCH}" & p5=$!; \
    fetch "etcd-${ETCD_VERSION}-${ARCH}.tar.gz" "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-${ARCH}.tar.gz" & p6=$!; \
    fetch "kwok-${KWOK_VERSION}-${ARCH}" "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok-linux-${ARCH}" & p7=$!; \
    wait $p1; wait $p2; wait $p3; wait $p4; wait $p5; wait $p6; wait $p7; \
    install -m 0755 "/dl/kube-apiserver-${KUBE_VERSION}-${ARCH}" /usr/local/bin/kube-apiserver; \
    install -m 0755 "/dl/kube-controller-manager-${KUBE_VERSION}-${ARCH}" /usr/local/bin/kube-controller-manager; \
    install -m 0755 "/dl/kube-scheduler-${KUBE_VERSION}-${ARCH}" /usr/local/bin/kube-scheduler; \
    install -m 0755 "/dl/kubectl-${KUBE_VERSION}-${ARCH}" /usr/local/bin/kubectl; \
    install -m 0755 "/dl/kine-${KINE_VERSION}-${ARCH}" /usr/local/bin/kine; \
    tar -xzOf "/dl/etcd-${ETCD_VERSION}-${ARCH}.tar.gz" "etcd-${ETCD_VERSION}-linux-${ARCH}/etcd" > /usr/local/bin/etcd; \
    chmod 0755 /usr/local/bin/etcd; \
    install -m 0755 "/dl/kwok-${KWOK_VERSION}-${ARCH}" /usr/local/bin/kwok

COPY --from=builder /out/bigfleet-operator /usr/local/bin/
COPY --from=builder /out/bigfleet-unschedulable-pod-controller /usr/local/bin/
COPY --from=builder /out/bigfleet-scaletest-pod-shim /usr/local/bin/
COPY --from=builder /out/bigfleet-scaletest-node-creator /usr/local/bin/
COPY --from=builder /out/load-driver /usr/local/bin/

# Bundle the CRDs the entrypoint applies into each KWOK cluster on startup.
COPY api/crd/ /etc/bigfleet/crd/

COPY test/scaletest/image/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY test/scaletest/image/entrypoint-apiserver.sh /usr/local/bin/entrypoint-apiserver.sh
COPY test/scaletest/image/entrypoint-workload.sh  /usr/local/bin/entrypoint-workload.sh
RUN chmod +x /usr/local/bin/entrypoint.sh \
            /usr/local/bin/entrypoint-apiserver.sh \
            /usr/local/bin/entrypoint-workload.sh

# Run as a fixed unprivileged UID so the chart can drop capabilities and
# the image is friendly to GKE Autopilot's PodSecurity baseline.
RUN groupadd -g 65532 scaletest && \
    useradd  -u 65532 -g 65532 -m -s /bin/bash scaletest
USER 65532
WORKDIR /home/scaletest

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
