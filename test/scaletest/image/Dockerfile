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

# kube-apiserver (single static binary).
RUN ARCH="${TARGETARCH:-amd64}" && \
    curl -fsSL "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kube-apiserver" \
      -o /usr/local/bin/kube-apiserver && \
    chmod +x /usr/local/bin/kube-apiserver

# kube-controller-manager — needed for the garbage-collector controller
# that cascade-deletes ownerRef-owned objects (CRs owned by Pods that
# get churned). Without it, kwok cluster apiservers accumulate orphan
# CRs from churn-deleted Pods and the operator's rollup carries stale
# demand. Surfaced in scaleway-50k Drop F: 2810 CRs vs 1000 live Pods,
# Phase 3 found nothing to reclaim, chain fell into Phase 2 thrash.
RUN ARCH="${TARGETARCH:-amd64}" && \
    curl -fsSL "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kube-controller-manager" \
      -o /usr/local/bin/kube-controller-manager && \
    chmod +x /usr/local/bin/kube-controller-manager

# kube-scheduler — ADR-0023: optional, gated by KWOK_SCHEDULER=kube-scheduler
# in the entrypoint. When enabled, replaces pod-shim's Pod-binding role
# with the real Kubernetes scheduler so the binding-latency numbers we
# publish reflect what a production fleet would see.
RUN ARCH="${TARGETARCH:-amd64}" && \
    curl -fsSL "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kube-scheduler" \
      -o /usr/local/bin/kube-scheduler && \
    chmod +x /usr/local/bin/kube-scheduler

# kubectl, used by entrypoint to apply CRDs and probe readiness.
RUN ARCH="${TARGETARCH:-amd64}" && \
    curl -fsSL "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kubectl" \
      -o /usr/local/bin/kubectl && \
    chmod +x /usr/local/bin/kubectl

# kine (sqlite-backed etcd shim). The release artefact is unconditionally
# linux-only, named `kine-${ARCH}` (no OS infix).
RUN ARCH="${TARGETARCH:-amd64}" && \
    curl -fsSL "https://github.com/k3s-io/kine/releases/download/${KINE_VERSION}/kine-${ARCH}" \
      -o /usr/local/bin/kine && \
    chmod +x /usr/local/bin/kine

# etcd — M45.5: optional storage backend for kwok apiserver, swapped in
# via KINE_BACKEND=etcd in the entrypoint. kine-sqlite serialises writes
# through one WAL fsync stream; etcd writes in parallel with a smaller
# tail. Worth the ~15 MB image bump for laptop scale tests where the
# kine WAL was the dominant CPU consumer.
ARG ETCD_VERSION
RUN ARCH="${TARGETARCH:-amd64}" && \
    curl -fsSL "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-${ARCH}.tar.gz" \
      | tar -xzC /tmp && \
    mv "/tmp/etcd-${ETCD_VERSION}-linux-${ARCH}/etcd" /usr/local/bin/etcd && \
    chmod +x /usr/local/bin/etcd && \
    rm -rf "/tmp/etcd-${ETCD_VERSION}-linux-${ARCH}"

# kwok controller.
RUN ARCH="${TARGETARCH:-amd64}" && \
    curl -fsSL "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok-linux-${ARCH}" \
      -o /usr/local/bin/kwok && \
    chmod +x /usr/local/bin/kwok

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
