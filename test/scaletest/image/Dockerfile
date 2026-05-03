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

# kwok controller.
RUN ARCH="${TARGETARCH:-amd64}" && \
    curl -fsSL "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok-linux-${ARCH}" \
      -o /usr/local/bin/kwok && \
    chmod +x /usr/local/bin/kwok

COPY --from=builder /out/bigfleet-operator /usr/local/bin/
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
