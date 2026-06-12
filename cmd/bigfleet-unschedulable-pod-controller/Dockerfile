# syntax=docker/dockerfile:1.7
#
# Image for the optional bigfleet-unschedulable-pod-controller (M75 —
# the chart referenced this image long before CI built it). Build from
# the repo root:
#
#   docker build -t bigfleet-unschedulable-pod-controller:dev \
#     -f cmd/bigfleet-unschedulable-pod-controller/Dockerfile .

ARG GO_VERSION=1.26
ARG TARGETARCH

FROM golang:${GO_VERSION}-bookworm AS builder
WORKDIR /src

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
      -o /out/bigfleet-unschedulable-pod-controller ./cmd/bigfleet-unschedulable-pod-controller

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/bigfleet-unschedulable-pod-controller /usr/local/bin/bigfleet-unschedulable-pod-controller
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/bigfleet-unschedulable-pod-controller"]
