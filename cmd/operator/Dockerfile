# syntax=docker/dockerfile:1.7
#
# Image for the per-cluster bigfleet-operator (M75 — the chart
# referenced this image long before CI built it). Build from the repo
# root:
#
#   docker build -t bigfleet-operator:dev -f cmd/operator/Dockerfile .

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
      -o /out/bigfleet-operator ./cmd/operator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/bigfleet-operator /usr/local/bin/bigfleet-operator
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/bigfleet-operator"]
