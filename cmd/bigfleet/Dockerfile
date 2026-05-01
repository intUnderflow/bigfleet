# syntax=docker/dockerfile:1.7
#
# Production-ish image for the cmd/bigfleet binary (runs the
# coordinator, shard, or all-in-one subcommands). Used by the scaletest
# harness and as the basis for real deployments. Build from the repo
# root:
#
#   docker build -t bigfleet:dev -f cmd/bigfleet/Dockerfile .

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
      -o /out/bigfleet ./cmd/bigfleet

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/bigfleet /usr/local/bin/bigfleet
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/bigfleet"]
