SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

MODULE  := github.com/intUnderflow/bigfleet
BIN     := $(CURDIR)/bin
GO      := go
GOFLAGS :=

# Pinned tool versions. Bump deliberately.
BUF_VERSION                  := v1.50.0
PROTOC_GEN_GO_VERSION        := v1.36.6
PROTOC_GEN_GO_GRPC_VERSION   := v1.5.1
GOLANGCI_LINT_VERSION        := v1.64.5
CONTROLLER_GEN_VERSION       := v0.17.2

export PATH := $(BIN):$(PATH)

##@ Help

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Tools

.PHONY: tools
tools: $(BIN)/buf $(BIN)/protoc-gen-go $(BIN)/protoc-gen-go-grpc $(BIN)/golangci-lint $(BIN)/controller-gen ## Install all developer tools into ./bin.

$(BIN):
	@mkdir -p $(BIN)

$(BIN)/buf: | $(BIN)
	GOBIN=$(BIN) $(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

$(BIN)/protoc-gen-go: | $(BIN)
	GOBIN=$(BIN) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

$(BIN)/protoc-gen-go-grpc: | $(BIN)
	GOBIN=$(BIN) $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

$(BIN)/golangci-lint: | $(BIN)
	GOBIN=$(BIN) $(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(BIN)/controller-gen: | $(BIN)
	GOBIN=$(BIN) $(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

##@ Build

.PHONY: generate
generate: tools ## Regenerate proto + CRD code.
	$(BIN)/buf generate
	$(BIN)/controller-gen \
		object:headerFile="hack/boilerplate.go.txt" \
		paths="./pkg/apis/..."
	$(BIN)/controller-gen \
		crd \
		paths="./pkg/apis/..." \
		output:crd:artifacts:config=api/crd

.PHONY: build
build: ## Compile all binaries.
	$(GO) build ./...

##@ Test

.PHONY: test
test: ## Run unit tests with the race detector.
	$(GO) test -race -count=1 ./...

.PHONY: test-cover
test-cover: ## Run tests with coverage.
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

.PHONY: integration
integration: ## Run integration tests.
	$(GO) test -race -count=1 -tags=integration ./test/integration/...

.PHONY: e2e
e2e: ## Run kind-based end-to-end tests. Requires kind, kubectl, and a running Docker daemon.
	$(GO) test -count=1 -tags=e2e -timeout=30m -v ./test/e2e/...

.PHONY: sim
sim: ## Run the simulator scenario suite + verify recorded goldens.
	$(GO) test -race -count=1 ./sim/...
	$(GO) build -o $(BIN)/fauxctl ./cmd/fauxctl
	@for s in $$(ls sim/golden/*.jsonl 2>/dev/null | xargs -n1 basename | sed 's/\.jsonl//'); do \
		$(BIN)/fauxctl verify $$s || exit 1; \
	done

.PHONY: scale
scale: ## Run scale ceiling tests (slow; tagged "scale"). Designed for the M5 Max + Docker Desktop budget; not part of PR CI by default.
	$(GO) test -count=1 -tags=scale -timeout=30m ./test/scale/...

.PHONY: soak
soak: ## Run the simulator soak test (tagged "soak"). Long; nightly CI only.
	$(GO) test -count=1 -tags=soak -timeout=10m ./sim/...

.PHONY: conformance
conformance: ## Run the provider conformance suite (TARGET=addr:port).
	@if [ -z "$$TARGET" ]; then echo "TARGET=addr:port required"; exit 1; fi
	$(GO) test -count=1 -tags=conformance -run . ./test/conformance/... -target=$$TARGET

.PHONY: conformance-self
conformance-self: ## Run the conformance suite against pkg/provider/fake (no TARGET needed).
	$(GO) test -count=1 -tags=conformance -run TestConformance_SelfTest_OnFake ./test/conformance/...

##@ Lint

.PHONY: lint
lint: tools ## Run linters.
	$(BIN)/golangci-lint run ./...
	$(BIN)/buf lint

.PHONY: helm-render
helm-render: ## Render all three Helm charts to /dev/null as a smoke check.
	@command -v helm >/dev/null || { echo "helm not on PATH"; exit 1; }
	helm template bigfleet deploy/helm/bigfleet --namespace bigfleet-system >/dev/null
	helm template bf-op deploy/helm/bigfleet-operator --namespace bigfleet-system --set clusterID=test --set shardAddress=bigfleet-shard:7780 >/dev/null
	helm template bf-cr deploy/helm/bigfleet-unschedulable-pod-controller --namespace bigfleet-system >/dev/null
	@echo "all three charts rendered"

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet ./...

##@ Verify

.PHONY: verify
verify: vet lint test ## What CI runs on every PR.

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(BIN) dist coverage.out coverage.html
