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

.PHONY: bench-hot
bench-hot: ## Run the hot-path benchmarks at measured uber-5k cardinality (~1 min). Pre-brief gate: a regression here is a starved shard in the cloud — see the #52-class ParseQuantity incident.
	$(GO) test -run xxx -bench 'Phase1_Uber5K_CoLocated|Phase3_Uber5K_CoLocated|AcquirableTotals_Uber5KShape|BuildRollup_CoLocated25K' \
		-benchtime=5x -count=1 ./pkg/decision/... ./pkg/operator/

.PHONY: prevalidate
prevalidate: ## The pre-brief gate: closed-loop sim + hot-path benches + dev-50 on kind (~8 min warm). Every SHA bound for a cloud brief runs this first.
	@docker info >/dev/null 2>&1 || { echo "prevalidate: Docker daemon not running — start Docker Desktop first"; exit 1; }
	@echo "[$$(date +%T)] rung 1/4: closed-loop sim"
	$(GO) test -count=1 -run ClosedLoop ./sim/...
	@echo "[$$(date +%T)] rung 2/4: hot-path benches"
	$(MAKE) bench-hot
	@echo "[$$(date +%T)] rung 3/4: images + kind"
	$(MAKE) scaletest-images
	@command -v kind >/dev/null || { echo "kind not on PATH"; exit 1; }
	@kind get clusters | grep -q '^bigfleet-prevalidate$$' || kind create cluster --name bigfleet-prevalidate
	@# Skip the load when the node already has these exact image IDs —
	@# the stamp lives ON the node so cluster recreation invalidates it.
	@IDS=$$( (docker images -q bigfleet:dev; docker images -q bigfleet-scaletest:dev) | tr '\n' ' ' ); \
	NODE=bigfleet-prevalidate-control-plane; \
	LOADED=$$(docker exec $$NODE cat /etc/bigfleet-loaded-ids 2>/dev/null || true); \
	if [ "$$IDS" != "$$LOADED" ]; then \
	  kind load docker-image bigfleet:dev bigfleet-scaletest:dev --name bigfleet-prevalidate && \
	  docker exec $$NODE sh -c "echo $$IDS > /etc/bigfleet-loaded-ids"; \
	else \
	  echo "kind load skipped — image IDs unchanged"; \
	fi
	@echo "[$$(date +%T)] rung 4/4: dev-50 on kind"
	$(MAKE) scaletest PROFILE=dev-50 DURATION=3m
	@echo "[$$(date +%T)] prevalidate green — SHA is brief-ready"

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
helm-render: ## Render all Helm charts to /dev/null as a smoke check.
	@command -v helm >/dev/null || { echo "helm not on PATH"; exit 1; }
	helm template bigfleet deploy/helm/bigfleet --namespace bigfleet-system >/dev/null
	helm template bf-op deploy/helm/bigfleet-operator --namespace bigfleet-system --set clusterID=test --set shardAddress=bigfleet-shard:7780 >/dev/null
	helm template bf-cr deploy/helm/bigfleet-unschedulable-pod-controller --namespace bigfleet-system >/dev/null
	helm template scaletest test/scaletest/chart -f test/scaletest/profiles/dev-500.yaml >/dev/null
	@echo "all charts rendered"

.PHONY: scaletest-images
scaletest-images: ## Build the two images the scaletest harness needs (bigfleet, bigfleet-scaletest). PLATFORM= overrides host detection (e.g. PLATFORM=linux/amd64 for cloud-arch validation).
	@command -v docker >/dev/null || { echo "docker not on PATH"; exit 1; }
	@# Detect host arch so kind on Apple Silicon doesn't end up running x86_64
	@# binaries through Rosetta emulation (~2× slowdown). The Dockerfile defaults
	@# GOARCH to amd64 when TARGETARCH is unset; passing --platform sets TARGETARCH
	@# so the Go build matches the host. CI sets PLATFORM=linux/amd64 explicitly
	@# for cloud-portable images.
	@ARCH=$$(echo "$(or $(PLATFORM),linux/$$(uname -m))" | sed -e 's|linux/||' -e 's/x86_64/amd64/' -e 's/aarch64/arm64/'); \
	echo "Building bigfleet images for $$ARCH (--platform alone doesn't set TARGETARCH; --build-arg does)"; \
	docker build --platform=linux/$$ARCH --build-arg TARGETARCH=$$ARCH -t bigfleet:dev -f cmd/bigfleet/Dockerfile . && \
	docker build --platform=linux/$$ARCH --build-arg TARGETARCH=$$ARCH -t bigfleet-scaletest:dev -f test/scaletest/image/Dockerfile .

.PHONY: scaletest
scaletest: ## Run the dev-500 profile end-to-end. Override with PROFILE=scaleway-500k etc. DURATION=Xm overrides the profile's soak window (default: loadProfile.durationSeconds).
	@mkdir -p test/scaletest/results
	$(GO) run ./test/scaletest/cmd/scaletest-runner \
		--profile=test/scaletest/profiles/$(or $(PROFILE),dev-500).yaml \
		$(if $(DURATION),--duration=$(DURATION),) \
		--output=test/scaletest/results/$$(date +%Y%m%d-%H%M%S)-$(or $(PROFILE),dev-500)/

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet ./...

##@ Verify

.PHONY: verify
verify: vet lint test ## What CI runs on every PR.

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(BIN) dist coverage.out coverage.html

##@ Git hooks

.PHONY: install-hooks
install-hooks: ## Point this clone's git hooks at .githooks/ (pre-commit lint + pre-push verify).
	@chmod +x .githooks/pre-commit .githooks/pre-push
	@git config core.hooksPath .githooks
	@echo "git hooks installed (core.hooksPath -> .githooks)"
	@echo "  pre-commit: make lint"
	@echo "  pre-push:   make verify"
	@echo "Bypass either run with --no-verify if you need to."

.PHONY: uninstall-hooks
uninstall-hooks: ## Restore default git hook handling (.git/hooks/) for this clone.
	@git config --unset core.hooksPath || true
	@echo "git hooks uninstalled (core.hooksPath cleared; .git/hooks/ is back in charge)"
