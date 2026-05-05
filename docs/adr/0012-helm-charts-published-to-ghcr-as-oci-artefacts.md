# ADR-0012: Helm charts are published to GHCR as OCI artefacts on every push to main

**Status**: Accepted

**Date**: 2026-05-05

## Context

Pre-M26, the helm charts (`bigfleet`, `bigfleet-operator`, `bigfleet-unschedulable-pod-controller`) lived in `deploy/helm/` and were never published anywhere. The user-stories install commands implicitly assumed a git checkout — a contract that's fine for a reference implementation but breaks the "real users adopting BigFleet" story. The image-publishing pipeline already pushed `:main` and `:sha-<short>` tags to GHCR; charts had no equivalent.

Two distribution candidates:

1. **OCI artefacts via GHCR.** `helm push` to `oci://ghcr.io/intunderflow/charts/<chart>`. Modern helm 3.8+ pattern; mirrors the existing image-publishing flow. Tag is the chart version from `Chart.yaml`.
2. **Static `index.yaml` on a gh-pages branch via chart-releaser.** Older but more compatible style. Works with `helm repo add bigfleet https://intunderflow.github.io/bigfleet`.

Option 1 piggy-backs on the existing GHCR auth (`secrets.GITHUB_TOKEN` from the same repo as the package owner). Option 2 needs a separate gh-pages branch and a public Pages site, plus chart-releaser's GitHub Actions integration.

## Decision

Charts publish to **OCI artefacts on GHCR** via a new `.github/workflows/charts.yml` that mirrors `images.yml`'s shape:

- **on push to main** → `helm package` + `helm push` to `oci://ghcr.io/<owner>/charts/<chart>`. Tag is the `Chart.yaml` `version` field. Pushes are immutable; re-pushing the same version is rejected by GHCR.
- **on pull_request** → `helm lint` + `helm package` + `helm template --kube-version=1.31.0` (the floor from ADR-0010). No push, no GHCR auth, no write-token exposure on PR builds.

Install commands switch to the OCI form:

```sh
helm install bigfleet-operator oci://ghcr.io/intunderflow/charts/bigfleet-operator \
  --version 0.1.0 \
  --namespace bigfleet-system --create-namespace …
```

The git-checkout form (`./deploy/helm/bigfleet-operator`) is retained as a one-line equivalence note in the operator-guide for development / air-gapped use.

## Consequences

- **Real users can install without a clone.** The operator-install user story is now end-to-end: `helm install bigfleet-operator oci://ghcr.io/intunderflow/charts/bigfleet-operator --version <V> …` works against a fresh laptop with helm 3.8+ installed.
- **Chart version is the canonical release identifier.** Bump `Chart.yaml`'s `version` field on every shippable change; that's the OCI tag end users pin against. No floating tags — there's no "latest" / "main" tag for charts the way there is for images, by design (immutable artefacts).
- **First-push-per-chart may need GHCR Manage-Actions-access flip.** Helm push to GHCR sometimes lands as a private package by default; visibility / repo-linkage settings may need a one-time UI flip per chart. The first run from this commit linked all three packages to the repo automatically — but if a future fork hits permission_denied: write_package, the fix is the same flow as the image-publishing first-push.
- **No `latest` floating tag.** Helm OCI doesn't have docker's tag-rewrite ergonomic. If we want a "moving latest" pointer, a separate workflow would push the same chart twice with different tags. v1 doesn't ship that — explicit version pinning is the model.
- **PR builds catch chart-version-related kubeVersion bumps fast.** The `helm template --kube-version=1.31.0` step in the PR pipeline fails the check if any chart accidentally requires a newer API surface than the declared floor (ADR-0010).
