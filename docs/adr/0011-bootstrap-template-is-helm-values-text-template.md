# ADR-0011: BootstrapTemplate is a helm-values text/template, not a CRD or admission webhook

**Status**: Accepted

**Date**: 2026-05-05

## Context

`Operator.Config.BootstrapTemplate` has always been a Go callback the operator's `handleBootstrapRequest` calls to render the kubelet bootstrap blob (userdata) per BootstrapRequest. Pre-M21 the only way to customise it was to fork the operator binary — embedders had to ship their own `cmd/operator` with their own `BootstrapTemplate` injected. That contradicts the per-cluster operator-install user story where a cluster owner runs `helm install bigfleet-operator …` and expects to declare userdata generation via chart values.

Three candidate shapes for surfacing the template:

1. **Helm values, text/template string.** `bootstrapTemplate: |\n  #cloud-config\n  …\n  {{ .ClusterID }}` rendered as a ConfigMap, mounted into the pod, parsed at startup.
2. **CRD-based template object.** `BootstrapTemplate` resource in `bigfleet.lucy.sh`, watched by the operator. Per-cluster overrides via per-cluster-namespace CRs.
3. **Admission webhook.** External webhook intercepts BootstrapRequest at the operator boundary and returns userdata. Maximum flexibility but heaviest dependency.

The platform team's bootstrap blob is a per-deployment artefact (one cluster ≈ one render). Per-cluster overrides through a CRD adds a real-time dependency on the cluster's apiserver during the BootstrapRequest hot path — re-introducing a kube-apiserver coupling on a path that was a single in-process function call. Webhooks add a second runtime BigFleet-must-talk-to. Helm values are static, file-mounted, parsed once.

## Decision

The operator's BootstrapTemplate is configured via a `bootstrapTemplate` helm values block — a multi-line string interpreted as a Go `text/template`. The chart renders it into a ConfigMap and mounts it at `/etc/bigfleet/bootstrap.tmpl`. The operator binary's `--bootstrap-template-file` flag points at the mounted file; the operator parses it at startup via `text/template.Parse`.

Template context is `BootstrapRendererInput`:

```go
{{ .ClusterID }}
{{ range .Requirements }}{{ .Key }} {{ .Operator }} {{ .Values }}{{ end }}
```

The Go callback (`Operator.Config.BootstrapTemplate`) is retained for embedders and tests — when both the file and the callback are set, the callback wins. When the file is set and the callback is nil, an internal callback wrapping the parsed template takes over. Empty file path falls through to the existing `stubBootstrapRenderer`.

## Consequences

- **No new CRD, no new webhook.** The platform team manages the template the same way they manage every other helm value.
- **Per-deployment, not per-cluster.** Two clusters needing different bootstrap templates run two operator chart releases. That matches the M-something cluster-to-shard binding model (ADR-0007) — one operator release per cluster.
- **Parse fails fast at startup.** A bad template doesn't ship to production silently — the operator binary refuses to start. The ConfigMap mount sequence means a broken template in helm values fails at `helm install` time, not at first BootstrapRequest.
- **Template execution failures surface as the BootstrapBlobResponse `Error` field.** The shard treats a non-empty Error as an unsatisfiable requirement and falls back to a shortfall — gating capacity instead of crashing the operator. This is the existing behaviour of the BootstrapRenderer contract; M21 just inherits it.
- **No Sprig.** `text/template` from the stdlib is the floor. Adding Sprig pulls a non-trivial dep tree onto every operator pod. Cluster owners that genuinely need Sprig idioms can write a wrapper in their bootstrap process; we don't pre-pay the dependency.
- **Embedder path retained.** Tests + in-process e2e harnesses (`cmd/fauxctl`, `pkg/operator` integration tests) wire a Go callback directly. They don't go through the file-template path.
