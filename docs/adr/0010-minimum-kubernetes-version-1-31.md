# ADR-0010: Minimum Kubernetes version 1.31

**Status**: Accepted

**Date**: 2026-05-05

## Context

BigFleet uses several Kubernetes API surfaces. Different surfaces stabilised in different versions:

| Feature | Used by | Stable since |
|---|---|---|
| CRD `subresource:status` | `bigfleet.lucy.sh/v1alpha1` resources | 1.16 |
| `policy/v1` Eviction | M20 ReclaimInstruction handler | 1.22 |
| `coordination.k8s.io/v1` Leases | controller-runtime managers | 1.14 |
| `NetworkPolicy` egress | M17.x partition runnerAction | 1.8 |
| `statefulset.kubernetes.io/pod-name` label | M17.x partition NetworkPolicy podSelector | 1.13 |
| **CRD `selectableFields`** | **M-just-now `kubectl --field-selector=status.phase=…`** | **1.31** |

The on-call user-stories runbook ships a `kubectl get capacityrequests --field-selector=status.phase=Pending` command. Without `selectableFields` declared on the CRD, that command fails. Pre-M-just-now the doc had a `jq` workaround; M-just-now added the marker to the CRD.

`selectableFields` went GA in Kubernetes 1.31. In 1.30 it was beta behind the `CustomResourceFieldSelectors` feature gate (off by default on most distributions). On 1.29 and earlier the apiserver rejects the `selectableFields` block at CRD apply time.

Two candidate floors:

1. **1.30 minimum, document the feature-gate dependency.** Requires every adopter to opt-in to a beta gate. Most managed-Kubernetes services don't expose the gate at all.
2. **1.31 minimum, document the floor.** Newer-than-some-deployments-have but matches the stability promise of every other API BigFleet uses.

## Decision

Minimum Kubernetes version: **1.31**.

All three helm charts (`bigfleet`, `bigfleet-operator`, `bigfleet-unschedulable-pod-controller`) declare `kubeVersion: ">= 1.31.0-0"`. `helm install` fails fast on older clusters instead of producing a half-applied broken state.

## Consequences

- **The runbook commands work as documented.** No "this command won't work on your cluster" footnotes.
- **Older clusters can't install the charts.** Operators on 1.30-or-earlier either upgrade or fork the chart to drop the `selectableFields` block. Forking is fine — it just means accepting the user-stories' jq workaround for the on-call query and not getting the field-selector ergonomics.
- **The K8s-version-support matrix in the operator-guide is the canonical place to look.** When future features tighten the floor (e.g., a Validating Admission Policy use-case GA'ing in a later version), the same matrix gets updated and the chart constraints follow.
- **K8s 1.31 was Aug 2024.** Most major distros (EKS / GKE / AKS, Scaleway Kapsule, OpenShift) ship it as a current supported version as of when this ADR is written; we are not pinning to bleeding-edge.
- **No back-compat shim to support older clusters.** The chart constraint is a hard fence; we don't carry conditional logic for "render the CRD without selectableFields if running on 1.30." That keeps the CRD schema honest and avoids two configurations to maintain.
