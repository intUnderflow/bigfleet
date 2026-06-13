package decision

import (
	"time"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// Idle-hold defaults — the paper §8 release rule, as named constants:
// "Idle → Speculative lazily per provider (bare metal: forever;
// on-demand: minutes; spot: ~1m)". Constants, not configuration —
// the ADR-0042-Addendum posture: tunables stay constants until
// evidence demands otherwise (ADR-0049).
const (
	// DefaultOnDemandIdleHold pins §8's "minutes" for on-demand at 10
	// minutes. The trade is asymmetric and cheap on both sides: a
	// wrong release costs one Create plus bootstrap latency — cloud
	// on-demand re-buys in single-digit minutes and charges nothing
	// for the Create itself, so the only real loss is provisioning
	// delay for demand that returns just after the release; a wrong
	// hold costs the machine's hourly price for the hold window
	// (~1/6 machine-hour at 10m). Ten minutes therefore rides out the
	// common short demand dips (rolling restarts, redeploys — the same
	// timescale ReclaimGrace already budgets for the drain ahead of
	// this hold) without ever holding a full billing hour of pure
	// waste.
	DefaultOnDemandIdleHold = 10 * time.Minute

	// DefaultSpotIdleHold is §8's "~1m", verbatim. Idle spot is paid-
	// for capacity the provider can interrupt anyway, so a long hold
	// buys an unreliable hedge; and workloads priced onto spot
	// tolerate provisioning latency by construction, so the re-buy
	// cost of a wrong release is the smallest in the fleet.
	DefaultSpotIdleHold = 1 * time.Minute
)

// ReleasePolicy is the per-CapacityType idle-hold policy for the paper
// §8 Idle → Speculative release (M73): how long an Idle machine of
// each elastic capacity type may sit unclaimed before Phase 3 emits a
// Delete for it.
//
// Fixed capacity (bare metal, reserved) is structurally absent: §8
// holds bare metal forever, and a reserved instance's commitment is
// paid whether the machine exists or not (paper §4 — fixed capacity's
// marginal cost is zero), so releasing it saves nothing. The policy
// cannot express releasing them, which is the safety property — no
// configuration mistake can delete owned hardware. Unspecified
// capacity types are likewise held forever: never delete capacity
// whose cost class is unknown.
//
// A zero hold disables release for that type (the repo's "zero value
// = historical behaviour" convention), so the zero ReleasePolicy
// releases nothing. Production uses DefaultReleasePolicy; the sim
// passes short holds so cycle-scale tests can cross hold expiry —
// there is deliberately no flag or chart value for this.
type ReleasePolicy struct {
	OnDemandHold time.Duration
	SpotHold     time.Duration
}

// DefaultReleasePolicy returns the paper-§8 constants.
func DefaultReleasePolicy() ReleasePolicy {
	return ReleasePolicy{
		OnDemandHold: DefaultOnDemandIdleHold,
		SpotHold:     DefaultSpotIdleHold,
	}
}

// Hold returns the idle-hold duration for the capacity type and
// whether the type is releasable at all under this policy.
func (p ReleasePolicy) Hold(ct machine.CapacityType) (time.Duration, bool) {
	switch ct {
	case machine.CapacityTypeOnDemand:
		return p.OnDemandHold, p.OnDemandHold > 0
	case machine.CapacityTypeSpot:
		return p.SpotHold, p.SpotHold > 0
	default:
		// BareMetal, Reserved, Unspecified: hold forever (see type doc).
		return 0, false
	}
}
