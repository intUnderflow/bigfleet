package occ

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// SeedConfiguredSupply runs the priority-sorted credit-existing-
// supply pre-pass before the OCC worker pool starts. For each Need
// in priority-descending order, it walks the matching Configured /
// Configuring machines in the Need's cluster and claims them via
// state.SeedClaim until either the cluster's matching supply
// exhausts or the Need's AggregateResources are covered.
//
// ADR-0045: this walk is the engine's ONLY supply attribution.
// Phase 1 reads the residual deficits to size fulfillment; Phase 3
// reads the final claimed-set and reclaims the unclaimed Configured
// remainder as excess (bound − demand). One implementation, two
// consumers — the phases cannot disagree, which is why the walk
// iterates SortedClusterStateBucket (keep-priority order: price asc,
// reclamation_penalty desc, ID asc): the remainder it leaves
// unclaimed is then exactly the paper-§8 release-order tail.
//
// Same-Profiles are domain-aware (ADR-0040): the domain is chosen
// once per Need, jointly over creditable supply (the cluster's
// Configured/Configuring) and acquirable supply (shard-wide Idle +
// Speculative), credit is confined to it, and the choice is recorded
// on state so acquisition (FindSame) is confined to it too — see
// seedSameProfile. Crediting across domains made the pre-pass see
// scattered supply as satisfied while FindSame (strict) saw the same
// Needs as unsatisfiable; choosing the domain twice (creditable-only
// here, best-Idle in FindSame) assembled cross-domain groups Phase 3
// then reclaimed — both halves of the Bootstrap↔Reclaim equilibrium
// the ADR and its Addendum document.
//
// Sequential by design: priority ordering preserves bigfleet.md §16
// (priority is the sole throttling mechanism). Mirrors the pre-OCC
// phase1Allocator.creditExistingSupply method but writes to the OCC
// SharedState directly. The seeded claims participate in normal
// displacement during the OCC pass.
//
// retryBudget is forwarded to each seeded claim — if a worker
// later displaces a seeded claim, the displaced Need re-enters
// the queue with retryBudget-1.
//
// Returns *needs.Need pointers in the same order needsByIdx
// presented; element i corresponds to needsByIdx[i]. Each Need's
// "residual deficit after pre-pass" is encoded in the returned
// NeedResult.Deficit. Callers that need stable Need pointers must
// guarantee needsByIdx doesn't shift the underlying storage after
// SeedConfiguredSupply returns.
func SeedConfiguredSupply(state *SharedState, needsByIdx []*needs.Need, retryBudget int) []NeedResult {
	// Sort an index slice (not the Needs themselves) so the caller's
	// pointers remain stable. Priority-descending walk.
	order := make([]int, len(needsByIdx))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return needsByIdx[order[a]].Profile.Priority() > needsByIdx[order[b]].Profile.Priority()
	})

	results := make([]NeedResult, len(needsByIdx))
	snap := state.Snapshot()
	acquirable := NewSameSupplyIndex(snap)

	// ADR-0041 rider, hoisted here from Phase 3's deleted mirror walk
	// (ADR-0045): the per-pass virtual-consumption ledger for acquirable
	// machines. When a Same Need's chosen domain falls short of its
	// creditable supply, the workers will fill the residual from that
	// domain's Idle/Speculative — so the domain's acquirable members are
	// consumed here and a later Need ranks against what is actually
	// left. Without it, the moment idle Same-capacity appears every gang
	// ranks the same fresh domain best; the losers assemble nothing and
	// Phase 3 reclaims their healthy serving machines as unclaimed.
	consumed := make(map[machine.ID]struct{})

	for _, idx := range order {
		n := needsByIdx[idx]
		profile := n.Profile
		prec := PrecedenceFromProfile(profile)
		remaining := n.AggregateResources

		if sameKey, ok := SameRequirementKey(profile); ok {
			remaining = seedSameProfile(state, snap, acquirable, consumed, n, prec, sameKey, retryBudget)
		} else {
			for _, st := range []machine.State{machine.StateConfigured, machine.StateConfiguring} {
				if needs.IsZero(remaining) {
					break
				}
				for _, m := range snap.SortedClusterStateBucket(n.ClusterID, st) {
					if needs.IsZero(remaining) {
						break
					}
					if state.IsClaimed(m.ID) {
						continue
					}
					if !matchProfile(profile, m) {
						continue
					}
					alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
					if !needs.Covers(alloc, n.MinUnit) {
						continue
					}
					state.SeedClaim(m.ID, n, prec, retryBudget)
					remaining = needs.SubResources(remaining, alloc)
				}
			}
		}

		results[idx] = NeedResult{
			Need:    n,
			Deficit: remaining,
		}
	}
	return results
}

// seedSameProfile is SeedConfiguredSupply's Same-Profile arm
// (ADR-0040 + Addendum): collect the matching, unclaimed,
// MinUnit-covering Configured + Configuring machines of the Need's
// cluster, bucket them by the machine's value for the Same key, add
// each domain's acquirable potential (shard-wide unclaimed Idle +
// Speculative from the per-fingerprint index — Idle has no cluster
// binding), and choose the single best bucket via ChooseSameBucket
// over those JOINT totals. The choice is recorded on state so
// findCandidatesFor confines acquisition to the same domain, and
// SeedClaim credits only the creditable members within it —
// Configured before Configuring, the same preference order as the
// non-Same walk. Returns the residual demand vector.
//
// Ranking creditable-only here while FindSame re-picked the best
// Idle bucket chose the domain twice per cycle: Phase 1 assembled
// cross-domain groups, Phase 3 (correctly strict) reclaimed the
// off-domain half next cycle, and it re-bootstrapped scattered — a
// reclaim↔re-bootstrap oscillation at cycle rate.
// seedCandidate is one creditable machine in a Same-domain bucket:
// the ID for the claim and the alloc the claim loop subtracts.
type seedCandidate struct {
	id    machine.ID
	alloc []needs.ResourceQty
}

func seedSameProfile(state *SharedState, snap *inventory.Snapshot, acquirable *SameSupplyIndex, consumed map[machine.ID]struct{}, n *needs.Need, prec Precedence, sameKey string, retryBudget int) []needs.ResourceQty {
	remaining := n.AggregateResources
	// Bucket totals are the index's integer vectors (ParseVec) so the
	// per-Need walk does no quantity parsing; candidates keep their
	// []ResourceQty alloc for the claim loop's SubResources boundary.
	minUnitVec := acquirable.ParseVec(n.MinUnit)
	index := make(map[string]int)
	var buckets []SameBucket
	var members [][]seedCandidate
	for _, st := range []machine.State{machine.StateConfigured, machine.StateConfiguring} {
		// Keep-priority order within the domain, like the non-Same walk:
		// ADR-0045 makes the unclaimed remainder Phase 3's excess, so the
		// claim preference doubles as the §8 release order.
		for _, m := range snap.SortedClusterStateBucket(n.ClusterID, st) {
			if state.IsClaimed(m.ID) {
				continue
			}
			if !matchProfile(n.Profile, m) {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			vec := acquirable.ParseVec(alloc)
			if !VecCovers(vec, minUnitVec) {
				continue
			}
			v, ok := lookupAttribute(sameKey, m)
			if !ok {
				continue
			}
			i, exists := index[v]
			if !exists {
				i = len(buckets)
				index[v] = i
				buckets = append(buckets, SameBucket{Value: v})
				members = append(members, nil)
			}
			buckets[i].Count++
			buckets[i].CreditableCount++
			buckets[i].Total = VecAdd(buckets[i].Total, vec)
			buckets[i].CreditableTotal = VecAdd(buckets[i].CreditableTotal, vec)
			members[i] = append(members[i], seedCandidate{id: m.ID, alloc: alloc})
		}
	}
	// Joint potential: fold in the acquirable half, consumption-aware
	// (ADR-0041 rider). Domains with only acquirable supply get an
	// empty members list — choosing one credits nothing and leaves
	// acquisition (confined there) to fill the deficit.
	//
	// ADR-0042 Addendum: a parked Need folds creditable-only — with no
	// acquirable bucket to rank, the incumbent domain wins trivially
	// and the Need cannot flip domains or acquire; it keeps its
	// concentrated partial assembly and ages in the shortfall buffer.
	// Its demand still counts: the creditable claims below keep its
	// serving machines out of Phase 3's excess (ADR-0045).
	if !n.AcquisitionParked {
		unavailable := func(id machine.ID) bool {
			if _, ok := consumed[id]; ok {
				return true
			}
			return state.IsClaimed(id)
		}
		foldAcquirable(acquirable, unavailable, n, sameKey, index, &buckets, &members)
	}
	deficitVec := acquirable.ParseVec(remaining)
	best := ChooseSameBucket(buckets, deficitVec)
	if best < 0 {
		return remaining
	}
	state.recordSameDomain(n, buckets[best].Value)
	state.recordSameSatisfiable(n, VecCovers(buckets[best].Total, deficitVec))
	for _, c := range members[best] {
		if needs.IsZero(remaining) {
			break
		}
		state.SeedClaim(c.id, n, prec, retryBudget)
		remaining = needs.SubResources(remaining, c.alloc)
	}
	// ADR-0041 rider: the creditable members didn't fully cover the
	// Need — the workers will fill the residual by acquiring the chosen
	// domain's Idle/Speculative, so consume them from later Needs'
	// joint view. A parked Need acquires nothing, so it consumes
	// nothing (ADR-0042 Addendum).
	if !n.AcquisitionParked && !needs.IsZero(remaining) {
		acquirable.ConsumeAcquirable(n.Profile, sameKey, buckets[best].Value, n.MinUnit, remaining, consumed)
	}
	return remaining
}

// foldAcquirable merges the shard-wide acquirable half (Idle +
// Speculative not claimed and not virtually consumed) into the
// per-domain buckets, mirroring the creditable fold above. Split out
// so the ADR-0042 parked path can skip it.
func foldAcquirable(acquirable *SameSupplyIndex, unavailable func(machine.ID) bool, n *needs.Need, sameKey string, index map[string]int, buckets *[]SameBucket, members *[][]seedCandidate) {
	for v, ab := range acquirable.AcquirableTotals(n.Profile, sameKey, n.MinUnit, unavailable) {
		i, exists := index[v]
		if !exists {
			i = len(*buckets)
			index[v] = i
			*buckets = append(*buckets, SameBucket{Value: v})
			*members = append(*members, nil)
		}
		(*buckets)[i].Count += ab.Count
		(*buckets)[i].Total = VecAdd((*buckets)[i].Total, ab.Total)
	}
}
