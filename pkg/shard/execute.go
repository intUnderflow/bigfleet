package shard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	"github.com/intUnderflow/bigfleet/pkg/provider"
)

// errProvisionRacedToIdle marks the specific race where a Bootstrap
// (called from Provision after Provider.Create + Provider.Configure,
// or directly) finds the machine at Idle at post-Configure-transition
// time instead of Configuring. The provider's Configure has already
// committed; a parallel actor (reconcile observing the provider's
// view, a competing Reclaim, a stale-snapshot duplicate emission)
// has flipped Configuring → Idle in the window between the
// Idle→Configuring transition and the post-Configure transition.
//
// Recovery is natural: the machine is at Idle in the inventory,
// next cycle's Phase 1 will see it and re-claim. The wasted
// Configure call costs one provider round-trip per fire; we accept
// that in exchange for not racing the FSM further inside one
// execute call (which would risk a tight retry loop if the
// flip-back keeps recurring).
//
// Surfaced by bigfleet-uber #23: 17–26% of Provision actions hit
// this race at uber-5k under the realistic catalog. Tracked
// separately from real state-machine violations via the
// transition_raced_to_idle outcome label so alerting on
// transition_error stays meaningful.
var errProvisionRacedToIdle = errors.New("post-Configure transition: machine raced from Configuring to Idle")

// execute dispatches an Action to the appropriate handler. Each handler
// drives the machine through its state-machine transitions and the
// matching provider RPC. Errors are returned to the caller (the cycle)
// so they can be logged, but they do not abort the cycle: the next
// cycle re-derives actions from current state.
func (s *Shard) execute(ctx context.Context, a decision.Action) (err error) {
	// M44.4 Drop A diagnostic: classify the outcome of each action
	// execution. The pre-instrumentation cloud run found 71% of
	// Bootstrap actions emitted by Phase 1 don't translate to
	// Configured machines; without per-outcome attribution we can't
	// tell which return path is the silent drop.
	metrics.ShardExecuteInflight.Inc()
	defer metrics.ShardExecuteInflight.Dec()
	defer func() {
		outcome := classifyExecuteError(ctx, err)
		metrics.ShardActionExecuteOutcomes.WithLabelValues(a.Kind.String(), outcome).Inc()
		if outcome == outcomeFenced {
			// Paper §11 / M71: the provider rejected our fencing token —
			// a newer epoch of this shard_id has already contacted it.
			// This process is a zombie (stale restart, duplicate identity,
			// split brain); its view of the fleet cannot be trusted and
			// retrying is wrong. Error-level on purpose: this is an
			// incident page, not noise.
			s.log.Error("provider fenced this shard's mutation — zombie-shard incident; do not retry, investigate duplicate shard identity",
				"kind", a.Kind.String(), "machine", a.MachineID, "cluster", a.Cluster, "err", err)
		}
		// ADR-0046 addendum: every executed action lands in the
		// decision audit log with its classified outcome.
		// Suppressed / dry-run actions are recorded at the cycle's
		// suppression branch instead; they never reach execute.
		s.auditAction(&a, outcome)
	}()
	switch a.Kind {
	case decision.ActionKindBootstrap:
		return s.executeBootstrap(ctx, a)
	case decision.ActionKindProvision:
		return s.executeProvision(ctx, a)
	case decision.ActionKindReclaim, decision.ActionKindPreempt:
		return s.executeDrain(ctx, a)
	}
	return fmt.Errorf("unknown action kind: %s", a.Kind)
}

// handleBootstrapBlobErr decides whether a blob-fetch failure is an
// orchestration timeout (rollback to Idle for retry) or a real failure
// (Failed, terminal). M44.4 Drop B: pre-fix, every blob-fetch error
// transitioned to Failed — so a slow operator under burst permanently
// shrank the Idle pool one machine per cycle ctx timeout. Distinguishing
// ctx errors from genuine failures restores retry semantics. Safe
// because the blob fetch happens BEFORE provider.Configure — the
// provider hasn't been touched yet, so rollback doesn't risk leaking
// provider-side state.
func (s *Shard) handleBootstrapBlobErr(id machine.ID, err error, lastErrPrefix string) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		_ = s.applyTransition(id, machine.StateIdle, func(m *machine.Machine) {
			m.Cluster = ""
			m.LastError = ""
		})
		return
	}
	_ = s.applyTransition(id, machine.StateFailed, func(m *machine.Machine) {
		m.LastError = lastErrPrefix + err.Error()
	})
}

// outcomeFenced labels a mutation the provider rejected for a stale
// fencing token (paper §11) — the zombie-shard signal. Checked by
// execute()'s deferred logger, so it's a named constant.
const outcomeFenced = "fenced"

// classifyExecuteError maps the err return of execute() to one of a
// fixed set of outcome labels for the per-execute counter.
func classifyExecuteError(ctx context.Context, err error) string {
	if err == nil {
		return "success"
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "ctx_canceled"
	}
	// Before the message-string buckets: a fenced rejection also matches
	// the "provider.Create/Configure/Drain" substrings via formatErr, and
	// it must NOT land in the retryable-looking provider_error bucket.
	if errors.Is(err, provider.ErrFenced) {
		return outcomeFenced
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "no active operator session"):
		return "no_session"
	case errors.Is(err, errProvisionRacedToIdle):
		// Provision/Bootstrap race: the post-Configure transition saw
		// the machine at Idle instead of Configuring. The provider's
		// Configure already succeeded; next cycle re-claims the
		// (now-Idle) machine and re-drives. Distinct outcome label so
		// the rate is visible separately from real state-machine
		// violations. See executeBootstrap below for the detection
		// site and bigfleet-uber #23 for the diagnostic that
		// surfaced this race at 17–26% of Provision actions.
		return "transition_raced_to_idle"
	case strings.Contains(msg, "→ Configuring"),
		strings.Contains(msg, "→ Creating"),
		strings.Contains(msg, "→ Draining"),
		strings.Contains(msg, "post-Create transition"),
		strings.Contains(msg, "post-Configure transition"),
		strings.Contains(msg, "post-Drain transition"):
		return "transition_error"
	case strings.Contains(msg, "LocalBootstrap"),
		strings.Contains(msg, "requestBootstrap"):
		return "blob_error"
	case strings.Contains(msg, "provider.Configure"),
		strings.Contains(msg, "provider.Create"),
		strings.Contains(msg, "provider.Drain"),
		strings.Contains(msg, "provider.Delete"):
		return "provider_error"
	// Drop I diagnostics: split the catch-all "other" bucket so the
	// scaleway-50k 22/sec residual is attributed. The dominant
	// suspect is "expected Idle" — a machine the worker picked up
	// has already moved out of Idle by the time the worker reads
	// inventory (reconcileIncremental between snap and execute, or
	// a parallel worker on a Reclaim → Idle path that re-emitted
	// before the previous Bootstrap cleared pendingActions). Once
	// it's labelled separately we can attack the right thing.
	case strings.Contains(msg, "expected Idle"),
		strings.Contains(msg, "expected Speculative"),
		strings.Contains(msg, "expected Configured"):
		return "stale_state"
	case strings.Contains(msg, "inventory get"):
		return "inventory_miss"
	case strings.Contains(msg, "missing source profile"):
		return "missing_profile"
	}
	return "other"
}

// executeProvision turns a Speculative machine into a Configured one for
// the destination cluster. Walks Speculative → Creating → Idle via the
// provider's Create, then hands off to executeBootstrap.
func (s *Shard) executeProvision(ctx context.Context, a decision.Action) error {
	if a.SourceProfile == nil {
		return errors.New("provision: missing source profile")
	}
	cur, err := s.inv.Get(a.MachineID)
	if err != nil {
		return formatErr("provision: inventory get", err)
	}
	if cur.State != machine.StateSpeculative {
		return fmt.Errorf("provision: machine %s in state %s; expected Speculative", a.MachineID, cur.State)
	}
	// Speculative → Creating
	if err := s.applyTransition(a.MachineID, machine.StateCreating, nil); err != nil {
		return formatErr("provision: → Creating", err)
	}
	ack, err := s.cfg.Provider.Create(ctx, provider.CreateRequest{MachineID: a.MachineID})
	if err != nil {
		// Provider rejected the transition. Mark the machine Failed so
		// the next cycle can recover (or the operator can intervene).
		_ = s.applyTransition(a.MachineID, machine.StateFailed, func(m *machine.Machine) {
			m.LastError = "create: " + err.Error()
		})
		return formatErr("provision: provider.Create", err)
	}
	// Reflect the provider's view of the host into the inventory.
	created, mErr := conv.MachineFromProto(conv.MachineToProto(ack.Machine))
	_ = mErr // round-trip can't fail since we control the source
	// ADR-0046 addendum: the Create ack is where provider-declared
	// price / interruption_probability enter the inventory — and the
	// locked cost formula. A garbage ack is treated like a provider
	// error: Failed, loud, counted — never ingested. (Configure /
	// Drain acks merge no cost fields — since M72 their round-tripped
	// form does carry the cluster binding, but screening stays
	// Create-only because the records those paths produce still pass
	// inventory.Apply's Invariant.)
	if vErr := s.validateProviderMachine(&created); vErr != nil {
		_ = s.applyTransition(a.MachineID, machine.StateFailed, func(m *machine.Machine) {
			m.LastError = "create: ack rejected: " + vErr.Error()
		})
		return formatErr("provision: provider.Create ack rejected", vErr)
	}
	if err := s.applyTransition(a.MachineID, created.State, func(m *machine.Machine) {
		m.Host = created.Host
		m.Profile = created.Profile
		m.PricePerHour = created.PricePerHour
		m.InterruptionProbability = created.InterruptionProbability
	}); err != nil {
		return formatErr("provision: post-Create transition", err)
	}
	// Hand off to bootstrap if the provider got us all the way to Idle.
	if created.State == machine.StateIdle {
		return s.executeBootstrap(ctx, a)
	}
	return nil
}

// executeBootstrap configures an Idle machine for the destination cluster.
// Walks Idle → Configuring, requests a bootstrap blob from the cluster
// operator over the active session, calls provider.Configure, then walks
// Configuring → Configured and stamps the workload's priority and
// penalties for Phase 2 victim scoring.
func (s *Shard) executeBootstrap(ctx context.Context, a decision.Action) error {
	if a.SourceProfile == nil {
		return errors.New("bootstrap: missing source profile")
	}
	cur, err := s.inv.Get(a.MachineID)
	if err != nil {
		return formatErr("bootstrap: inventory get", err)
	}
	// Drop J: idempotent re-application. If the machine is already
	// Configured for the same cluster + fingerprint that this action
	// would set, the desired end state already holds — return success
	// without doing anything. Drop I diagnostics found 39/sec
	// "expected Idle" errors at 50 K-Pod cloud scale (29 % of all
	// Bootstrap dispatches); the machines are uniformly already-
	// Configured for the same cluster the action targets, meaning a
	// previous Bootstrap completed before this duplicate dispatch.
	// The duplicates leak through the pendingActions dedup gate via
	// snapshot-vs-live race windows: cycle N takes a snapshot just
	// before worker A finishes a Bootstrap on M; cycle N then emits
	// a fresh Bootstrap(M) under a different fingerprint demand;
	// worker B runs it and finds M Configured. The action is a no-op
	// because worker A's transition is the desired end state.
	//
	// Errors only when the existing Configured state is for a
	// *different* cluster/fingerprint — that's a real conflict the
	// state machine should refuse.
	if cur.State == machine.StateConfigured &&
		cur.Cluster == a.Cluster &&
		cur.AssignedNeedFingerprint == a.SourceProfile.Fingerprint() {
		return nil
	}
	if cur.State != machine.StateIdle {
		return fmt.Errorf("bootstrap: machine %s in state %s; expected Idle", a.MachineID, cur.State)
	}

	// Idle → Configuring (record the destination cluster + assigned
	// Need fingerprint early so Phase 1's deficit math counts this
	// machine as supply for its target Need *while it's in flight*.
	// Without this, Phase 1 sees an in-flight machine as "not yet
	// supply" and emits another Bootstrap on a fresh Idle machine
	// for the same Need next cycle. Drop G surfaced this: 318 K of
	// 522 K Bootstrap emits (61 %) were skipped at the dedup gate
	// because Phase 1 had over-emitted on duplicate fresh machines
	// for the same fingerprint.
	if err := s.applyTransition(a.MachineID, machine.StateConfiguring, func(m *machine.Machine) {
		m.Cluster = a.Cluster
		m.AssignedNeedFingerprint = a.SourceProfile.Fingerprint()
	}); err != nil {
		return formatErr("bootstrap: → Configuring", err)
	}
	// Drop R: per-machine configure-phase timer starts here. Observed
	// only on the success path (after the Configuring→Configured
	// transition); error paths bypass the observation so failures
	// don't pollute the latency histogram.
	configureStart := time.Now()

	// Pull a kubelet bootstrap blob. Either via the operator's
	// Shard.Session stream (production) or via the LocalBootstrap
	// hook (simulator / test).
	blobCtx, cancel := context.WithTimeout(ctx, s.cfg.BootstrapTimeout)
	defer cancel()
	var blob []byte
	if s.cfg.LocalBootstrap != nil {
		var err error
		blob, err = s.cfg.LocalBootstrap(blobCtx, a.Cluster, a.SourceProfile.Requirements())
		if err != nil {
			s.handleBootstrapBlobErr(a.MachineID, err, "bootstrap: ")
			return formatErr("bootstrap: LocalBootstrap", err)
		}
	} else {
		sess := s.lookupSession(a.Cluster)
		if sess == nil {
			// No session = orchestration failure, not provider failure.
			// Rollback to Idle so the next cycle retries on a connected
			// session.
			_ = s.applyTransition(a.MachineID, machine.StateIdle, func(m *machine.Machine) { m.Cluster = "" })
			return fmt.Errorf("bootstrap: no active operator session for cluster %s", a.Cluster)
		}
		var err error
		bootstrapStart := time.Now()
		blob, err = sess.requestBootstrap(blobCtx, a.Cluster, a.SourceProfile.Requirements())
		metrics.ShardRequestBootstrap.Observe(time.Since(bootstrapStart).Seconds())
		if err != nil {
			s.handleBootstrapBlobErr(a.MachineID, err, "bootstrap: ")
			return formatErr("bootstrap: requestBootstrap", err)
		}
	}

	intPen := decision.BucketUpperBoundDollars(a.SourceProfile.InterruptionPenaltyBucket())
	recPen := decision.BucketUpperBoundDollars(a.SourceProfile.ReclamationPenaltyBucket())
	priority := a.SourceProfile.Priority()

	ack, err := s.cfg.Provider.Configure(ctx, provider.ConfigureRequest{
		MachineID:     a.MachineID,
		ClusterID:     a.Cluster,
		BootstrapBlob: blob,
		// M72 / paper §7: the assignment's protection state rides to the
		// provider as opaque store-and-echo metadata so a shard restart
		// can rebuild it from List+Get (reconcile.go decodes it on the
		// Insert path). Configure is the ONLY writer: these values never
		// change between Configure and Drain — every reassignment is a
		// Drain (which clears the echo with the binding) followed by a
		// fresh Configure, and the post-Configure transition below stamps
		// the same values locally — so write-once keeps the provider copy
		// exact, with no drift window to re-Configure over.
		ShardMetadata: machine.EncodeShardMetadata(priority, intPen, recPen, a.SourceProfile.Fingerprint()),
	})
	if err != nil {
		_ = s.applyTransition(a.MachineID, machine.StateFailed, func(m *machine.Machine) {
			m.LastError = "configure: " + err.Error()
		})
		return formatErr("bootstrap: provider.Configure", err)
	}
	configured, mErr := conv.MachineFromProto(conv.MachineToProto(ack.Machine))
	_ = mErr

	if err := s.applyTransition(a.MachineID, configured.State, func(m *machine.Machine) {
		m.Cluster = a.Cluster
		m.Host = configured.Host
		m.Profile = configured.Profile
		m.AssignedPriority = priority
		m.AssignedInterruptionPenaltyDollars = intPen
		m.AssignedReclamationPenaltyDollars = recPen
		m.AssignedNeedFingerprint = a.SourceProfile.Fingerprint()
	}); err != nil {
		// Detect the Configuring→Idle race (see errProvisionRacedToIdle
		// above). If the FSM rejected the transition AND the machine
		// is now at Idle, a parallel actor flipped it back between
		// our Idle→Configuring transition and this post-Configure
		// commit. Recovery happens naturally next cycle.
		if errors.Is(err, machine.ErrInvalidTransition) {
			if cur, getErr := s.inv.Get(a.MachineID); getErr == nil && cur.State == machine.StateIdle {
				return errProvisionRacedToIdle
			}
		}
		return formatErr("bootstrap: post-Configure transition", err)
	}
	// Drop R: configure-phase timer ends at the post-Configure
	// transition. This is the gap that pod-shim observes as
	// "UpcomingNode created in Configuring, updated to Ready later"
	// — the wall-clock between the two NodeStateUpdate frames.
	metrics.ShardConfigurePhase.Observe(time.Since(configureStart).Seconds())
	// Paper §10.7: end-to-end provisioning latency from first
	// rolled-up demand observation to Configured. Best-effort —
	// silently skipped if the fingerprint isn't in demandObservedAt
	// (e.g., the action was emitted by Phase 2 / Phase 3 paths that
	// aren't gated on rolled-up demand).
	if configured.State == machine.StateConfigured {
		s.observeProvisioningLatency(a.Cluster, a.SourceProfile.Fingerprint())
	}
	return nil
}

// executeDrain handles both Reclaim (Phase 3) and Preempt (Phase 2):
// walks Configured → Draining → Idle through provider.Drain. Both kinds
// notify the cluster operator first so the cordon + PDB-respecting
// eviction path (ADR-0009) runs ahead of the provider drain.
func (s *Shard) executeDrain(ctx context.Context, a decision.Action) error {
	cur, err := s.inv.Get(a.MachineID)
	if err != nil {
		return formatErr("drain: inventory get", err)
	}
	if cur.State != machine.StateConfigured {
		// Already moving — nothing to do this cycle.
		return nil
	}

	// M69: PreemptorPriority is 0 on a voluntary Reclaim — there is no
	// preemptor; the operator records it as telemetry only.
	if sess := s.lookupSession(a.Cluster); sess != nil {
		sess.sendReclaimInstruction(a.MachineID, a.GracePeriod, a.PreemptorPriority)
	} else if a.Kind == decision.ActionKindReclaim {
		// No session: we still drain via the provider below (kubelet
		// default grace), but a voluntary reclaim is supposed to be the
		// graceful path — skipping the operator's cordon/PDB/evict pass
		// deserves its own alertable log line, unlike the historically
		// silent Preempt fallback.
		s.log.Warn("reclaim fallback: no operator session; PDB-respecting drain skipped",
			"machine", a.MachineID, "cluster", a.Cluster)
	}

	if err := s.applyTransition(a.MachineID, machine.StateDraining, nil); err != nil {
		return formatErr("drain: → Draining", err)
	}
	// Drop W: symmetric to configure_phase. Times the Configured → Idle
	// wall-clock so we can tell whether Reclaim is per-action slow (high
	// p99 here) or just under-saturated (low p99 with low Reclaim rate).
	drainStart := time.Now()
	ack, err := s.cfg.Provider.Drain(ctx, provider.DrainRequest{
		MachineID:   a.MachineID,
		GracePeriod: provider.GracePeriod(a.GracePeriod.Seconds()),
	})
	if err != nil {
		_ = s.applyTransition(a.MachineID, machine.StateFailed, func(m *machine.Machine) {
			m.LastError = "drain: " + err.Error()
		})
		return formatErr("drain: provider.Drain", err)
	}
	drained, mErr := conv.MachineFromProto(conv.MachineToProto(ack.Machine))
	_ = mErr
	if err := s.applyTransition(a.MachineID, drained.State, func(m *machine.Machine) {
		m.Cluster = ""
		m.AssignedPriority = 0
		m.AssignedInterruptionPenaltyDollars = 0
		m.AssignedReclamationPenaltyDollars = 0
		m.AssignedNeedFingerprint = ""
		if !drained.Host.Empty() {
			m.Host = drained.Host
		}
	}); err != nil {
		return formatErr("drain: post-Drain transition", err)
	}
	metrics.ShardDrainPhase.Observe(time.Since(drainStart).Seconds())
	return nil
}
