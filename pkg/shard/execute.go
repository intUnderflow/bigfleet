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
		metrics.ShardActionExecuteOutcomes.WithLabelValues(a.Kind.String(), classifyExecuteError(ctx, err)).Inc()
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

// classifyExecuteError maps the err return of execute() to one of a
// fixed set of outcome labels for the per-execute counter.
func classifyExecuteError(ctx context.Context, err error) string {
	if err == nil {
		return "success"
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "ctx_canceled"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "no active operator session"):
		return "no_session"
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

	ack, err := s.cfg.Provider.Configure(ctx, provider.ConfigureRequest{
		MachineID:     a.MachineID,
		ClusterID:     a.Cluster,
		BootstrapBlob: blob,
	})
	if err != nil {
		_ = s.applyTransition(a.MachineID, machine.StateFailed, func(m *machine.Machine) {
			m.LastError = "configure: " + err.Error()
		})
		return formatErr("bootstrap: provider.Configure", err)
	}
	configured, mErr := conv.MachineFromProto(conv.MachineToProto(ack.Machine))
	_ = mErr

	intPen := decision.BucketUpperBoundDollars(a.SourceProfile.InterruptionPenaltyBucket())
	recPen := decision.BucketUpperBoundDollars(a.SourceProfile.ReclamationPenaltyBucket())
	priority := a.SourceProfile.Priority()

	if err := s.applyTransition(a.MachineID, configured.State, func(m *machine.Machine) {
		m.Cluster = a.Cluster
		m.Host = configured.Host
		m.Profile = configured.Profile
		m.AssignedPriority = priority
		m.AssignedInterruptionPenaltyDollars = intPen
		m.AssignedReclamationPenaltyDollars = recPen
		m.AssignedNeedFingerprint = a.SourceProfile.Fingerprint()
	}); err != nil {
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
// walks Configured → Draining → Idle through provider.Drain. For
// Preempt, also notifies the cluster operator so kubelet-side graceful
// shutdown begins immediately.
func (s *Shard) executeDrain(ctx context.Context, a decision.Action) error {
	cur, err := s.inv.Get(a.MachineID)
	if err != nil {
		return formatErr("drain: inventory get", err)
	}
	if cur.State != machine.StateConfigured {
		// Already moving — nothing to do this cycle.
		return nil
	}

	if a.Kind == decision.ActionKindPreempt {
		if sess := s.lookupSession(a.Cluster); sess != nil {
			sess.sendReclaimInstruction(a.MachineID, a.GracePeriod, a.PreemptorPriority)
		}
		// If no session is available we still drain via the provider —
		// the kubelet will use its own default grace period.
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
