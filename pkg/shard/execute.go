package shard

import (
	"context"
	"errors"
	"fmt"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/provider"
)

// execute dispatches an Action to the appropriate handler. Each handler
// drives the machine through its state-machine transitions and the
// matching provider RPC. Errors are returned to the caller (the cycle)
// so they can be logged, but they do not abort the cycle: the next
// cycle re-derives actions from current state.
func (s *Shard) execute(ctx context.Context, a decision.Action) error {
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
	if cur.State != machine.StateIdle {
		return fmt.Errorf("bootstrap: machine %s in state %s; expected Idle", a.MachineID, cur.State)
	}

	sess := s.lookupSession(a.Cluster)
	if sess == nil {
		return fmt.Errorf("bootstrap: no active operator session for cluster %s", a.Cluster)
	}

	// Idle → Configuring (record the destination cluster early so
	// observers can see it).
	if err := s.applyTransition(a.MachineID, machine.StateConfiguring, func(m *machine.Machine) {
		m.Cluster = a.Cluster
	}); err != nil {
		return formatErr("bootstrap: → Configuring", err)
	}

	// Pull a kubelet bootstrap blob from the operator.
	blobCtx, cancel := context.WithTimeout(ctx, s.cfg.BootstrapTimeout)
	defer cancel()
	blob, err := sess.requestBootstrap(blobCtx, a.Cluster, a.SourceProfile.Requirements())
	if err != nil {
		_ = s.applyTransition(a.MachineID, machine.StateFailed, func(m *machine.Machine) {
			m.LastError = "bootstrap: " + err.Error()
		})
		return formatErr("bootstrap: requestBootstrap", err)
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
	}); err != nil {
		return formatErr("bootstrap: post-Configure transition", err)
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
		if !drained.Host.Empty() {
			m.Host = drained.Host
		}
	}); err != nil {
		return formatErr("drain: post-Drain transition", err)
	}
	return nil
}
