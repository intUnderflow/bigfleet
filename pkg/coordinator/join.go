package coordinator

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// joinLoop reconciles this replica's Raft membership until it is a
// voter at its CURRENT advertise address (ADR-0047). The conventional
// hashicorp/raft StatefulSet pattern: ordinal 0 bootstraps, every
// other ordinal starts with an empty configuration and is voted in by
// the leader.
//
// It is a reconciler rather than a one-shot join because the TCP
// transport advertises a resolved IP (ResolveTCPAddr on the pod's DNS
// name), and in Kubernetes every pod restart changes that IP — the
// cluster's configuration keeps the dead one until an AddVoter
// rewrites it. So the loop runs on EVERY ordinal, every start:
//
//   - Fresh ordinal-N join: ask the leader via JoinRaftCluster until
//     it accepts. Dialing JoinAddress fresh per attempt re-resolves
//     DNS, so pointing it at the headless Service spreads attempts
//     across replicas until one is the leader (followers answer
//     FailedPrecondition).
//   - Restart with unchanged address: membershipCurrent is true
//     within a heartbeat interval; the loop exits without a join. If
//     a join races in first, AddVoter of an existing voter is
//     idempotent in hashicorp/raft (the configuration change rewrites
//     the address in place — pinned by
//     TestAddVoter_ExistingVoterIsIdempotent).
//   - Restart with a new IP: the leader can't reach our old address,
//     so we ask it to AddVoter us at the new one. If we won an
//     election ourselves in the meantime (our log was current and we
//     dial peers outbound), fix our own entry locally — the RPC is
//     leader-only and we are the leader.
func (c *Coordinator) joinLoop(ctx context.Context) {
	const (
		initialBackoff = time.Second
		maxBackoff     = 15 * time.Second
	)
	backoff := initialBackoff
	for {
		if c.membershipCurrent() {
			c.log.Info("raft membership established", "leader", c.LeaderAddress())
			return
		}
		var err error
		if c.IsLeader() {
			err = c.AddVoter(c.cfg.NodeID, string(c.transport.LocalAddr()))
		} else {
			err = c.joinOnce(ctx)
		}
		if err != nil {
			c.log.Info("join attempt failed; will retry", "join_addr", c.cfg.JoinAddress, "backoff", backoff, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// membershipCurrent reports whether this replica observes a leader AND
// the latest configuration it knows lists this node as a voter at its
// current advertise address. The address check is the part that heals
// pod-IP churn; "observes a leader" guards against declaring victory
// from a stale local config while partitioned.
func (c *Coordinator) membershipCurrent() bool {
	if c.LeaderAddress() == "" {
		return false
	}
	future := c.raft.GetConfiguration()
	if future.Error() != nil {
		return false
	}
	self := c.transport.LocalAddr()
	for _, s := range future.Configuration().Servers {
		if s.ID == raft.ServerID(c.cfg.NodeID) {
			return s.Suffrage == raft.Voter && s.Address == self
		}
	}
	return false
}

// joinOnce makes one JoinRaftCluster attempt against JoinAddress.
func (c *Coordinator) joinOnce(ctx context.Context) error {
	// ADR-0048: under mTLS the replica presents the coordinator's own
	// certificate (URI SAN bigfleet://admin) to the leader's
	// admin-gated JoinRaftCluster. Zero-value TLS = plaintext.
	dialOpts, err := c.cfg.TLS.DialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(c.cfg.JoinAddress, dialOpts...)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = pb.NewCoordinatorClient(conn).JoinRaftCluster(rpcCtx, &pb.JoinRaftClusterRequest{
		NodeId: c.cfg.NodeID,
		// LocalAddr is the resolved advertise address — what peers
		// must dial to reach this node's Raft transport.
		RaftAddress: string(c.transport.LocalAddr()),
	})
	return err
}
