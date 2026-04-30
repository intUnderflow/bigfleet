// Package operator implements the per-cluster BigFleet agent.
//
// Each Kubernetes cluster runs one of these agents. It dials the shard
// over gRPC, holds a long-lived bidirectional Shard.Session stream, and
// multiplexes:
//
//   - outbound: ClusterCapacityNeeds rollups (every 10s, full
//     replacement), BootstrapBlobResponse to shard pulls, ReclaimAck
//     to drain instructions.
//   - inbound: BootstrapRequest, ReclaimInstruction, NodeStateUpdate,
//     AvailableCapacityUpdate.
//
// The operator never opens an inbound listener. All cluster ↔ shard
// traffic is multiplexed on the one stream the operator dials. See
// CLAUDE.md ("No inbound listener on the cluster operator").
package operator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// Config configures an Operator. ClusterID and ShardAddress are
// required; everything else has reasonable defaults.
type Config struct {
	// ClusterID is this cluster's stable identifier. Must match the
	// cluster_id used in CapacityRequests / UpcomingNodes.
	ClusterID machine.ClusterID

	// ShardAddress is the host:port of the BigFleet shard's gRPC
	// endpoint.
	ShardAddress string

	// KubeClient is the controller-runtime client used to read
	// CapacityRequests and write UpcomingNodes. Must be wired against a
	// scheme that has bigfleet.lucy.sh/v1alpha1 registered.
	KubeClient client.Client

	// Namespace is where AvailableCapacity / UpcomingNode CRs live in
	// the paper's kubectl experience. CRs are cluster-scoped so this
	// only affects metadata Namespace fields where applicable.
	Namespace string

	// RollupInterval is how often the rollup loop fires. Default 10s
	// per the operating-model paper.
	RollupInterval time.Duration

	// AcknowledgeConcurrency caps the number of in-flight status
	// updates the rollup loop runs in parallel when transitioning
	// CRs from Pending to Acknowledged. Default 16. Bump on clusters
	// that have raised apiserver QPS limits accordingly.
	AcknowledgeConcurrency int

	// ReconnectInitialBackoff / ReconnectMaxBackoff bound the
	// reconnect-with-backoff schedule.
	ReconnectInitialBackoff time.Duration
	ReconnectMaxBackoff     time.Duration

	// BootstrapTemplate is the function the operator uses to render a
	// kubelet bootstrap blob in response to a BootstrapRequest. If nil,
	// a stub template is used (M4); a real template lands when the
	// kind-based e2e in M5 actually joins kubelets.
	BootstrapTemplate BootstrapRenderer

	// Logger receives structured events.
	Logger *slog.Logger
}

// BootstrapRenderer produces a bootstrap blob from a BootstrapRequest's
// requirements. Returning a non-empty error string in the response
// signals that the cluster cannot satisfy the request (e.g., kubelet
// version skew); the shard treats this as an unsatisfiable requirement.
type BootstrapRenderer func(ctx context.Context, req BootstrapRendererInput) (BootstrapRendererOutput, error)

// BootstrapRendererInput is what the operator passes to a renderer.
type BootstrapRendererInput struct {
	ClusterID    machine.ClusterID
	Requirements []RequirementInput
}

// RequirementInput is the renderer's view of a single requirement.
// Decoupled from the proto type so renderers don't pull in the proto
// package directly.
type RequirementInput struct {
	Key      string
	Operator string // "In" | "NotIn" | "Exists" | "DoesNotExist" | "Same"
	Values   []string
}

// BootstrapRendererOutput is what a renderer returns. UserData is the
// opaque blob forwarded to the provider's Configure RPC.
type BootstrapRendererOutput struct {
	UserData   []byte
	TTLSeconds int64
}

// Operator is the running per-cluster agent.
type Operator struct {
	cfg Config
	log *slog.Logger
}

// New constructs an Operator. Returns error if required Config fields
// are missing.
func New(cfg Config) (*Operator, error) {
	if cfg.ClusterID == "" {
		return nil, errors.New("operator: Config.ClusterID is required")
	}
	if cfg.ShardAddress == "" {
		return nil, errors.New("operator: Config.ShardAddress is required")
	}
	if cfg.KubeClient == nil {
		return nil, errors.New("operator: Config.KubeClient is required")
	}
	if cfg.RollupInterval == 0 {
		cfg.RollupInterval = 10 * time.Second
	}
	if cfg.AcknowledgeConcurrency <= 0 {
		cfg.AcknowledgeConcurrency = 16
	}
	if cfg.ReconnectInitialBackoff == 0 {
		cfg.ReconnectInitialBackoff = 500 * time.Millisecond
	}
	if cfg.ReconnectMaxBackoff == 0 {
		cfg.ReconnectMaxBackoff = 30 * time.Second
	}
	if cfg.BootstrapTemplate == nil {
		cfg.BootstrapTemplate = stubBootstrapRenderer
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Operator{
		cfg: cfg,
		log: log.With("component", "operator", "cluster", string(cfg.ClusterID)),
	}, nil
}

// Run blocks until ctx is cancelled, holding the stream and driving the
// roll-up loop, reconnecting as needed.
func (o *Operator) Run(ctx context.Context) error {
	o.log.Info("operator started", "shard", o.cfg.ShardAddress, "rollup_interval", o.cfg.RollupInterval)
	defer o.log.Info("operator stopped")

	backoff := o.cfg.ReconnectInitialBackoff
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := o.runOnce(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		o.log.Warn("session ended, reconnecting", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff, o.cfg.ReconnectMaxBackoff)
	}
}

// nextBackoff doubles the previous interval up to the configured max.
func nextBackoff(prev, max time.Duration) time.Duration {
	if prev <= 0 {
		return max
	}
	next := prev * 2
	if next > max {
		return max
	}
	return next
}

// stubBootstrapRenderer is the placeholder M4 renderer. Real kubelet
// bootstrap (token minting, CA bundle, kubelet config) is wired in M5
// when an actual kubelet has to join a kind cluster.
func stubBootstrapRenderer(_ context.Context, in BootstrapRendererInput) (BootstrapRendererOutput, error) {
	return BootstrapRendererOutput{
		UserData:   []byte(fmt.Sprintf("#cloud-config\n# bigfleet stub bootstrap for %s\n", in.ClusterID)),
		TTLSeconds: 600,
	}, nil
}
