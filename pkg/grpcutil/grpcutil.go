// Package grpcutil centralises the gRPC server/dial options shared by
// every BigFleet process so the wire limits — and the ADR-0048
// opt-in mTLS layer with its bigfleet:// URI SAN identity helpers
// (tls.go) — are implemented in exactly one place.
package grpcutil

import "google.golang.org/grpc"

// MaxMessageBytes is the gRPC send/receive frame ceiling for every
// BigFleet server and client.
//
// gRPC's library default for MaxRecvMsgSize is 4 MiB. That is far too
// small for the Shard.Session stream: a roll-up is full-replacement
// (paper §3.1) and its size scales with a cluster's Need cardinality.
// Once a cluster's ClusterCapacityNeeds message crossed 4 MiB the
// shard's Recv returned ResourceExhausted and tore the session down;
// the operator reconnected, re-sent the same oversized roll-up, and
// failed again — a silent reconnect loop that froze the shard's
// NeedsTable at the last sub-4-MiB roll-up. The build-only
// OperatorRollupDuration metric stayed healthy throughout, so the
// failure was invisible. Surfaced as the devpod-5k "shard
// Bootstrap-emit ceiling" (bigfleet-uber issues #3/#4): 250K+ CRs
// outstanding, NeedsTable stuck at ~49K, Phase 1 correctly emitting
// nothing because the demand never arrived.
//
// 256 MiB is a generous ceiling, not an expected size — roll-ups
// should stay small once demand aggregates by workload. It exists so
// a large roll-up degrades into a slow cycle, never a broken session.
const MaxMessageBytes = 256 << 20 // 256 MiB

// ServerOptions returns the grpc.ServerOption set every BigFleet gRPC
// server is constructed with.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMessageBytes),
		grpc.MaxSendMsgSize(MaxMessageBytes),
	}
}

// DialOptions returns the grpc.DialOption set every BigFleet gRPC
// client is constructed with. Callers append their own transport
// credentials and any per-client options.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageBytes),
			grpc.MaxCallSendMsgSize(MaxMessageBytes),
		),
	}
}
