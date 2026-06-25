package shard

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// fakeInspectStream captures the frames the handler streams. Only Context
// and Send are exercised by InspectNeeds.
type fakeInspectStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*pb.InspectNeedsResponse
}

func (f *fakeInspectStream) Context() context.Context { return f.ctx }
func (f *fakeInspectStream) Send(m *pb.InspectNeedsResponse) error {
	f.sent = append(f.sent, m)
	return nil
}

func newReadTestShard(t *testing.T) *Shard {
	t.Helper()
	s := &Shard{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	nA := needs.Need{ClusterID: "cluster-a", Profile: nvProfile(1000, false), AggregateResources: nvQty("8"), MinUnit: nvQty("1")}
	nB := needs.Need{ClusterID: "cluster-b", Profile: nvProfile(2000, false), AggregateResources: nvQty("8"), MinUnit: nvQty("1")}
	p1 := decision.Phase1Result{Verdicts: []decision.NeedVerdict{
		{Need: &nA, Satisfied: true, ClaimedCount: 2},
		{Need: &nB, Satisfied: false, Deficit: nvQty("8"), MatchingSupplyExists: false},
	}}
	s.recordNeedViews(7, p1, decision.Phase2Result{})
	return s
}

// InspectNeeds streams a header then one NeedView per Need, mapped to proto.
func TestInspectNeeds_StreamsHeaderAndRows(t *testing.T) {
	t.Parallel()
	rs := NewReadServer(newReadTestShard(t))
	stream := &fakeInspectStream{ctx: context.Background()} // plaintext: gate skipped

	if err := rs.InspectNeeds(&pb.InspectNeedsRequest{}, stream); err != nil {
		t.Fatalf("InspectNeeds: %v", err)
	}
	if len(stream.sent) != 3 {
		t.Fatalf("frames = %d, want 3 (header + 2 needs)", len(stream.sent))
	}
	hdr := stream.sent[0].GetHeader()
	if hdr == nil || hdr.GetCycle() != 7 || hdr.GetTotalNeeds() != 2 {
		t.Fatalf("header wrong: %+v", hdr)
	}

	// Rows sorted priority desc: cluster-b (2000) before cluster-a (1000).
	first := stream.sent[1].GetNeed()
	if first == nil || first.GetClusterId() != "cluster-b" {
		t.Fatalf("first row = %+v, want cluster-b", first)
	}
	if first.GetSatisfied() {
		t.Errorf("cluster-b satisfied=true, want false")
	}
	if first.GetUnmetReason() != pb.UnmetReason_UNMET_REASON_NO_MATCHING_SUPPLY {
		t.Errorf("cluster-b reason = %v, want NO_MATCHING_SUPPLY", first.GetUnmetReason())
	}
	if first.GetResidualDeficit().GetResources()["cpu"] != "8" {
		t.Errorf("cluster-b deficit = %v, want cpu=8", first.GetResidualDeficit().GetResources())
	}
	if first.GetNeed().GetPriority() != 2000 {
		t.Errorf("cluster-b priority = %d, want 2000", first.GetNeed().GetPriority())
	}

	second := stream.sent[2].GetNeed()
	if second.GetClusterId() != "cluster-a" || !second.GetSatisfied() {
		t.Errorf("second row = %+v, want satisfied cluster-a", second)
	}
	if second.GetUnmetReason() != pb.UnmetReason_UNMET_REASON_UNSPECIFIED {
		t.Errorf("satisfied row reason = %v, want UNSPECIFIED", second.GetUnmetReason())
	}
}

// The cluster_id filter restricts the stream to one cluster.
func TestInspectNeeds_ClusterFilter(t *testing.T) {
	t.Parallel()
	rs := NewReadServer(newReadTestShard(t))
	stream := &fakeInspectStream{ctx: context.Background()}

	if err := rs.InspectNeeds(&pb.InspectNeedsRequest{ClusterId: "cluster-a"}, stream); err != nil {
		t.Fatalf("InspectNeeds: %v", err)
	}
	if len(stream.sent) != 2 { // header + 1 need
		t.Fatalf("frames = %d, want 2", len(stream.sent))
	}
	if stream.sent[0].GetHeader().GetTotalNeeds() != 1 {
		t.Errorf("total = %d, want 1", stream.sent[0].GetHeader().GetTotalNeeds())
	}
	if stream.sent[1].GetNeed().GetClusterId() != "cluster-a" {
		t.Errorf("row cluster = %s, want cluster-a", stream.sent[1].GetNeed().GetClusterId())
	}
}

// On a plaintext transport the read gate is skipped (ADR-0048/0061): no peer
// identity, no rejection. (The mTLS reject path is the same code as the
// coordinator's requireReadIdentity, covered by its mTLS identity test.)
func TestShardRead_PlaintextSkipsGate(t *testing.T) {
	t.Parallel()
	rs := NewReadServer(&Shard{log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := rs.requireReadIdentity(context.Background(), "InspectNeeds"); err != nil {
		t.Errorf("plaintext gate = %v, want nil (skipped)", err)
	}
}
