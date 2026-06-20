package main

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func seedSpec(p *fake.Provider, n int) {
	for i := 0; i < n; i++ {
		p.AddSpeculative(machine.ID("spec-"+strconv.Itoa(i)),
			machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 1, 0)
	}
}

// TestRunSpeculativeShed_TracksDemandDown is the reclaim-cycle Run-1 fix:
// at the scheduled offset the shed removes (1 - surviveMultiplier) of the
// Speculative pool so the burst quota tracks the demand scale-down. A tiny
// atSeconds keeps the test fast.
func TestRunSpeculativeShed_TracksDemandDown(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	seedSpec(p, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		runSpeculativeShed(ctx, p, 0 /*fire immediately*/, 0.5, quietLogger())
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("runSpeculativeShed did not return")
	}

	if got := p.SpeculativeCount(); got != 50 {
		t.Errorf("post-shed SpeculativeCount = %d, want 50 (kept surviveMultiplier=0.5)", got)
	}
}

// TestRunSpeculativeShed_NoOpAtMultiplierOne: a survive-multiplier of 1
// (the default, no scale-down) sheds nothing and returns immediately.
func TestRunSpeculativeShed_NoOpAtMultiplierOne(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	seedSpec(p, 40)

	done := make(chan struct{})
	go func() {
		// atSeconds large, but the multiplier>=1 guard must short-circuit
		// before any wait, so this returns at once.
		runSpeculativeShed(context.Background(), p, 100000, 1.0, quietLogger())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runSpeculativeShed(multiplier=1) did not short-circuit")
	}
	if got := p.SpeculativeCount(); got != 40 {
		t.Errorf("SpeculativeCount = %d, want 40 (multiplier 1 = no shed)", got)
	}
}

// TestRunSpeculativeShed_CancelBeforeFire: cancelling the context before
// the offset elapses must exit without shedding.
func TestRunSpeculativeShed_CancelBeforeFire(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	seedSpec(p, 30)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runSpeculativeShed(ctx, p, 3600 /*far future*/, 0.5, quietLogger())
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runSpeculativeShed did not exit on context cancel")
	}
	if got := p.SpeculativeCount(); got != 30 {
		t.Errorf("SpeculativeCount = %d, want 30 (cancelled before fire = no shed)", got)
	}
}

// TestSeedZoneRack pins the ADR-0040 Addendum §4 blocking math:
// sameRack archetypes fill one (zone, rack) for a whole max-group-size
// block before advancing; everything else round-robins per machine.
func TestSeedZoneRack(t *testing.T) {
	t.Parallel()
	zones := []string{"zone-a", "zone-b", "zone-c"}
	sameRack := &archetype.Archetype{Name: "gpu-train", SameRack: true, GroupSizeRange: [2]int{2, 4}}
	sameRackNoRange := &archetype.Archetype{Name: "db", SameRack: true}
	spread := &archetype.Archetype{Name: "cpu-svc", GroupSizeRange: [2]int{2, 4}}

	cases := []struct {
		name         string
		a            *archetype.Archetype
		idx          int
		racksPerZone int
		zones        []string
		wantZone     string
		wantRack     string
	}{
		// nil archetype (legacy seed): zone and rack round-robin on idx.
		{"nil archetype idx 0", nil, 0, 10, zones, "zone-a", "zone-a-rack-0"},
		{"nil archetype idx 1", nil, 1, 10, zones, "zone-b", "zone-b-rack-1"},
		{"nil archetype rack wraps", nil, 11, 10, zones, "zone-c", "zone-c-rack-1"},
		// non-sameRack archetype: round-robin, even with a GroupSizeRange.
		{"non-sameRack idx 0", spread, 0, 10, zones, "zone-a", "zone-a-rack-0"},
		{"non-sameRack idx 1", spread, 1, 10, zones, "zone-b", "zone-b-rack-1"},
		// sameRack, block = GroupSizeRange[1] = 4: indices 0-3 share one
		// (zone, rack); index 4 starts the next block.
		{"sameRack block start", sameRack, 0, 10, zones, "zone-a", "zone-a-rack-0"},
		{"sameRack block interior", sameRack, 3, 10, zones, "zone-a", "zone-a-rack-0"},
		{"sameRack next block", sameRack, 4, 10, zones, "zone-b", "zone-b-rack-1"},
		{"sameRack third block", sameRack, 8, 10, zones, "zone-c", "zone-c-rack-2"},
		// Rack ordinal wraps at racksPerZone (block 10 → rack-0 again).
		{"sameRack rack wrap", sameRack, 40, 10, zones, "zone-b", "zone-b-rack-0"},
		// Unset GroupSizeRange falls back to blocks of 8.
		{"fallback block interior", sameRackNoRange, 7, 10, zones, "zone-a", "zone-a-rack-0"},
		{"fallback next block", sameRackNoRange, 8, 10, zones, "zone-b", "zone-b-rack-1"},
		// Empty zone list defaults to zone-a.
		{"no zones", sameRack, 5, 10, nil, "zone-a", "zone-a-rack-1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			zone, rack := seedZoneRack(c.a, c.idx, c.racksPerZone, c.zones)
			if zone != c.wantZone || rack != c.wantRack {
				t.Errorf("seedZoneRack(%v, %d, %d) = (%q, %q), want (%q, %q)",
					c.a, c.idx, c.racksPerZone, zone, rack, c.wantZone, c.wantRack)
			}
		})
	}
}

// TestSeedZoneRack_BlocksAreContiguous sweeps a counter range and
// asserts the contiguity property the table can't express directly:
// every run of block-size consecutive per-archetype machines shares
// exactly one rack label, and consecutive blocks differ.
func TestSeedZoneRack_BlocksAreContiguous(t *testing.T) {
	t.Parallel()
	a := &archetype.Archetype{Name: "x", SameRack: true, GroupSizeRange: [2]int{3, 5}}
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const block = 5
	var prev string
	for i := 0; i < 100; i++ {
		_, rack := seedZoneRack(a, i, 10, zones)
		if i%block == 0 {
			if rack == prev {
				t.Fatalf("idx %d: new block reused rack %q", i, rack)
			}
			prev = rack
			continue
		}
		if rack != prev {
			t.Fatalf("idx %d: rack %q differs from block rack %q — block fragmented", i, rack, prev)
		}
	}
}

func TestParseStatefulSetOrdinal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"bigfleet-shard-0", 0, false},
		{"bigfleet-shard-2", 2, false},
		{"shard-17", 17, false},
		{"a-1234567", 1234567, false},
		{"shard-", 0, true},
		{"shard", 0, true},
		{"shard-abc", 0, true},
		{"", 0, true},
	}
	for _, c := range cases {
		got, err := parseStatefulSetOrdinal(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseStatefulSetOrdinal(%q) = %d, nil; want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseStatefulSetOrdinal(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseStatefulSetOrdinal(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
