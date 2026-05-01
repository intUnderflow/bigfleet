package shard_test

import (
	"go/build"
	"strings"
	"testing"
)

// Static-stability is the load-bearing safety property of BigFleet:
// shards must keep operating when the coordinator is down. The
// architectural rule that makes this defensible is: pkg/shard must
// NOT import pkg/coordinator (CLAUDE.md "Hard rules"). This test
// fails if a future change accidentally introduces that dependency.
//
// Coordinator client adapters can live in a sub-package the hot path
// doesn't import (e.g., pkg/shard/coordclient/), but anything reachable
// from the cycle through pkg/shard's own files is forbidden.
func TestStaticStability_ShardDoesNotImportCoordinator(t *testing.T) {
	t.Parallel()
	pkg, err := build.Default.Import("github.com/intUnderflow/bigfleet/pkg/shard", ".", 0)
	if err != nil {
		t.Fatalf("import pkg/shard: %v", err)
	}
	for _, dep := range pkg.Imports {
		if strings.HasPrefix(dep, "github.com/intUnderflow/bigfleet/pkg/coordinator") {
			t.Errorf("pkg/shard must not import pkg/coordinator (got %s) — see CLAUDE.md Hard rules", dep)
		}
	}
	for _, dep := range pkg.TestImports {
		if strings.HasPrefix(dep, "github.com/intUnderflow/bigfleet/pkg/coordinator") {
			t.Errorf("pkg/shard test imports must not pull pkg/coordinator (got %s)", dep)
		}
	}
}
