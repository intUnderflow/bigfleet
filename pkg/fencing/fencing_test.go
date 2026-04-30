package fencing_test

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
)

func TestCoordinatorTerm_AcceptsAndAdvances(t *testing.T) {
	t.Parallel()
	c := fencing.NewCoordinatorTerm()
	if got := c.HighWaterMark(); got != 0 {
		t.Fatalf("initial hwm = %d, want 0", got)
	}
	if err := c.Validate(3); err != nil {
		t.Errorf("Validate(3): %v", err)
	}
	if got := c.HighWaterMark(); got != 3 {
		t.Errorf("after advance hwm = %d, want 3", got)
	}
	// Same-term repeats are accepted.
	if err := c.Validate(3); err != nil {
		t.Errorf("Validate(3) repeat: %v", err)
	}
	// Higher term advances.
	if err := c.Validate(5); err != nil {
		t.Errorf("Validate(5): %v", err)
	}
	if got := c.HighWaterMark(); got != 5 {
		t.Errorf("hwm = %d, want 5", got)
	}
}

func TestCoordinatorTerm_RejectsStale(t *testing.T) {
	t.Parallel()
	c := fencing.NewCoordinatorTerm()
	_ = c.Validate(5)
	err := c.Validate(4)
	if !errors.Is(err, fencing.ErrStaleTerm) {
		t.Errorf("expected ErrStaleTerm, got %v", err)
	}
	if got := c.HighWaterMark(); got != 5 {
		t.Errorf("hwm changed after rejection: %d", got)
	}
}

func TestCoordinatorTerm_ConcurrentValidate(t *testing.T) {
	t.Parallel()
	c := fencing.NewCoordinatorTerm()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Validate(int64(i))
		}()
	}
	wg.Wait()
	if got := c.HighWaterMark(); got != 99 {
		t.Errorf("hwm = %d, want 99", got)
	}
}

func TestEpoch_PersistsAndIncrements(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch")

	for want := int64(1); want <= 5; want++ {
		e, err := fencing.LoadEpoch(path)
		if err != nil {
			t.Fatalf("LoadEpoch round %d: %v", want, err)
		}
		if e.Value() != want {
			t.Errorf("round %d: epoch = %d, want %d", want, e.Value(), want)
		}
	}
}

func TestEpoch_RejectsCorruptFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch")
	if err := os.WriteFile(path, []byte("not-a-number"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := fencing.LoadEpoch(path)
	if err == nil {
		t.Errorf("expected error parsing corrupt epoch file")
	}
}

func TestSequence_MonotonicAndConcurrent(t *testing.T) {
	t.Parallel()
	var seq fencing.Sequence
	const n = 200
	results := make(chan int64, n)
	for i := 0; i < n; i++ {
		go func() { results <- seq.Next() }()
	}
	seen := make(map[int64]bool, n)
	for i := 0; i < n; i++ {
		v := <-results
		if v == 0 {
			t.Errorf("Next returned 0; must be non-zero")
		}
		if seen[v] {
			t.Errorf("Next returned duplicate: %d", v)
		}
		seen[v] = true
	}
}
