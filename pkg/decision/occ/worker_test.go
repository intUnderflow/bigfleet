package occ_test

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

func TestWorkerPool_DefaultWorkersIsGOMAXPROCS(t *testing.T) {
	t.Parallel()
	p := occ.NewWorkerPool(0)
	if got, want := p.Workers(), runtime.GOMAXPROCS(0); got != want {
		t.Fatalf("default workers = %d, want GOMAXPROCS = %d", got, want)
	}
	if got, want := occ.NewWorkerPool(-1).Workers(), runtime.GOMAXPROCS(0); got != want {
		t.Fatalf("negative workers fallback = %d, want GOMAXPROCS = %d", got, want)
	}
}

func TestWorkerPool_ExplicitWorkers(t *testing.T) {
	t.Parallel()
	if got, want := occ.NewWorkerPool(7).Workers(), 7; got != want {
		t.Fatalf("explicit workers = %d, want 7", got)
	}
}

func TestWorkerPool_EmptyQueueExitsImmediately(t *testing.T) {
	t.Parallel()
	p := occ.NewWorkerPool(4)
	called := 0
	p.RunCycle(nil, func(_ needs.Need) {
		called++
	})
	if called != 0 {
		t.Fatalf("empty queue invoked fn %d times, want 0", called)
	}
}

func TestWorkerPool_EachNeedProcessedExactlyOnce(t *testing.T) {
	t.Parallel()
	const N = 500
	queue := make([]needs.Need, N)
	for i := range queue {
		queue[i] = needs.Need{ClusterID: machine.ClusterID("c-" + strconv.Itoa(i))}
	}

	var mu sync.Mutex
	seen := make(map[machine.ClusterID]int)
	occ.NewWorkerPool(8).RunCycle(queue, func(n needs.Need) {
		mu.Lock()
		seen[n.ClusterID]++
		mu.Unlock()
	})

	if got := len(seen); got != N {
		t.Fatalf("distinct Needs processed = %d, want %d", got, N)
	}
	for cid, count := range seen {
		if count != 1 {
			t.Fatalf("Need %q processed %d times, want exactly 1", cid, count)
		}
	}
}

func TestWorkerPool_WorkersActuallyParallel(t *testing.T) {
	t.Parallel()
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("need GOMAXPROCS >= 2 to observe parallelism")
	}
	const workers = 8
	const N = 100
	queue := make([]needs.Need, N)
	for i := range queue {
		queue[i] = needs.Need{ClusterID: machine.ClusterID("c-" + strconv.Itoa(i))}
	}

	// Each worker fn blocks on `gate` until enough goroutines have
	// entered to confirm genuine parallelism. The Nth concurrent
	// arrival closes the gate, releasing everyone. If the pool were
	// single-threaded the gate would never reach N and the test
	// would deadlock — so the assertion below also doubles as a
	// liveness check.
	gate := make(chan struct{})
	var inFlight, peak atomic.Int32
	var once sync.Once

	occ.NewWorkerPool(workers).RunCycle(queue, func(_ needs.Need) {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		if cur >= int32(workers) {
			once.Do(func() { close(gate) })
		}
		<-gate
	})

	if got := peak.Load(); got < int32(workers) {
		t.Fatalf("peak concurrent in-flight = %d, want >= %d", got, workers)
	}
}

func TestWorkerPool_WorkersClampedToQueueSize(t *testing.T) {
	t.Parallel()
	// With 4 Needs and 16 workers, only 4 workers should actually
	// run — spawning the rest is wasted setup cost. We can't
	// directly observe goroutine count without ugly hooks, but we
	// can confirm correctness still holds.
	const N = 4
	queue := make([]needs.Need, N)
	for i := range queue {
		queue[i] = needs.Need{ClusterID: machine.ClusterID("c-" + strconv.Itoa(i))}
	}

	var count atomic.Int64
	occ.NewWorkerPool(16).RunCycle(queue, func(_ needs.Need) {
		count.Add(1)
	})
	if got := count.Load(); got != N {
		t.Fatalf("count = %d, want %d", got, N)
	}
}
