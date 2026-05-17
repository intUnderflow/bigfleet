package occ

import (
	"runtime"
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// WorkerFn is the per-Need scheduling logic — given one Need it
// computes proposals, submits them to the broker, handles conflicts
// via re-read + retry, and records the outcome. It runs concurrently
// across all workers in the pool, so it must not assume single-
// threaded access to any external state.
//
// M46.1 supplies the pool harness only; M46.3 wires a WorkerFn that
// owns the real proposal-construction logic (the OCC replacement for
// phase1Allocator.take). Tests can substitute a trivial WorkerFn for
// pool-mechanics validation.
type WorkerFn func(n needs.Need)

// WorkerPool dispatches Needs across N goroutines sharing one queue.
// Per ADR-0029 there is no worker specialisation — every worker can
// process any Need; the Need's per-Need mode (Incremental vs
// AllOrNothing) is decided at proposal-construction time.
type WorkerPool struct {
	workers int
}

// NewWorkerPool returns a pool sized to workers goroutines. A non-
// positive workers value resolves to runtime.GOMAXPROCS(0), matching
// the ADR-0029 default.
func NewWorkerPool(workers int) *WorkerPool {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	return &WorkerPool{workers: workers}
}

// Workers returns the configured worker count for the pool.
func (p *WorkerPool) Workers() int { return p.workers }

// RunCycle dispatches each Need in queue through fn. Returns when
// the queue drains and every worker has exited — this is the cycle
// barrier described in ADR-0029. After RunCycle returns, any
// SharedState the workers shared is coherent: every commit is
// durable, every conflicted Need has either been retried-and-
// committed or recorded a shortfall via fn's own bookkeeping.
//
// fn must be safe for concurrent invocation across goroutines — it
// will be called by up to p.workers goroutines simultaneously.
func (p *WorkerPool) RunCycle(queue []needs.Need, fn WorkerFn) {
	if len(queue) == 0 {
		return
	}
	workers := p.workers
	if workers > len(queue) {
		workers = len(queue)
	}

	ch := make(chan needs.Need, len(queue))
	for _, n := range queue {
		ch <- n
	}
	close(ch)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for n := range ch {
				fn(n)
			}
		}()
	}
	wg.Wait()
}
