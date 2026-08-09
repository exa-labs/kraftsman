/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"context"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/util/sets"
)

// discoveryResult is the outcome of one candidate's speculative scheduling simulation.
type discoveryResult struct {
	cmd Command
	err error
	// gateSkipped reports that the worker never simulated the candidate because the walk gate
	// already disqualified it. The gate only ever accumulates disqualifications within a pass,
	// so a candidate the gate skipped is guaranteed to also be skipped by the consumer's
	// authoritative in-order checks.
	gateSkipped bool
	// durability captures whether the simulation's no-op verdict is conclusive, mirroring what
	// the serial walk derives from its own per-candidate context.
	durability *noOpDurability
}

// walkGate is the workers' read-only window onto the consumer's in-order bookkeeping. Workers
// consult it at dispatch time to avoid simulating candidates the consumer is already bound to
// skip. It is advisory for the workers and authoritative for nobody: the consumer re-applies
// every check in candidate order before acting on a result. Soundness rests on monotonicity —
// claimed provider IDs and held balanced pools only grow, and pool budgets only decrement —
// so a disqualification a worker observes still holds when the consumer reaches the candidate.
// The converse is not true: a worker may simulate a candidate the consumer later skips, which
// costs a wasted simulation and nothing else.
type walkGate struct {
	mu                    sync.Mutex
	claimedProviderIDs    sets.Set[string]
	balancedNodePoolsHeld sets.Set[string]
	budget                map[string]int
	// canPassThreshold is the evaluator's best-case score check; it reads only NodePool totals
	// fixed before the walk starts.
	canPassThreshold func(*Candidate) bool
}

func newWalkGate(budget map[string]int, canPassThreshold func(*Candidate) bool) *walkGate {
	return &walkGate{
		claimedProviderIDs:    sets.New[string](),
		balancedNodePoolsHeld: sets.New[string](),
		budget:                budget,
		canPassThreshold:      canPassThreshold,
	}
}

// disqualified reports whether the gate's current view already rules the candidate out.
func (g *walkGate) disqualified(c *Candidate) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.claimedProviderIDs.Has(c.ProviderID()) ||
		g.balancedNodePoolsHeld.Has(c.NodePool.Name) ||
		g.budget[c.NodePool.Name] == 0 ||
		!g.canPassThreshold(c)
}

// hold records a held proposal's effects. Only the consumer calls it, in candidate order.
func (g *walkGate) hold(providerIDs []string, nodePoolName string, balanced bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, id := range providerIDs {
		g.claimedProviderIDs.Insert(id)
	}
	g.budget[nodePoolName]--
	if balanced {
		g.balancedNodePoolsHeld.Insert(nodePoolName)
	}
}

// candidateWalker runs candidate scheduling simulations on a pool of workers while preserving
// the walk's candidate order: results are delivered to the consumer strictly in candidate-list
// order, whatever order the workers finish in. Discovery is the only thing parallelized — the
// consumer applies every skip check, scores, holds proposals, and admits commands exactly as
// the serial walk does, one candidate at a time.
//
// Workers speculate: a worker may simulate candidate i+3 against cluster state that does not
// yet reflect a proposal the consumer holds at candidate i. That is safe for the same reason
// the serial walk is safe across admitted commands — a discovery result is a nomination, never
// a decision. Every held proposal is re-simulated against live cluster state by the validator
// immediately before it is queued, so a stale speculative result is rejected at admission, not
// executed. The worst case of speculation is wasted CPU, never a wrong disruption.
//
// Dispatch is bounded to a window past the consumer's position, so a consumer that stops early
// (timeout, maxCommands reached) leaves at most a window's worth of speculative work in flight
// before cancellation lands.
type candidateWalker struct {
	slots     []chan discoveryResult
	workerCtx context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	mu       sync.Mutex
	cond     *sync.Cond
	consumed int
	stopped  bool
	window   int
}

// startCandidateWalker dispatches candidates to workers and returns the walker. simulate must be
// safe for concurrent use; everything it touches through the context is the set of pass-scoped
// caches, all of which serialize access internally.
func startCandidateWalker(
	ctx context.Context,
	candidates []*Candidate,
	workers int,
	gate *walkGate,
	simulate func(context.Context, *Candidate) (Command, error),
) *candidateWalker {
	workerCtx, cancel := context.WithCancel(ctx)
	w := &candidateWalker{
		slots:     make([]chan discoveryResult, len(candidates)),
		workerCtx: workerCtx,
		cancel:    cancel,
		window:    2 * workers,
	}
	w.cond = sync.NewCond(&w.mu)
	for i := range w.slots {
		w.slots[i] = make(chan discoveryResult, 1)
	}

	var next atomic.Int64
	for range workers {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(candidates) {
					return
				}
				if !w.waitForWindow(i) {
					return
				}
				if gate.disqualified(candidates[i]) {
					w.slots[i] <- discoveryResult{gateSkipped: true}
					continue
				}
				candidateCtx, durability := withNoOpDurability(workerCtx)
				cmd, err := simulate(candidateCtx, candidates[i])
				w.slots[i] <- discoveryResult{cmd: cmd, err: err, durability: durability}
			}
		}()
	}
	return w
}

// waitForWindow blocks until candidate i falls within the dispatch window past the consumer's
// position, reporting false when the walker is stopped instead.
func (w *candidateWalker) waitForWindow(i int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i >= w.consumed+w.window && !w.stopped {
		w.cond.Wait()
	}
	return !w.stopped
}

// advance records that the consumer is past candidate i, widening the dispatch window.
func (w *candidateWalker) advance(i int) {
	w.mu.Lock()
	w.consumed = i + 1
	w.mu.Unlock()
	w.cond.Broadcast()
}

// result blocks until candidate i's simulation is delivered, then advances the dispatch window.
// Cancellation of the parent context unblocks it with the context's error, so a controller
// shutdown never strands the consumer waiting on a worker that already exited.
func (w *candidateWalker) result(i int) discoveryResult {
	defer w.advance(i)
	select {
	case res := <-w.slots[i]:
		return res
	case <-w.workerCtx.Done():
		select {
		case res := <-w.slots[i]:
			return res
		default:
			return discoveryResult{err: w.workerCtx.Err()}
		}
	}
}

// discard advances past candidate i without waiting for its simulation. The consumer calls it
// for candidates its own in-order checks skip, so the walk never blocks on a result it will not
// read; the worker's eventual write lands in the slot's buffer unread. It is a no-op on a nil
// walker (the serial walk).
func (w *candidateWalker) discard(i int) {
	if w == nil {
		return
	}
	w.advance(i)
}

// stop cancels outstanding speculative work and waits for the workers to exit. It must be called
// before the pass moves from discovery into anything that observes or mutates cluster state on
// the pass's behalf (validation, admission), so no simulation is left running behind them.
// It is idempotent and a no-op on a nil walker (the serial walk).
func (w *candidateWalker) stop() {
	if w == nil {
		return
	}
	w.cancel()
	w.mu.Lock()
	w.stopped = true
	w.mu.Unlock()
	w.cond.Broadcast()
	w.wg.Wait()
}
