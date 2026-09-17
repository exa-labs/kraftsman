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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func walkCandidate(i int, nodePool string) *Candidate {
	return &Candidate{
		StateNode: &state.StateNode{
			Node: &corev1.Node{Spec: corev1.NodeSpec{ProviderID: fmt.Sprintf("provider-%d", i)}},
		},
		NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: nodePool}},
	}
}

func walkCandidates(n int) []*Candidate {
	out := make([]*Candidate, n)
	for i := range out {
		out[i] = walkCandidate(i, "np")
	}
	return out
}

func passAll(*Candidate) bool { return true }

// Results must arrive in candidate order regardless of worker completion order.
func TestWalkerDeliversResultsInCandidateOrder(t *testing.T) {
	candidates := walkCandidates(32)
	gate := newWalkGate(map[string]int{"np": 32}, passAll)
	var order []string
	w := startCandidateWalker(context.Background(), candidates, 4, gate, func(_ context.Context, c *Candidate) (Command, error) {
		// Uneven, completion-order-scrambling durations.
		time.Sleep(time.Duration(len(c.ProviderID())%3) * time.Millisecond)
		return Command{}, fmt.Errorf("sim %s", c.ProviderID())
	})
	defer w.stop()
	for i := range candidates {
		res := w.result(i)
		order = append(order, res.err.Error())
	}
	for i := range candidates {
		if want := fmt.Sprintf("sim provider-%d", i); order[i] != want {
			t.Fatalf("result %d = %q, want %q", i, order[i], want)
		}
	}
}

// Every candidate is simulated exactly once.
func TestWalkerSimulatesEachCandidateOnce(t *testing.T) {
	candidates := walkCandidates(50)
	gate := newWalkGate(map[string]int{"np": 50}, passAll)
	var mu sync.Mutex
	seen := map[string]int{}
	w := startCandidateWalker(context.Background(), candidates, 8, gate, func(_ context.Context, c *Candidate) (Command, error) {
		mu.Lock()
		seen[c.ProviderID()]++
		mu.Unlock()
		return Command{}, nil
	})
	for i := range candidates {
		w.result(i)
	}
	w.stop()
	if len(seen) != len(candidates) {
		t.Fatalf("simulated %d candidates, want %d", len(seen), len(candidates))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("candidate %s simulated %d times, want 1", id, n)
		}
	}
}

// Workers must not simulate candidates the gate has already disqualified, and the consumer sees
// those as gate skips.
func TestWalkerGateSkipsDisqualifiedCandidates(t *testing.T) {
	candidates := walkCandidates(10)
	gate := newWalkGate(map[string]int{"np": 10}, passAll)
	// Disqualify even-indexed candidates by claiming their provider IDs up front.
	for i := 0; i < 10; i += 2 {
		gate.hold([]string{fmt.Sprintf("provider-%d", i)}, "np", false)
	}
	var simulated atomic.Int64
	w := startCandidateWalker(context.Background(), candidates, 3, gate, func(_ context.Context, _ *Candidate) (Command, error) {
		simulated.Add(1)
		return Command{}, nil
	})
	defer w.stop()
	for i := range candidates {
		res := w.result(i)
		if got, want := res.gateSkipped, i%2 == 0; got != want {
			t.Fatalf("candidate %d gateSkipped = %v, want %v", i, got, want)
		}
	}
	if simulated.Load() != 5 {
		t.Fatalf("simulated %d candidates, want 5", simulated.Load())
	}
}

// A zero pool budget and a failed threshold check disqualify at the gate too.
func TestWalkGateDisqualifications(t *testing.T) {
	c := walkCandidate(0, "np")
	if newWalkGate(map[string]int{"np": 0}, passAll).disqualified(c) != true {
		t.Fatal("zero budget should disqualify")
	}
	if newWalkGate(map[string]int{"np": 1}, func(*Candidate) bool { return false }).disqualified(c) != true {
		t.Fatal("failed threshold should disqualify")
	}
	if newWalkGate(map[string]int{"np": 1}, passAll).disqualified(c) != false {
		t.Fatal("qualified candidate should not be disqualified")
	}
	balanced := newWalkGate(map[string]int{"np": 2}, passAll)
	balanced.hold(nil, "np", true)
	if balanced.disqualified(c) != true {
		t.Fatal("held balanced pool should disqualify")
	}
}

// Dispatch must stay within the lookahead window of the consumer's position, bounding the
// speculative work wasted when the consumer stops early.
func TestWalkerBoundsSpeculativeLookahead(t *testing.T) {
	const workers = 2
	candidates := walkCandidates(100)
	gate := newWalkGate(map[string]int{"np": 100}, passAll)
	var maxDispatched atomic.Int64
	w := startCandidateWalker(context.Background(), candidates, workers, gate, func(_ context.Context, c *Candidate) (Command, error) {
		var i int
		if _, err := fmt.Sscanf(c.ProviderID(), "provider-%d", &i); err != nil {
			panic(err)
		}
		for {
			cur := maxDispatched.Load()
			if int64(i) <= cur || maxDispatched.CompareAndSwap(cur, int64(i)) {
				break
			}
		}
		return Command{}, nil
	})
	defer w.stop()
	// Consume only the first 10, then stop: nothing far past 10 may have been dispatched.
	for i := range 10 {
		w.result(i)
	}
	w.stop()
	if got := maxDispatched.Load(); got >= int64(10+2*workers+workers) {
		t.Fatalf("dispatched up to candidate %d with consumer at 10; lookahead not bounded", got)
	}
}

// The consumer discarding candidates it skips must keep the walk moving.
func TestWalkerDiscardAdvancesWindow(t *testing.T) {
	candidates := walkCandidates(40)
	gate := newWalkGate(map[string]int{"np": 40}, passAll)
	w := startCandidateWalker(context.Background(), candidates, 2, gate, func(_ context.Context, _ *Candidate) (Command, error) {
		return Command{}, nil
	})
	defer w.stop()
	for i := range candidates {
		if i%2 == 0 {
			w.discard(i)
			continue
		}
		w.result(i)
	}
}

// stop must cancel in-flight simulations and return once all workers exit.
func TestWalkerStopCancelsInFlightWork(t *testing.T) {
	candidates := walkCandidates(20)
	gate := newWalkGate(map[string]int{"np": 20}, passAll)
	started := make(chan struct{}, 20)
	w := startCandidateWalker(context.Background(), candidates, 4, gate, func(ctx context.Context, _ *Candidate) (Command, error) {
		started <- struct{}{}
		<-ctx.Done()
		return Command{}, ctx.Err()
	})
	<-started
	done := make(chan struct{})
	go func() {
		w.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not return; workers leaked")
	}
	// Idempotent, and safe on a nil walker (the serial path).
	w.stop()
	var nilWalker *candidateWalker
	nilWalker.stop()
	nilWalker.discard(0)
}

// Cancellation of the parent context must unblock a consumer waiting on a result.
func TestWalkerResultUnblocksOnParentCancellation(t *testing.T) {
	candidates := walkCandidates(4)
	gate := newWalkGate(map[string]int{"np": 4}, passAll)
	ctx, cancel := context.WithCancel(context.Background())
	w := startCandidateWalker(ctx, candidates, 2, gate, func(ctx context.Context, _ *Candidate) (Command, error) {
		<-ctx.Done()
		return Command{}, ctx.Err()
	})
	defer w.stop()
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	res := w.result(0)
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("result err = %v, want context.Canceled", res.err)
	}
}
