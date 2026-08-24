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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func cursorConsolidation(evaluated ...string) *SingleNodeConsolidation {
	return &SingleNodeConsolidation{evaluatedThisCycle: sets.New(evaluated...)}
}

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

func providerIDs(candidates []*Candidate) []string {
	out := make([]string, len(candidates))
	for i, c := range candidates {
		out[i] = c.ProviderID()
	}
	return out
}

func expectOrder(t *testing.T, got []*Candidate, want ...string) {
	t.Helper()
	ids := providerIDs(got)
	if len(ids) != len(want) {
		t.Fatalf("got %d candidates %v, want %d %v", len(ids), ids, len(want), want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("order = %v, want %v", ids, want)
		}
	}
}

// A fresh cycle leaves the sorted order untouched.
func TestCursorFreshCycleKeepsSortedOrder(t *testing.T) {
	s := cursorConsolidation()
	candidates := walkCandidates(4)
	expectOrder(t, s.resumeCoverageCycle(context.Background(), candidates), "provider-0", "provider-1", "provider-2", "provider-3")
}

// Candidates the cycle already reached move behind the unreached ones; relative order is
// preserved within each half.
func TestCursorMovesReachedCandidatesBehindUnreached(t *testing.T) {
	s := cursorConsolidation("provider-0", "provider-2")
	candidates := walkCandidates(5)
	expectOrder(t, s.resumeCoverageCycle(context.Background(), candidates), "provider-1", "provider-3", "provider-4", "provider-0", "provider-2")
}

// Once every live candidate has been reached, the cycle resets and the next walk starts back at
// the head of the sorted order.
func TestCursorResetsAfterFullCoverage(t *testing.T) {
	s := cursorConsolidation("provider-0", "provider-1", "provider-2")
	candidates := walkCandidates(3)
	expectOrder(t, s.resumeCoverageCycle(context.Background(), candidates), "provider-0", "provider-1", "provider-2")
	if s.evaluatedThisCycle.Len() != 0 {
		t.Fatalf("cycle not reset: %v", sets.List(s.evaluatedThisCycle))
	}
}

// Candidates that left the candidate set drop out of the cycle: they neither pin the cycle open
// nor affect ordering of the live candidates.
func TestCursorPrunesDisappearedCandidates(t *testing.T) {
	s := cursorConsolidation("provider-0", "provider-1", "gone-a", "gone-b")
	candidates := walkCandidates(2)
	// Both live candidates were reached, so despite the stale entries the cycle is complete.
	expectOrder(t, s.resumeCoverageCycle(context.Background(), candidates), "provider-0", "provider-1")
	if s.evaluatedThisCycle.Len() != 0 {
		t.Fatalf("stale entries kept the cycle open: %v", sets.List(s.evaluatedThisCycle))
	}
}

// A cycle whose reached set only contains disappeared candidates starts fresh.
func TestCursorResetsWhenAllReachedCandidatesDisappeared(t *testing.T) {
	s := cursorConsolidation("gone-a", "gone-b")
	candidates := walkCandidates(3)
	expectOrder(t, s.resumeCoverageCycle(context.Background(), candidates), "provider-0", "provider-1", "provider-2")
	if s.evaluatedThisCycle.Len() != 0 {
		t.Fatalf("cycle not reset: %v", sets.List(s.evaluatedThisCycle))
	}
}

// Candidates that appear mid-cycle are unreached, so they go to the front of the next walk.
func TestCursorPutsNewCandidatesFirst(t *testing.T) {
	s := cursorConsolidation("provider-0", "provider-1")
	candidates := append(walkCandidates(2), walkCandidate(9, "np"))
	expectOrder(t, s.resumeCoverageCycle(context.Background(), candidates), "provider-9", "provider-0", "provider-1")
}

// Simulating repeatedly timed-out walks that each reach only k candidates: every candidate must
// be reached within ceil(n/k) passes — the tail is never starved.
func TestCursorGuaranteesFullCoverageAcrossTimedOutPasses(t *testing.T) {
	const n, perPass = 10, 3
	s := cursorConsolidation()
	reached := sets.New[string]()
	for pass := 0; pass < (n+perPass-1)/perPass; pass++ {
		ordered := s.resumeCoverageCycle(context.Background(), walkCandidates(n))
		for _, c := range ordered[:perPass] {
			// What ComputeCommands does for each candidate the walk reaches.
			s.evaluatedThisCycle.Insert(c.ProviderID())
			reached.Insert(c.ProviderID())
		}
	}
	for i := range n {
		if id := fmt.Sprintf("provider-%d", i); !reached.Has(id) {
			t.Fatalf("candidate %s never reached across passes: %v", id, sets.List(reached))
		}
	}
}
