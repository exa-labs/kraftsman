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

// Tests for the existing node ordering the scheduler tries nodes in: initialized nodes first, then by name,
// stable for equal keys.

package scheduling

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// randomExistingNodes returns n existing nodes mixing managed initialized, managed uninitialized, unmanaged and
// NodeClaim-only (not yet registered) nodes, with repeated names so stability matters.
func randomExistingNodes(r *rand.Rand, n int) []*ExistingNode {
	nodes := make([]*ExistingNode, n)
	for i := range nodes {
		name := fmt.Sprintf("node-%d", r.Intn(n/2+1))
		labels := map[string]string{}
		switch r.Intn(3) {
		case 0:
			labels[v1.NodePoolLabelKey] = "default"
			labels[v1.NodeInitializedLabelKey] = "true"
		case 1:
			labels[v1.NodePoolLabelKey] = "default"
		}
		sn := &state.StateNode{Node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}}
		if r.Intn(4) == 0 {
			sn = &state.StateNode{NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1.NodePoolLabelKey: "default"}}}}
		}
		nodes[i] = &ExistingNode{StateNode: sn}
	}
	return nodes
}

func TestSortExistingNodesMatchesPerComparisonOrdering(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	r := rand.New(rand.NewSource(seed)) //nolint:gosec
	for range 50 {
		nodes := randomExistingNodes(r, 1+r.Intn(200))
		want := append([]*ExistingNode(nil), nodes...)
		sort.SliceStable(want, func(i, j int) bool {
			if want[i].Initialized() && !want[j].Initialized() {
				return true
			}
			if !want[i].Initialized() && want[j].Initialized() {
				return false
			}
			return want[i].Name() < want[j].Name()
		})
		s := &Scheduler{existingNodes: nodes}
		s.sortExistingNodes()
		for i := range want {
			if s.existingNodes[i] != want[i] {
				t.Fatalf("seed %d: position %d holds %s, want %s", seed, i, s.existingNodes[i].Name(), want[i].Name())
			}
		}
	}
}

func BenchmarkSortExistingNodes(b *testing.B) {
	nodes := randomExistingNodes(rand.New(rand.NewSource(1)), 1400) //nolint:gosec
	s := &Scheduler{existingNodes: make([]*ExistingNode, len(nodes))}
	for b.Loop() {
		copy(s.existingNodes, nodes)
		s.sortExistingNodes()
	}
}
