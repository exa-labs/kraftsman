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

package scheduling

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// TestExistingNodeIngredientsIsolation proves the contract the per-pass ExistingNode ingredient
// reuse rests on: repeated calls for the same node return equal resource lists, and the lists a
// candidate receives are its own, so a simulation that subtracts from its remaining resources
// cannot leak into the next candidate's view of the same node.
func TestExistingNodeIngredientsIsolation(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:            "node-1",
		UID:             "node-uid-1",
		ResourceVersion: "1",
	}, Status: corev1.NodeStatus{
		Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
	}}
	sn := state.NewNode()
	sn.Node = node

	ctx := karpopts.ToContext(context.Background(), test.Options())
	s := &Scheduler{daemonOverheadCache: NewDaemonOverheadCache()}

	taints1, remaining1 := s.existingNodeIngredients(ctx, sn, labelRequirementsForStateNode(s.nodeRequirementsCache, sn), nil)
	_, remaining2 := s.existingNodeIngredients(ctx, sn, labelRequirementsForStateNode(s.nodeRequirementsCache, sn), nil)

	if len(taints1) != 0 {
		t.Fatalf("unexpected taints: %v", taints1)
	}
	for name, want := range remaining1 {
		if got := remaining2[name]; got.Cmp(want) != 0 {
			t.Fatalf("remaining diverged for %s: %s vs %s", name, want.String(), got.String())
		}
	}

	// A candidate's simulation subtracts from its remaining resources in place; a later candidate
	// must still see the untouched base.
	cpu := remaining2[corev1.ResourceCPU]
	cpu.Sub(resource.MustParse("3"))
	remaining2[corev1.ResourceCPU] = cpu
	_, remaining3 := s.existingNodeIngredients(ctx, sn, labelRequirementsForStateNode(s.nodeRequirementsCache, sn), nil)
	if got := remaining3[corev1.ResourceCPU]; got.Cmp(resource.MustParse("4")) != 0 {
		t.Fatalf("mutation leaked between candidates: %s", got.String())
	}

	// Dropping state-derived entries forces a rebuild, which is what every churn-observation
	// point in a pass does before re-simulating.
	s.daemonOverheadCache.DropStateDerived()
	if _, ok := s.daemonOverheadCache.ingredients(mustNodeCacheKey(t, sn)); ok {
		t.Fatal("ingredients survived DropStateDerived")
	}

	// A node without a stable identity must bypass the cache entirely.
	anon := state.NewNode()
	anon.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "anon"}, Status: node.Status}
	_, remainingAnon := s.existingNodeIngredients(ctx, anon, labelRequirementsForStateNode(s.nodeRequirementsCache, anon), nil)
	if got := remainingAnon[corev1.ResourceCPU]; got.Cmp(resource.MustParse("4")) != 0 {
		t.Fatalf("unexpected remaining for uncacheable node: %s", got.String())
	}
	if len(s.daemonOverheadCache.ingredientsByKey) != 0 {
		t.Fatal("uncacheable node left an ingredient entry")
	}
}

func mustNodeCacheKey(t *testing.T, node *state.StateNode) string {
	t.Helper()
	key, ok := nodeCacheKey(node, true)
	if !ok {
		t.Fatal("expected a cacheable node key")
	}
	return key
}
