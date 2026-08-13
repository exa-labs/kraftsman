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
	"fmt"
	"math/rand"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// TestTopologyCountReplayMatchesFreshScan is the differential proof behind the count cache: over
// randomized worlds (nodes across zones, tainted nodes, leaked pods bound to deleted nodes,
// terminal pods, several selector shapes and topology types) and several candidates per pass (each
// with its own excluded pod set and its own state-node view), a topology group counted through the
// cached replay must end with exactly the domains and empty domains of one counted by a fresh
// scan. Both sides share one TopologyPassCache so they read the same pinned pod and node lists,
// which is also how the two paths coexist within one pass in shadow mode.
func TestTopologyCountReplayMatchesFreshScan(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec
	t.Logf("seed: %d", seed)

	for iter := 0; iter < 40; iter++ {
		nodes, pods, objs := randomTopologyWorld(rng, iter)
		kubeClient := fakecr.NewClientBuilder().WithObjects(objs...).Build()
		specs := randomGroupSpecs(rng)
		mkGroup := func(spec groupSpec) *TopologyGroup {
			owner := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default"}}
			return NewTopologyGroup(spec.topologyType, spec.key, owner, spec.namespaces, spec.selector, 1, nil, nil, nil, NewTopologyDomainGroup())
		}

		// One pass: candidates share the pass cache; each candidate has its own excluded pods
		// (the pods it would reschedule) and its own state-node view (its node filtered out).
		passCache := NewTopologyPassCache()
		freshCtx := karpopts.ToContext(WithTopologyPassCache(context.Background(), passCache),
			test.Options(test.OptionsFields{TopologyCountCacheMode: toPtr(karpopts.TopologyCountCacheModeOff)}))
		cachedCtx := karpopts.ToContext(WithTopologyPassCache(context.Background(), passCache),
			test.Options(test.OptionsFields{TopologyCountCacheMode: toPtr(karpopts.TopologyCountCacheModeOn)}))

		numCandidates := 1 + rng.Intn(3)
		for c := 0; c < numCandidates; c++ {
			excluded := sets.New[string]()
			for _, pod := range pods {
				if rng.Intn(4) == 0 {
					excluded.Insert(string(pod.UID))
				}
			}
			stateNodes := candidateStateNodes(nodes, rng.Intn(len(nodes)))

			for _, spec := range specs {
				fresh := &Topology{kubeClient: kubeClient, stateNodes: stateNodes, excludedPods: excluded}
				freshGroup := mkGroup(spec)
				if err := fresh.countDomains(freshCtx, freshGroup); err != nil {
					t.Fatalf("seed %d: fresh count: %v", seed, err)
				}

				cached := &Topology{kubeClient: kubeClient, stateNodes: stateNodes, excludedPods: excluded}
				cachedGroup := mkGroup(spec)
				if err := cached.countDomains(cachedCtx, cachedGroup); err != nil {
					t.Fatalf("seed %d: cached count: %v", seed, err)
				}

				expectGroupsEqual(t, seed, freshGroup, cachedGroup)
			}
		}
	}
}

// TestTopologyCountShadowMode proves shadow mode computes the fresh scan, records agreement, and
// still uses the fresh result, and that groups with different hashes never share an entry.
func TestTopologyCountShadowMode(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "pod-1", Namespace: "default", UID: "uid-1", Labels: map[string]string{"app": "a"},
		OwnerReferences: nil,
	}, Spec: corev1.PodSpec{NodeName: "node-1"}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-1",
		Labels: map[string]string{corev1.LabelTopologyZone: "zone-1", corev1.LabelHostname: "node-1"},
	}}
	kubeClient := fakecr.NewClientBuilder().WithObjects(pod, node).Build()
	owner := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default"}}
	mkGroup := func() *TopologyGroup {
		return NewTopologyGroup(TopologyTypeSpread, corev1.LabelTopologyZone, owner, sets.New("default"),
			&metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}, 1, nil, nil, nil, NewTopologyDomainGroup())
	}

	passCache := NewTopologyPassCache()
	ctx := karpopts.ToContext(WithTopologyPassCache(context.Background(), passCache),
		test.Options(test.OptionsFields{TopologyCountCacheMode: toPtr(karpopts.TopologyCountCacheModeShadow)}))

	first, err := topologyPodRecords(ctx, kubeClient, mkGroup())
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].domain != "zone-1" || first[0].uid != "uid-1" {
		t.Fatalf("unexpected records: %+v", first)
	}
	// The entry now exists, so a second shadow scan compares against it and must agree.
	second, err := topologyPodRecords(ctx, kubeClient, mkGroup())
	if err != nil {
		t.Fatal(err)
	}
	if !podDomainRecordsEqual(first, second) {
		t.Fatalf("shadow scan diverged: %+v vs %+v", first, second)
	}
	// A group with a different hash scans on its own.
	other := NewTopologyGroup(TopologyTypeSpread, corev1.LabelHostname, owner, sets.New("default"),
		&metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}, 1, nil, nil, nil, NewTopologyDomainGroup())
	records, err := topologyPodRecords(ctx, kubeClient, other)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].domain != "node-1" {
		t.Fatalf("unexpected records for other group: %+v", records)
	}
}

func toPtr[T any](v T) *T { return &v }

type groupSpec struct {
	topologyType TopologyType
	key          string
	namespaces   sets.Set[string]
	selector     *metav1.LabelSelector
}

// randomTopologyWorld builds nodes with and without taints and topology labels, and pods bound to
// real nodes, to a node that no longer exists, pending, and terminal.
func randomTopologyWorld(rng *rand.Rand, iter int) ([]*corev1.Node, []*corev1.Pod, []client.Object) {
	zones := []string{"zone-1", "zone-2", "zone-3"}
	apps := []string{"a", "b", "c"}
	namespaces := []string{"default", "batch"}

	var objs []client.Object
	var nodes []*corev1.Node
	numNodes := 3 + rng.Intn(6)
	for i := 0; i < numNodes; i++ {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("node-%d-%d", iter, i),
			Labels: map[string]string{
				corev1.LabelTopologyZone: zones[rng.Intn(len(zones))],
				corev1.LabelHostname:     fmt.Sprintf("node-%d-%d", iter, i),
			},
		}}
		if rng.Intn(4) == 0 {
			node.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "special", Effect: corev1.TaintEffectNoSchedule}}
		}
		if rng.Intn(5) == 0 {
			delete(node.Labels, corev1.LabelTopologyZone)
		}
		nodes = append(nodes, node)
		objs = append(objs, node)
	}
	var pods []*corev1.Pod
	numPods := 10 + rng.Intn(30)
	for i := 0; i < numPods; i++ {
		nodeName := nodes[rng.Intn(len(nodes))].Name
		switch rng.Intn(10) {
		case 0:
			nodeName = "" // pending, ignored for topology
		case 1:
			nodeName = fmt.Sprintf("gone-%d", i) // leaked pod on a deleted node
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("pod-%d-%d", iter, i),
				Namespace: namespaces[rng.Intn(len(namespaces))],
				UID:       types.UID(fmt.Sprintf("uid-%d-%d", iter, i)),
				Labels:    map[string]string{"app": apps[rng.Intn(len(apps))]},
			},
			Spec: corev1.PodSpec{NodeName: nodeName},
		}
		if rng.Intn(8) == 0 {
			pod.Status.Phase = corev1.PodSucceeded
		}
		pods = append(pods, pod)
		objs = append(objs, pod)
	}
	return nodes, pods, objs
}

// randomGroupSpecs draws topology group shapes: spread and affinity types, zone and hostname keys,
// single and multi namespace scopes, and selectorless groups. Each candidate builds its groups
// afresh, exactly as Topology.Update does, so the group under test is never shared between paths.
func randomGroupSpecs(rng *rand.Rand) []groupSpec {
	apps := []string{"a", "b", "c"}
	namespaces := []string{"default", "batch"}
	var specs []groupSpec
	numGroups := 1 + rng.Intn(4)
	for i := 0; i < numGroups; i++ {
		spec := groupSpec{
			topologyType: []TopologyType{TopologyTypeSpread, TopologyTypePodAffinity, TopologyTypePodAntiAffinity}[rng.Intn(3)],
			key:          []string{corev1.LabelTopologyZone, corev1.LabelHostname}[rng.Intn(2)],
			namespaces:   sets.New(namespaces[rng.Intn(len(namespaces))]),
			selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": apps[rng.Intn(len(apps))]}},
		}
		if rng.Intn(4) == 0 {
			spec.namespaces = sets.New(namespaces...)
		}
		if rng.Intn(5) == 0 {
			spec.selector = nil
		}
		specs = append(specs, spec)
	}
	return specs
}

// expectGroupsEqual asserts the cached replay produced exactly the fresh scan's domain counts and
// empty-domain set.
func expectGroupsEqual(t *testing.T, seed int64, freshGroup, cachedGroup *TopologyGroup) {
	t.Helper()
	if len(freshGroup.domains) != len(cachedGroup.domains) {
		t.Fatalf("seed %d: domain sets diverged: fresh=%v cached=%v", seed, freshGroup.domains, cachedGroup.domains)
	}
	for domain, count := range freshGroup.domains {
		if cachedGroup.domains[domain] != count {
			t.Fatalf("seed %d: domain %q diverged: fresh=%d cached=%d", seed, domain, count, cachedGroup.domains[domain])
		}
	}
	if !freshGroup.emptyDomains.Equal(cachedGroup.emptyDomains) {
		t.Fatalf("seed %d: empty domains diverged: fresh=%v cached=%v", seed, freshGroup.emptyDomains, cachedGroup.emptyDomains)
	}
}

// candidateStateNodes is one candidate's view of the fleet: every node but its own.
func candidateStateNodes(nodes []*corev1.Node, removed int) []*state.StateNode {
	var stateNodes []*state.StateNode
	for i, node := range nodes {
		if i == removed {
			continue
		}
		sn := state.NewNode()
		sn.Node = node
		sn.NodeClaim = &v1.NodeClaim{}
		stateNodes = append(stateNodes, sn)
	}
	return stateNodes
}
