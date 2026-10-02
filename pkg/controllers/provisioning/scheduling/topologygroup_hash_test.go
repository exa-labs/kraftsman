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

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func spreadGroupFor(spec corev1.PodSpec) *TopologyGroup {
	owner := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default"}, Spec: spec}
	return NewTopologyGroup(TopologyTypeSpread, corev1.LabelTopologyZone, owner, sets.New("default"),
		&metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}, 1, nil, nil, nil, NewTopologyDomainGroup())
}

func requiredAffinity(terms ...[]corev1.NodeSelectorRequirement) *corev1.Affinity {
	nodeSelectorTerms := make([]corev1.NodeSelectorTerm, 0, len(terms))
	for _, term := range terms {
		nodeSelectorTerms = append(nodeSelectorTerms, corev1.NodeSelectorTerm{MatchExpressions: term})
	}
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: nodeSelectorTerms},
	}}
}

// TestTopologyGroupHashCoversNodeFilterValues checks that the group hash distinguishes node filters
// whose requirements differ only in values, operators or bounds, and stays equal for filters that
// select the same nodes.
func TestTopologyGroupHashCoversNodeFilterValues(t *testing.T) {
	familyA := []corev1.NodeSelectorRequirement{{Key: "family", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}}
	familyB := []corev1.NodeSelectorRequirement{{Key: "family", Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}}
	notFamilyA := []corev1.NodeSelectorRequirement{{Key: "family", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"a"}}}
	gt4 := []corev1.NodeSelectorRequirement{{Key: "gen", Operator: corev1.NodeSelectorOpGt, Values: []string{"4"}}}
	gt6 := []corev1.NodeSelectorRequirement{{Key: "gen", Operator: corev1.NodeSelectorOpGt, Values: []string{"6"}}}

	different := []struct {
		name string
		a, b corev1.PodSpec
	}{
		{"nodeSelector value", corev1.PodSpec{NodeSelector: map[string]string{"family": "a"}}, corev1.PodSpec{NodeSelector: map[string]string{"family": "b"}}},
		{"affinity value", corev1.PodSpec{Affinity: requiredAffinity(familyA)}, corev1.PodSpec{Affinity: requiredAffinity(familyB)}},
		{"affinity operator", corev1.PodSpec{Affinity: requiredAffinity(familyA)}, corev1.PodSpec{Affinity: requiredAffinity(notFamilyA)}},
		{"affinity bound", corev1.PodSpec{Affinity: requiredAffinity(gt4)}, corev1.PodSpec{Affinity: requiredAffinity(gt6)}},
		{"extra term", corev1.PodSpec{Affinity: requiredAffinity(familyA)}, corev1.PodSpec{Affinity: requiredAffinity(familyA, familyB)}},
	}
	for _, tc := range different {
		if spreadGroupFor(tc.a).Hash() == spreadGroupFor(tc.b).Hash() {
			t.Errorf("%s: groups with different node filters hash equally", tc.name)
		}
	}

	same := []struct {
		name string
		a, b corev1.PodSpec
	}{
		{"identical nodeSelector", corev1.PodSpec{NodeSelector: map[string]string{"family": "a"}}, corev1.PodSpec{NodeSelector: map[string]string{"family": "a"}}},
		{"term order", corev1.PodSpec{Affinity: requiredAffinity(familyA, familyB)}, corev1.PodSpec{Affinity: requiredAffinity(familyB, familyA)}},
		{"value order", corev1.PodSpec{Affinity: requiredAffinity([]corev1.NodeSelectorRequirement{{Key: "family", Operator: corev1.NodeSelectorOpIn, Values: []string{"a", "b"}}})},
			corev1.PodSpec{Affinity: requiredAffinity([]corev1.NodeSelectorRequirement{{Key: "family", Operator: corev1.NodeSelectorOpIn, Values: []string{"b", "a"}}})}},
	}
	for _, tc := range same {
		if spreadGroupFor(tc.a).Hash() != spreadGroupFor(tc.b).Hash() {
			t.Errorf("%s: groups with equivalent node filters hash differently", tc.name)
		}
	}
}

// TestTopologyCountCacheSeparatesNodeFilters is the regression for count-cache replays across
// groups that share a selector but filter different nodes: with the cache on, the second group
// must count its own nodes, not replay the first group's records.
func TestTopologyCountCacheSeparatesNodeFilters(t *testing.T) {
	nodeA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelTopologyZone: "zone-1", "family": "a"}}}
	nodeB := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelTopologyZone: "zone-2", "family": "b"}}}
	podA := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "default", UID: "uid-a", Labels: map[string]string{"app": "a"}}, Spec: corev1.PodSpec{NodeName: "node-a"}}
	podB := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-b", Namespace: "default", UID: "uid-b", Labels: map[string]string{"app": "a"}}, Spec: corev1.PodSpec{NodeName: "node-b"}}
	kubeClient := fakecr.NewClientBuilder().WithObjects(nodeA, nodeB, podA, podB).Build()

	ctx := karpopts.ToContext(WithTopologyPassCache(context.Background(), NewTopologyPassCache()),
		test.Options(test.OptionsFields{TopologyCountCacheMode: toPtr(karpopts.TopologyCountCacheModeOn)}))

	for _, family := range []string{"a", "b"} {
		records, err := topologyPodRecords(ctx, kubeClient, spreadGroupFor(corev1.PodSpec{NodeSelector: map[string]string{"family": family}}))
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || string(records[0].uid) != "uid-"+family {
			t.Fatalf("family=%s: got records %+v, want only uid-%s", family, records, family)
		}
	}
}

func spreadGroupWithSelector(spec corev1.PodSpec, selector *metav1.LabelSelector) *TopologyGroup {
	owner := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default"}, Spec: spec}
	return NewTopologyGroup(TopologyTypeSpread, corev1.LabelTopologyZone, owner, sets.New("default"), selector, 1, nil,
		lo.ToPtr(corev1.NodeInclusionPolicyHonor), nil, NewTopologyDomainGroup())
}

// TestTopologyGroupHashDuplicatesDoNotCancel: a selector's values, a pod's tolerations and its
// required node affinity terms are sets, and repeating an entry must not make it cancel out of the
// group hash. Hashing them with SlicesAsSets combines entries by XOR, so a repeated entry erased
// itself and `app In [x, x, y]` hashed like `app In [y]`.
func TestTopologyGroupHashDuplicatesDoNotCancel(t *testing.T) {
	in := func(values ...string) *metav1.LabelSelector {
		return &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: values}}}
	}
	dedicated := corev1.Toleration{Key: "dedicated", Operator: corev1.TolerationOpExists}
	gpu := corev1.Toleration{Key: "gpu", Operator: corev1.TolerationOpExists}
	familyA := []corev1.NodeSelectorRequirement{{Key: "family", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}}
	familyB := []corev1.NodeSelectorRequirement{{Key: "family", Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}}
	matchA := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}

	for _, tc := range []struct {
		name     string
		a, b     *TopologyGroup
		sameHash bool
	}{
		{"repeated selector value", spreadGroupWithSelector(corev1.PodSpec{}, in("x", "x", "y")), spreadGroupWithSelector(corev1.PodSpec{}, in("y")), false},
		{"repeated selector value is the same set", spreadGroupWithSelector(corev1.PodSpec{}, in("x", "x", "y")), spreadGroupWithSelector(corev1.PodSpec{}, in("y", "x")), true},
		{"repeated toleration", spreadGroupWithSelector(corev1.PodSpec{Tolerations: []corev1.Toleration{dedicated, dedicated, gpu}}, matchA),
			spreadGroupWithSelector(corev1.PodSpec{Tolerations: []corev1.Toleration{gpu}}, matchA), false},
		{"repeated toleration is the same set", spreadGroupWithSelector(corev1.PodSpec{Tolerations: []corev1.Toleration{dedicated, dedicated, gpu}}, matchA),
			spreadGroupWithSelector(corev1.PodSpec{Tolerations: []corev1.Toleration{gpu, dedicated}}, matchA), true},
		{"repeated affinity term", spreadGroupWithSelector(corev1.PodSpec{Affinity: requiredAffinity(familyA, familyA, familyB)}, matchA),
			spreadGroupWithSelector(corev1.PodSpec{Affinity: requiredAffinity(familyB)}, matchA), false},
		{"repeated affinity term is the same set", spreadGroupWithSelector(corev1.PodSpec{Affinity: requiredAffinity(familyA, familyA, familyB)}, matchA),
			spreadGroupWithSelector(corev1.PodSpec{Affinity: requiredAffinity(familyB, familyA)}, matchA), true},
	} {
		if got := tc.a.Hash() == tc.b.Hash(); got != tc.sameHash {
			t.Errorf("%s: equal hashes = %t, want %t", tc.name, got, tc.sameHash)
		}
	}
}

// TestTopologyCountCacheSeparatesRepeatedSelectorValues is the count-cache consequence: with the
// cache on, a group selecting `app In [y]` must not replay the records of a group selecting
// `app In [x, x, y]` scanned earlier in the pass.
func TestTopologyCountCacheSeparatesRepeatedSelectorValues(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", Labels: map[string]string{corev1.LabelTopologyZone: "zone-1"}}}
	podX := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-x", Namespace: "default", UID: "uid-x", Labels: map[string]string{"app": "x"}}, Spec: corev1.PodSpec{NodeName: "node-1"}}
	podY := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-y", Namespace: "default", UID: "uid-y", Labels: map[string]string{"app": "y"}}, Spec: corev1.PodSpec{NodeName: "node-1"}}
	kubeClient := fakecr.NewClientBuilder().WithObjects(node, podX, podY).Build()
	ctx := karpopts.ToContext(WithTopologyPassCache(context.Background(), NewTopologyPassCache()),
		test.Options(test.OptionsFields{TopologyCountCacheMode: toPtr(karpopts.TopologyCountCacheModeOn)}))

	in := func(values ...string) *metav1.LabelSelector {
		return &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: values}}}
	}
	if records, err := topologyPodRecords(ctx, kubeClient, spreadGroupWithSelector(corev1.PodSpec{}, in("x", "x", "y"))); err != nil || len(records) != 2 {
		t.Fatalf("app In [x, x, y]: got %+v, %v; want both pods", records, err)
	}
	records, err := topologyPodRecords(ctx, kubeClient, spreadGroupWithSelector(corev1.PodSpec{}, in("y")))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].uid != "uid-y" {
		t.Fatalf("app In [y]: got records %+v, want only uid-y", records)
	}
}

func BenchmarkTopologyGroupHash(b *testing.B) {
	tg := spreadGroupWithSelector(corev1.PodSpec{
		Tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}, {Key: "gpu", Operator: corev1.TolerationOpExists}},
		Affinity:    requiredAffinity([]corev1.NodeSelectorRequirement{{Key: "family", Operator: corev1.NodeSelectorOpIn, Values: []string{"a", "b"}}}),
	}, &metav1.LabelSelector{
		MatchLabels:      map[string]string{"app": "a", "tier": "web"},
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "rev", Operator: metav1.LabelSelectorOpIn, Values: []string{"1", "2", "3"}}},
	})
	for b.Loop() {
		tg.Hash()
	}
}
