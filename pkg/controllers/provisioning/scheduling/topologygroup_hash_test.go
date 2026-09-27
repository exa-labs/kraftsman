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
