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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestInverseTopologyGroupKeyGrouping(t *testing.T) {
	namespaces := sets.New("default")
	selector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"app": "a", "tier": "web"},
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"prod", "staging"}},
			{Key: "region", Operator: metav1.LabelSelectorOpExists},
		},
	}
	base := inverseTopologyGroupKey("zone", namespaces, selector)

	// Expression order and value order must not change the identity.
	reordered := &metav1.LabelSelector{
		MatchLabels: map[string]string{"tier": "web", "app": "a"},
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "region", Operator: metav1.LabelSelectorOpExists},
			{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"staging", "prod"}},
		},
	}
	if got := inverseTopologyGroupKey("zone", namespaces, reordered); got != base {
		t.Fatalf("expected reordered selector to share the key, got %q vs %q", got, base)
	}

	// Repeated expressions collapse, mirroring hashSelector's set semantics.
	duplicated := reordered.DeepCopy()
	duplicated.MatchExpressions = append(duplicated.MatchExpressions, metav1.LabelSelectorRequirement{Key: "region", Operator: metav1.LabelSelectorOpExists})
	if got := inverseTopologyGroupKey("zone", namespaces, duplicated); got != base {
		t.Fatalf("expected duplicated expression to collapse, got %q vs %q", got, base)
	}

	// Different topology key, namespaces, or selector content are different groups.
	if got := inverseTopologyGroupKey("hostname", namespaces, selector); got == base {
		t.Fatal("expected a different topology key to change the identity")
	}
	if got := inverseTopologyGroupKey("zone", sets.New("other"), selector); got == base {
		t.Fatal("expected different namespaces to change the identity")
	}
	changed := selector.DeepCopy()
	changed.MatchLabels["app"] = "b"
	if got := inverseTopologyGroupKey("zone", namespaces, changed); got == base {
		t.Fatal("expected different selector content to change the identity")
	}

	// A nil selector (matches nothing) must be distinct from an empty one (matches everything).
	if inverseTopologyGroupKey("zone", namespaces, nil) == inverseTopologyGroupKey("zone", namespaces, &metav1.LabelSelector{}) {
		t.Fatal("expected nil and empty selectors to have distinct identities")
	}
}

func TestInverseAffinityCacheLookups(t *testing.T) {
	kubeClient := fakecr.NewClientBuilder().Build()
	topology := &Topology{kubeClient: kubeClient}
	ctx := WithInverseAffinityCache(context.Background(), NewInverseAffinityCache())

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod-1", UID: "uid-1", ResourceVersion: "1"}}
	term := corev1.PodAffinityTerm{TopologyKey: "zone", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}}

	key1, ns1, err := topology.inverseGroupKeyAndNamespaces(ctx, pod, 0, term)
	if err != nil || !ns1.Equal(sets.New("default")) {
		t.Fatalf("unexpected first resolution: %v %v", ns1, err)
	}
	key2, ns2, err := topology.inverseGroupKeyAndNamespaces(ctx, pod, 0, term)
	if err != nil || key2 != key1 {
		t.Fatalf("expected cached key on repeat, got %q vs %q (%v)", key2, key1, err)
	}
	if !ns2.Equal(ns1) {
		t.Fatalf("expected cached namespaces on repeat, got %v vs %v", ns2, ns1)
	}

	// A pod update (new resource version) is a new entry.
	updated := pod.DeepCopy()
	updated.ResourceVersion = "2"
	if key3, _, err := topology.inverseGroupKeyAndNamespaces(ctx, updated, 0, term); err != nil || key3 != key1 {
		t.Fatalf("expected identical term content to produce the same identity, got %q vs %q (%v)", key3, key1, err)
	}

	// Distinct terms on the same pod are distinct entries.
	other := corev1.PodAffinityTerm{TopologyKey: "hostname", LabelSelector: term.LabelSelector}
	keyOther, _, err := topology.inverseGroupKeyAndNamespaces(ctx, pod, 1, other)
	if err != nil || keyOther == key1 {
		t.Fatalf("expected a distinct identity for a distinct term, got %q (%v)", keyOther, err)
	}
}

func TestInverseAffinityCacheBypasses(t *testing.T) {
	topology := &Topology{kubeClient: fakecr.NewClientBuilder().Build()}
	ctx := WithInverseAffinityCache(context.Background(), NewInverseAffinityCache())

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod-1", UID: "uid-1", ResourceVersion: "1"}}
	term := corev1.PodAffinityTerm{TopologyKey: "zone", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}}
	key1, _, err := topology.inverseGroupKeyAndNamespaces(ctx, pod, 0, term)
	if err != nil {
		t.Fatal(err)
	}

	// Without UID or resource version the cache is bypassed but resolution still works.
	anonymous := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod-2"}}
	keyAnon, _, err := topology.inverseGroupKeyAndNamespaces(ctx, anonymous, 0, term)
	if err != nil || keyAnon != key1 {
		t.Fatalf("expected bypass to resolve the same identity, got %q vs %q (%v)", keyAnon, key1, err)
	}

	// Without a cache in context, resolution goes straight through.
	keyFresh, _, err := topology.inverseGroupKeyAndNamespaces(context.Background(), pod, 0, term)
	if err != nil || keyFresh != key1 {
		t.Fatalf("expected uncached resolution to match, got %q vs %q (%v)", keyFresh, key1, err)
	}
}

func TestInverseAffinityCachePinsNamespaceReads(t *testing.T) {
	kubeClient := fakecr.NewClientBuilder().WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a", Labels: map[string]string{"team": "yes"}}},
	).Build()
	topology := &Topology{kubeClient: kubeClient}
	ctx := WithInverseAffinityCache(context.Background(), NewInverseAffinityCache())

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod-1", UID: "uid-1", ResourceVersion: "1"}}
	term := corev1.PodAffinityTerm{
		TopologyKey:       "zone",
		LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}},
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "yes"}},
	}

	_, first, err := topology.inverseGroupKeyAndNamespaces(ctx, pod, 0, term)
	if err != nil || !first.Equal(sets.New("team-a")) {
		t.Fatalf("unexpected first namespace resolution: %v %v", first, err)
	}

	// A namespace created after the first read must not appear: the cache pins the pass's view.
	if err := kubeClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b", Labels: map[string]string{"team": "yes"}}}); err != nil {
		t.Fatal(err)
	}
	_, second, err := topology.inverseGroupKeyAndNamespaces(ctx, pod, 0, term)
	if err != nil || !second.Equal(sets.New("team-a")) {
		t.Fatalf("expected pinned namespaces within the pass, got %v %v", second, err)
	}
}

// BenchmarkUpdateInverseAntiAffinity models what one consolidation pass does: every candidate
// simulation rebuilds Topology, which re-processes every anti-affinity pod in the cluster.
func BenchmarkUpdateInverseAntiAffinity(b *testing.B) {
	kubeClient := fakecr.NewClientBuilder().Build()
	pods := make([]*corev1.Pod, 500)
	for i := range pods {
		pods[i] = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: fmt.Sprintf("pod-%d", i), UID: types.UID(fmt.Sprintf("uid-%d", i)), ResourceVersion: "1"},
			Spec: corev1.PodSpec{Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					TopologyKey:   corev1.LabelHostname,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": fmt.Sprintf("app-%d", i%25)}},
				}},
			}}},
		}
	}
	domains := map[string]string{corev1.LabelHostname: "node-1"}
	for _, cached := range []bool{false, true} {
		ctx := context.Background()
		if cached {
			ctx = WithInverseAffinityCache(ctx, NewInverseAffinityCache())
		}
		b.Run(map[bool]string{false: "uncached", true: "cached"}[cached], func(b *testing.B) {
			for b.Loop() {
				topology := &Topology{kubeClient: kubeClient, domainGroups: map[string]TopologyDomainGroup{}, inverseTopologyGroups: map[string]*TopologyGroup{}}
				for _, pod := range pods {
					if err := topology.updateInverseAntiAffinity(ctx, pod, domains); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
