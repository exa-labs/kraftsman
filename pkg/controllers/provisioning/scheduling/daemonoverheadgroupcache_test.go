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

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func overheadGroupTestTemplate(fingerprint uint64, valid bool) *NodeClaimTemplate {
	it := fake.NewInstanceType("test-instance-type")
	return &NodeClaimTemplate{
		NodePoolName:          "pool-a",
		InstanceTypeOptions:   []*cloudprovider.InstanceType{it},
		Requirements:          scheduling.NewRequirements(),
		cacheFingerprint:      fingerprint,
		cacheFingerprintValid: valid,
	}
}

func TestDaemonOverheadGroupCacheHitAndInvalidation(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	cache := NewDaemonOverheadCache()
	daemon := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "ds-pod"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			}},
		},
	}
	cache.updateDaemonSetGeneration([]*corev1.Pod{daemon})
	nct := overheadGroupTestTemplate(1, true)

	first := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, []*corev1.Pod{daemon})[nct]
	if len(first) != 1 || first[0].DaemonOverhead.Cpu().MilliValue() != 100 {
		t.Fatalf("unexpected groups: %+v", first)
	}
	second := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, []*corev1.Pod{daemon})[nct]
	if &first[0] != &second[0] {
		t.Fatalf("expected cached groups to be shared across builds")
	}

	// A fingerprint change (NodePool spec or instance type set changed) must invalidate the entry.
	changed := overheadGroupTestTemplate(2, true)
	changed.NodePoolName = "pool-a"
	third := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{changed}, []*corev1.Pod{daemon})[changed]
	if &first[0] == &third[0] {
		t.Fatalf("expected recomputed groups after fingerprint change")
	}

	// A daemonset change must invalidate all entries.
	updatedDaemon := daemon.DeepCopy()
	updatedDaemon.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("200m")
	cache.updateDaemonSetGeneration([]*corev1.Pod{updatedDaemon})
	fourth := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, []*corev1.Pod{updatedDaemon})[nct]
	if fourth[0].DaemonOverhead.Cpu().MilliValue() != 200 {
		t.Fatalf("expected recomputed overhead after daemonset change, got %v", fourth[0].DaemonOverhead)
	}
}

func TestDaemonOverheadGroupCacheBypassesWithoutFingerprint(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	cache := NewDaemonOverheadCache()
	cache.updateDaemonSetGeneration(nil)
	nct := overheadGroupTestTemplate(0, false)

	first := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, nil)[nct]
	second := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, nil)[nct]
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("unexpected groups: %+v %+v", first, second)
	}
	if &first[0] == &second[0] {
		t.Fatalf("expected bypass to recompute groups when the template has no fingerprint")
	}
	if len(cache.overheadGroupsByTemplate) != 0 {
		t.Fatalf("expected nothing cached on bypass")
	}
}

// Split retries interleave a NodePool's price-limited templates with its unlimited one, and those have different
// fingerprints. Both must stay cached or every candidate rebuilds the overhead groups this cache exists to reuse.
func TestDaemonOverheadGroupCacheKeepsInterleavedFingerprints(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	cache := NewDaemonOverheadCache()
	cache.updateDaemonSetGeneration(nil)
	unlimited := overheadGroupTestTemplate(1, true)
	limited := overheadGroupTestTemplate(2, true)

	firstUnlimited := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{unlimited}, nil)[unlimited]
	firstLimited := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{limited}, nil)[limited]
	secondUnlimited := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{unlimited}, nil)[unlimited]
	secondLimited := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{limited}, nil)[limited]

	if &firstUnlimited[0] != &secondUnlimited[0] || &firstLimited[0] != &secondLimited[0] {
		t.Fatalf("expected both fingerprints to stay cached across interleaved builds")
	}
	if &firstUnlimited[0] == &firstLimited[0] {
		t.Fatalf("expected distinct groups per fingerprint")
	}
}

// referenceDaemonPodCompatible is the per-instance-type daemon compatibility check: tolerate the template's taints,
// then relax required node affinity terms until one step is compatible with the template and intersects the
// instance type. buildDaemonOverheadGroupsForTemplate must admit exactly the pods it admits.
func referenceDaemonPodCompatible(nct *NodeClaimTemplate, it *cloudprovider.InstanceType, p *corev1.Pod) bool {
	p = p.DeepCopy()
	preferences := &Preferences{}
	_ = preferences.toleratePreferNoScheduleTaints(p)
	if err := scheduling.Taints(nct.Spec.Taints).ToleratesPod(p); err != nil {
		return false
	}
	for {
		podRequirements := scheduling.NewStrictPodRequirements(p)
		if nct.Requirements.IsCompatible(podRequirements, scheduling.AllowUndefinedWellKnownLabels) &&
			it.Requirements.Intersects(podRequirements) == nil {
			return true
		}
		if preferences.removeRequiredNodeAffinityTerm(p) == nil {
			return false
		}
	}
}

func TestDaemonOverheadGroupsMatchPerInstanceTypeCompatibility(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	its := fake.InstanceTypes(6)
	tainted := &NodeClaimTemplate{
		NodePoolName:        "tainted",
		InstanceTypeOptions: its,
		Requirements:        scheduling.NewRequirements(scheduling.NewRequirement("pool", corev1.NodeSelectorOpIn, "tainted")),
	}
	tainted.Spec.Taints = []corev1.Taint{
		{Key: "dedicated", Value: "sandbox", Effect: corev1.TaintEffectNoSchedule},
		{Key: "soft", Value: "true", Effect: corev1.TaintEffectPreferNoSchedule},
	}
	plain := &NodeClaimTemplate{
		NodePoolName:        "plain",
		InstanceTypeOptions: its,
		Requirements:        scheduling.NewRequirements(scheduling.NewRequirement("pool", corev1.NodeSelectorOpIn, "plain")),
	}
	toleratesSandbox := func(p *corev1.Pod) *corev1.Pod {
		p.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "sandbox", Effect: corev1.TaintEffectNoSchedule}}
		return p
	}
	daemons := []*corev1.Pod{
		daemonPod("everywhere", "10m"),
		toleratesSandbox(daemonPod("sandbox-only", "20m", in("pool", "tainted"))),
		toleratesSandbox(daemonPod("sandbox-small-types", "40m", in("pool", "tainted"), in(corev1.LabelInstanceTypeStable, its[0].Name, its[1].Name))),
		daemonPod("plain-large-types", "80m", in("pool", "plain"), notIn(corev1.LabelInstanceTypeStable, its[0].Name, its[1].Name)),
		// Relaxation: the first term only matches type 2, the second only the plain pool, so the pod
		// lands on type 2 in either pool it tolerates and on every type of the plain pool.
		daemonPodWithTerms("or-terms", "160m",
			[]corev1.NodeSelectorRequirement{in(corev1.LabelInstanceTypeStable, its[2].Name)},
			[]corev1.NodeSelectorRequirement{in("pool", "plain")}),
		daemonPod("nowhere", "320m", in("pool", "other")),
	}
	for _, nct := range []*NodeClaimTemplate{tainted, plain} {
		groups := buildDaemonOverheadGroupsForTemplate(ctx, nct, daemons)
		for _, it := range its {
			var want []*corev1.Pod
			for _, p := range daemons {
				if referenceDaemonPodCompatible(nct, it, p) {
					want = append(want, p)
				}
			}
			wantOverhead, _ := computeDaemonOverhead(candidateRequirements(nct, it), want)
			var got []DaemonOverheadGroup
			for _, g := range groups {
				for _, git := range g.InstanceTypes {
					if git == it {
						got = append(got, g)
					}
				}
			}
			if len(got) != 1 {
				t.Fatalf("%s/%s: expected exactly one group, got %d", nct.NodePoolName, it.Name, len(got))
			}
			if !equality(got[0].DaemonOverhead, wantOverhead) {
				t.Fatalf("%s/%s: overhead %v, want %v (pods %s)", nct.NodePoolName, it.Name, got[0].DaemonOverhead, wantOverhead, podSetKey(want))
			}
		}
	}
}

func equality(a, b corev1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for name, qa := range a {
		qb, ok := b[name]
		if !ok || qa.Cmp(qb) != 0 {
			return false
		}
	}
	return true
}

func TestDaemonOverheadGroupStoreServesLaterPasses(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	store := NewDaemonOverheadGroupStore()
	daemon := daemonPod("ds-pod", "100m")

	firstPass := NewDaemonOverheadCacheWithGroupStore(store)
	firstPass.updateDaemonSetGeneration([]*corev1.Pod{daemon})
	firstTemplate := overheadGroupTestTemplate(1, true)
	first := buildDaemonOverheadGroups(ctx, firstPass, []*NodeClaimTemplate{firstTemplate}, []*corev1.Pod{daemon})[firstTemplate]

	// A later pass resolves its own instance type objects; the stored groups must bind to those.
	secondPass := NewDaemonOverheadCacheWithGroupStore(store)
	secondPass.updateDaemonSetGeneration([]*corev1.Pod{daemon})
	secondTemplate := overheadGroupTestTemplate(1, true)
	second := buildDaemonOverheadGroups(ctx, secondPass, []*NodeClaimTemplate{secondTemplate}, []*corev1.Pod{daemon})[secondTemplate]
	if len(second) != 1 || second[0].InstanceTypes[0] != secondTemplate.InstanceTypeOptions[0] {
		t.Fatalf("expected stored groups rebound to the later pass's instance types, got %+v", second)
	}
	if second[0].InstanceTypes[0] == first[0].InstanceTypes[0] {
		t.Fatalf("expected distinct instance type objects across passes")
	}
	if !equality(second[0].DaemonOverhead, first[0].DaemonOverhead) {
		t.Fatalf("overhead changed across passes: %v vs %v", second[0].DaemonOverhead, first[0].DaemonOverhead)
	}
	if _, ok := secondPass.overheadGroups(secondTemplate.NodePoolName, secondTemplate.cacheFingerprint); !ok {
		t.Fatalf("expected a cross-pass hit to populate the pass cache")
	}

	// A DaemonSet change in a later pass must flush the store.
	updated := daemonPod("ds-pod", "200m")
	thirdPass := NewDaemonOverheadCacheWithGroupStore(store)
	thirdPass.updateDaemonSetGeneration([]*corev1.Pod{updated})
	thirdTemplate := overheadGroupTestTemplate(1, true)
	third := buildDaemonOverheadGroups(ctx, thirdPass, []*NodeClaimTemplate{thirdTemplate}, []*corev1.Pod{updated})[thirdTemplate]
	if third[0].DaemonOverhead.Cpu().MilliValue() != 200 {
		t.Fatalf("expected recomputed overhead after daemonset change, got %v", third[0].DaemonOverhead)
	}
}

func TestDaemonOverheadGroupStoreMissesOnUnknownInstanceType(t *testing.T) {
	store := NewDaemonOverheadGroupStore()
	store.updateDaemonSetGeneration("g", true)
	nct := overheadGroupTestTemplate(1, true)
	store.setOverheadGroups(nct, []DaemonOverheadGroup{{InstanceTypes: nct.InstanceTypeOptions}})
	other := overheadGroupTestTemplate(1, true)
	other.InstanceTypeOptions = []*cloudprovider.InstanceType{fake.NewInstanceType("another-instance-type")}
	if _, ok := store.overheadGroups(other); ok {
		t.Fatalf("expected a miss when a stored instance type is not among the template's options")
	}
}

func TestDaemonOverheadGroupStoreIsBounded(t *testing.T) {
	store := NewDaemonOverheadGroupStore()
	store.updateDaemonSetGeneration("g", true)
	for i := range daemonOverheadGroupStoreMaxEntries + 1 {
		store.setOverheadGroups(overheadGroupTestTemplate(uint64(i), true), nil) //nolint:gosec
	}
	if len(store.entries) > daemonOverheadGroupStoreMaxEntries {
		t.Fatalf("store grew to %d entries", len(store.entries))
	}
}
