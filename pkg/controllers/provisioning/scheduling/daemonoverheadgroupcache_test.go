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
	"k8s.io/apimachinery/pkg/api/equality"
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

// storeTestPass starts a pass-scoped cache over store that observed daemons.
func storeTestPass(store *DaemonOverheadGroupStore, daemons []*corev1.Pod) *DaemonOverheadCache {
	c := NewDaemonOverheadCacheWithGroupStore(store)
	c.updateDaemonSetGeneration(daemons)
	return c
}

func TestDaemonOverheadGroupStoreServesLaterPasses(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	store := NewDaemonOverheadGroupStore()
	daemons := []*corev1.Pod{daemonPod("ds-pod", "100m")}

	firstTemplate := overheadGroupTestTemplate(1, true)
	first := buildDaemonOverheadGroups(ctx, storeTestPass(store, daemons), []*NodeClaimTemplate{firstTemplate}, daemons)[firstTemplate]

	// A later pass resolves its own instance type objects; the stored groups must bind to those.
	secondPass := storeTestPass(store, daemons)
	secondTemplate := overheadGroupTestTemplate(1, true)
	groups, outcome, ok := secondPass.overheadGroups(secondTemplate)
	if !ok || outcome != cacheOutcomeHitCrossPass {
		t.Fatalf("expected a cross-pass hit, got ok=%v outcome=%q", ok, outcome)
	}
	if len(groups) != 1 || groups[0].InstanceTypes[0] != secondTemplate.InstanceTypeOptions[0] {
		t.Fatalf("expected stored groups rebound to the later pass's instance types, got %+v", groups)
	}
	if !equality.Semantic.DeepEqual(groups[0].DaemonOverhead, first[0].DaemonOverhead) {
		t.Fatalf("overhead changed across passes: %v vs %v", groups[0].DaemonOverhead, first[0].DaemonOverhead)
	}
	if _, outcome, _ := secondPass.overheadGroups(secondTemplate); outcome != cacheOutcomeHit {
		t.Fatalf("expected a cross-pass hit to populate the pass cache, got %q", outcome)
	}

	// A DaemonSet change in a later pass must flush the store.
	updated := []*corev1.Pod{daemonPod("ds-pod", "200m")}
	thirdTemplate := overheadGroupTestTemplate(1, true)
	third := buildDaemonOverheadGroups(ctx, storeTestPass(store, updated), []*NodeClaimTemplate{thirdTemplate}, updated)[thirdTemplate]
	if third[0].DaemonOverhead.Cpu().MilliValue() != 200 {
		t.Fatalf("expected recomputed overhead after daemonset change, got %v", third[0].DaemonOverhead)
	}
}

// A pass that observed an older DaemonSet set must not publish its groups once another pass moved the store on, or
// every later pass at the new generation would be served the old overhead.
func TestDaemonOverheadGroupStoreIgnoresWritesFromOtherGenerations(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	store := NewDaemonOverheadGroupStore()
	oldDaemons := []*corev1.Pod{daemonPod("ds-pod", "100m")}
	newDaemons := []*corev1.Pod{daemonPod("ds-pod", "200m")}

	stalePass := storeTestPass(store, oldDaemons)
	storeTestPass(store, newDaemons)
	staleTemplate := overheadGroupTestTemplate(1, true)
	buildDaemonOverheadGroups(ctx, stalePass, []*NodeClaimTemplate{staleTemplate}, oldDaemons)

	nct := overheadGroupTestTemplate(1, true)
	if _, outcome, _ := storeTestPass(store, newDaemons).overheadGroups(nct); outcome != cacheOutcomeMiss {
		t.Fatalf("expected the stale pass's groups to be dropped, got %q", outcome)
	}
}

func TestDaemonOverheadGroupStoreMissesUnlessGroupsCoverTheOptionsExactly(t *testing.T) {
	store := NewDaemonOverheadGroupStore()
	store.updateDaemonSetGeneration("g", true)
	nct := overheadGroupTestTemplate(1, true)
	key := overheadGroupsCacheKey(nct.NodePoolName, nct.cacheFingerprint)
	store.setOverheadGroups("g", key, []DaemonOverheadGroup{{InstanceTypes: nct.InstanceTypeOptions}})

	replaced := overheadGroupTestTemplate(1, true)
	replaced.InstanceTypeOptions = []*cloudprovider.InstanceType{fake.NewInstanceType("another-instance-type")}
	extended := overheadGroupTestTemplate(1, true)
	extended.InstanceTypeOptions = append(extended.InstanceTypeOptions, fake.NewInstanceType("another-instance-type"))
	for name, template := range map[string]*NodeClaimTemplate{"stored type missing": replaced, "option in no group": extended} {
		if _, ok := store.overheadGroups("g", key, template); ok {
			t.Errorf("%s: expected a miss", name)
		}
	}
	if _, ok := store.overheadGroups("g", key, overheadGroupTestTemplate(1, true)); !ok {
		t.Errorf("expected a hit for the same options")
	}
}

func TestDaemonOverheadGroupStoreStartsOverWhenFull(t *testing.T) {
	store := NewDaemonOverheadGroupStore()
	store.updateDaemonSetGeneration("g", true)
	templates := make([]*NodeClaimTemplate, daemonOverheadGroupStoreMaxEntries+1)
	for i := range templates {
		templates[i] = overheadGroupTestTemplate(uint64(i), true) //nolint:gosec
		store.setOverheadGroups("g", overheadGroupsCacheKey(templates[i].NodePoolName, templates[i].cacheFingerprint), []DaemonOverheadGroup{{InstanceTypes: templates[i].InstanceTypeOptions}})
	}
	lookup := func(nct *NodeClaimTemplate) bool {
		_, ok := store.overheadGroups("g", overheadGroupsCacheKey(nct.NodePoolName, nct.cacheFingerprint), nct)
		return ok
	}
	if !lookup(templates[len(templates)-1]) {
		t.Fatalf("expected the entry written after the reset to be kept")
	}
	if lookup(templates[0]) {
		t.Fatalf("expected entries written before the reset to be dropped")
	}
}
