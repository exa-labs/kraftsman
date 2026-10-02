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

// Tests for how the priced placement path evaluates in-flight NodeClaims: which claims it evaluates for a pod and
// that the claim it picks matches an exhaustive evaluation of every claim.

package scheduling

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// sizedInstanceType is an instance type with cpu cores and room for 100 pods, offered as spot in one zone at price.
func sizedInstanceType(name string, cpu int, price float64) *cloudprovider.InstanceType {
	return fake.NewInstanceType(name,
		fake.WithResources(corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(fmt.Sprint(cpu)),
			corev1.ResourceMemory: resource.MustParse("64Gi"),
			corev1.ResourcePods:   resource.MustParse("100"),
		}),
		fake.WithOfferings(offering(v1.CapacityTypeSpot, price, true)),
	)
}

// packingNodePool is a NodePool with the given packing policy annotation, empty for none.
func packingNodePool(name string, policy PackingPolicy) *v1.NodePool {
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}})
	if policy != "" {
		np.Annotations = map[string]string{v1.NodePoolPackingPolicyAnnotationKey: string(policy)}
	}
	return np
}

// cpuPod is a pod requesting 1.5 cores, pinned to nodePool when it is not empty.
func cpuPod(name string, nodePool string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:      "main",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m")}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	if nodePool != "" {
		pod.Spec.NodeSelector = map[string]string{v1.NodePoolLabelKey: nodePool}
	}
	return pod
}

// newPackingTestScheduler builds a scheduler over an empty cluster in which every NodePool offers instanceTypes.
func newPackingTestScheduler(t *testing.T, nodePools []*v1.NodePool, instanceTypes []*cloudprovider.InstanceType, pods []*corev1.Pod) (context.Context, *Scheduler) {
	t.Helper()
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	client := fakecr.NewFakeClient()
	provider := fake.NewCloudProvider()
	provider.InstanceTypes = instanceTypes
	cluster := state.NewCluster(clock.RealClock{}, client, provider)
	byNodePool := lo.SliceToMap(nodePools, func(np *v1.NodePool) (string, []*cloudprovider.InstanceType) { return np.Name, instanceTypes })
	topology, err := NewTopology(ctx, client, cluster, nil, nodePools, byNodePool, pods)
	if err != nil {
		t.Fatalf("creating topology: %v", err)
	}
	s := NewScheduler(ctx, client, nodePools, cluster, nil, topology, byNodePool, nil,
		events.NewRecorder(&record.FakeRecorder{}), clock.RealClock{}, nil, nil, NumConcurrentReconciles(4))
	return ctx, s
}

// inflightFixture solves three one-pod NodeClaims - two of a marginal-cost NodePool, one of a binpack NodePool - and
// returns the scheduler, a pod that fits all three, and the claims by role. Each 1.5-core pod fills the 2-core size, so
// growing a marginal-cost claim by another pod steps it up to the 8-core size and costs $9 over its $1.
func inflightFixture(t *testing.T) (ctx context.Context, s *Scheduler, pod *corev1.Pod, pricedA, flat, pricedB *NodeClaim) {
	t.Helper()
	priced, binpack := packingNodePool("priced", PackingPolicyMarginalCost), packingNodePool("flat", PackingPolicyBinpack)
	seed := []*corev1.Pod{cpuPod("priced-a", priced.Name), cpuPod("flat", binpack.Name), cpuPod("priced-b", priced.Name)}
	pod = cpuPod("incoming", "")
	ctx, s = newPackingTestScheduler(t, []*v1.NodePool{priced, binpack},
		[]*cloudprovider.InstanceType{sizedInstanceType("small", 2, 1), sizedInstanceType("large", 8, 10)}, append(seed, pod))
	results, err := s.Solve(ctx, seed)
	if err != nil || len(results.PodErrors) != 0 {
		t.Fatalf("solving the seed pods: %v %v", err, results.PodErrors)
	}
	byPod := map[string]*NodeClaim{}
	for _, nc := range results.NewNodeClaims {
		if len(nc.Pods) != 1 {
			t.Fatalf("expected one pod per seeded NodeClaim, got %d", len(nc.Pods))
		}
		byPod[nc.Pods[0].Name] = nc
	}
	if len(byPod) != 3 {
		t.Fatalf("expected three seeded NodeClaims, got %d", len(byPod))
	}
	s.updateCachedPodData(ctx, pod)
	return ctx, s, pod, byPod["priced-a"], byPod["flat"], byPod["priced-b"]
}

// exhaustivePlacements evaluates every in-flight NodeClaim for pod, the reference inflightPlacements must agree with.
func exhaustivePlacements(ctx context.Context, s *Scheduler, pod *corev1.Pod) []*inflightPlacement {
	return lo.Map(s.newNodeClaims, func(nc *NodeClaim, _ int) *inflightPlacement {
		r, its, ofr, result, err := nc.CanAdd(ctx, pod, s.cachedPodData[pod.UID], false, s.allocator)
		if err != nil {
			return nil
		}
		p := &inflightPlacement{placement: placement{nodeClaim: nc, requirements: r, instanceTypes: its, offeringsToReserve: ofr, allocationResult: result}}
		if s.marginalCostPrices(nc) {
			p.delta, p.unpriced = marginalLaunchPrice(nc, its, r, ofr)
		}
		return p
	})
}

// cheapestOf applies cheapestInflightPlacement's selection rule to already evaluated placements.
func cheapestOf(placements []*inflightPlacement) *inflightPlacement {
	var best *inflightPlacement
	for _, p := range placements {
		if p != nil && (best == nil || cheaperThan(p.delta, best.delta)) {
			best = p
		}
	}
	return best
}

// firstZeroDelta is the index of the first placement that takes the pod at zero delta, or -1 when none does.
func firstZeroDelta(placements []*inflightPlacement) int {
	return lo.IndexOf(lo.Map(placements, func(p *inflightPlacement, _ int) bool { return p != nil && p.delta == 0 }), true)
}

// checkPlacementsAgainstReference fails unless got matches reference on every claim up to and including the first
// zero-delta claim and holds nothing after it.
func checkPlacementsAgainstReference(t *testing.T, label string, got, reference []*inflightPlacement) {
	t.Helper()
	zero := firstZeroDelta(reference)
	for i := range reference {
		evaluated := zero < 0 || i <= zero
		switch {
		case !evaluated && got[i] != nil:
			t.Fatalf("%s: claim %d past the first zero-delta claim was returned", label, i)
		case evaluated && (got[i] == nil) != (reference[i] == nil):
			t.Fatalf("%s: claim %d evaluated differently from the reference", label, i)
		case evaluated && got[i] != nil && (got[i].delta != reference[i].delta || got[i].unpriced != reference[i].unpriced):
			t.Fatalf("%s: claim %d priced %v, reference %v", label, i, got[i].delta, reference[i].delta)
		}
	}
}

// sameNodeClaim reports whether two placements, either possibly nil, are onto the same NodeClaim.
func sameNodeClaim(a, b *inflightPlacement) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.nodeClaim == b.nodeClaim
}

func TestInflightPlacementsStopAtTheFirstZeroDeltaClaim(t *testing.T) {
	ctx, s, pod, pricedA, flat, pricedB := inflightFixture(t)
	s.newNodeClaims = []*NodeClaim{pricedA, flat, pricedB}

	reference := exhaustivePlacements(ctx, s, pod)
	deltas := lo.Map(reference, func(p *inflightPlacement, _ int) float64 {
		if p == nil {
			return -1
		}
		return p.delta
	})
	if !slices.Equal(deltas, []float64{9, 0, 9}) {
		t.Fatalf("the incoming pod must fit every seeded NodeClaim at deltas [9 0 9], got %v", deltas)
	}
	got := s.inflightPlacements(ctx, pod)
	if got[2] != nil {
		t.Fatal("a claim after the first zero-delta claim cannot win and must not be evaluated")
	}
	checkPlacementsAgainstReference(t, "[priced flat priced]", got, reference)
	if !sameNodeClaim(s.cheapestInflightPlacement(ctx, pod), cheapestOf(reference)) {
		t.Fatal("the cheapest placement must match an exhaustive evaluation")
	}
	firstFit, cheapest := s.firstAndCheapestInflight(ctx, pod)
	if firstFit.nodeClaim != pricedA || cheapest.nodeClaim != flat {
		t.Fatalf("expected first fit %s and cheapest %s, got %s and %s", pricedA.Name, flat.Name, firstFit.nodeClaim.Name, cheapest.nodeClaim.Name)
	}
}

func TestInflightPlacementsMatchAnExhaustiveEvaluationInEveryOrder(t *testing.T) {
	ctx, s, pod, pricedA, flat, pricedB := inflightFixture(t)
	for _, order := range [][]*NodeClaim{
		{pricedA, flat, pricedB},
		{pricedA, pricedB, flat},
		{flat, pricedA, pricedB},
		{pricedB, pricedA},
		{pricedA},
		{},
	} {
		s.newNodeClaims = order
		label := fmt.Sprint(lo.Map(order, func(nc *NodeClaim, _ int) string { return nc.NodePoolName }))
		reference := exhaustivePlacements(ctx, s, pod)
		checkPlacementsAgainstReference(t, label, s.inflightPlacements(ctx, pod), reference)
		if !sameNodeClaim(s.cheapestInflightPlacement(ctx, pod), cheapestOf(reference)) {
			t.Fatalf("%s: cheapest placement differs from the reference", label)
		}
	}
}
