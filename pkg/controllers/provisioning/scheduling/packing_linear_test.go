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

// Tests for what the marginal-cost packing policy does to a NodePool whose instance type prices scale linearly with
// size, compared with binpack on the same pods.

package scheduling

import (
	"context"
	"fmt"
	"math"
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

// linearLadder is a doubling ladder of 2, 4, 8 and 16 core instance types priced at $0.10 per core.
func linearLadder() []*cloudprovider.InstanceType {
	return lo.Map([]int{2, 4, 8, 16}, func(cores int, _ int) *cloudprovider.InstanceType {
		return fake.NewInstanceType(fmt.Sprintf("linear-%d", cores),
			fake.WithResources(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(fmt.Sprint(cores)),
				corev1.ResourceMemory: resource.MustParse("64Gi"),
				corev1.ResourcePods:   resource.MustParse("100"),
			}),
			fake.WithOfferings(offering(v1.CapacityTypeSpot, 0.1*float64(cores), true)),
		)
	})
}

// solveLinearLadder places fifteen one-core pods on a single NodePool with the given packing policy, and returns the
// NodeClaims and the sum of their cheapest launch prices. The fake instance types reserve 100m for the kubelet, so a
// node of n cores holds n-1 one-core pods.
func solveLinearLadder(t *testing.T, policy PackingPolicy) ([]*NodeClaim, float64) {
	t.Helper()
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	nodePool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{
		Name:        "linear",
		Annotations: map[string]string{v1.NodePoolPackingPolicyAnnotationKey: string(policy)},
	}})
	pods := lo.Times(15, func(i int) *corev1.Pod {
		name := fmt.Sprintf("pod-%d", i)
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:      "main",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		}
	})
	instanceTypes := linearLadder()
	client := fakecr.NewFakeClient()
	provider := fake.NewCloudProvider()
	provider.InstanceTypes = instanceTypes
	cluster := state.NewCluster(clock.RealClock{}, client, provider)
	byNodePool := map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes}
	topology, err := NewTopology(ctx, client, cluster, nil, []*v1.NodePool{nodePool}, byNodePool, pods)
	if err != nil {
		t.Fatalf("creating topology: %v", err)
	}
	s := NewScheduler(ctx, client, []*v1.NodePool{nodePool}, cluster, nil, topology, byNodePool, nil,
		events.NewRecorder(&record.FakeRecorder{}), clock.RealClock{}, nil, nil)
	results, err := s.Solve(ctx, pods)
	if err != nil || len(results.PodErrors) != 0 {
		t.Fatalf("solving: %v %v", err, results.PodErrors)
	}
	total := 0.0
	for _, nc := range results.NewNodeClaims {
		price, ok := launchPrice(nc.InstanceTypeOptions, nc.Requirements, nc.reservedOfferings)
		if !ok {
			t.Fatalf("NodeClaim %s has no launch price", nc.Name)
		}
		total += price
	}
	return results.NewNodeClaims, total
}

// The step from one size to the next costs as much as the claim already does, while a fresh claim for one pod costs
// the smallest size that fits it, so marginal-cost stops growing a claim at about twice that size. On a linear ladder
// that buys no discount and pays the per-node overhead (here the kubelet reservation) once per extra node: a 4-core
// claim holds three pods, so fifteen pods take five of them where binpack fits all fifteen on one 16-core claim.
func TestMarginalCostSplitsLinearPriceLadders(t *testing.T) {
	binpacked, binpackPrice := solveLinearLadder(t, PackingPolicyBinpack)
	if len(binpacked) != 1 || math.Abs(binpackPrice-1.6) > 1e-9 {
		t.Fatalf("binpack must fit all fifteen pods on one 16-core NodeClaim at $1.60, got %d NodeClaims at $%.2f", len(binpacked), binpackPrice)
	}
	split, marginalPrice := solveLinearLadder(t, PackingPolicyMarginalCost)
	if len(split) != 5 || math.Abs(marginalPrice-2.0) > 1e-9 {
		t.Fatalf("marginal-cost must split into five 4-core NodeClaims at $2.00, got %d NodeClaims at $%.2f", len(split), marginalPrice)
	}
}
