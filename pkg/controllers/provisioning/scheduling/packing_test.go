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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestPackingPolicyForNodePool(t *testing.T) {
	for _, tc := range []struct {
		annotation *string
		want       PackingPolicy
		wantErr    bool
	}{
		{annotation: nil, want: PackingPolicyBinpack},
		{annotation: ptr(""), want: PackingPolicyBinpack},
		{annotation: ptr("binpack"), want: PackingPolicyBinpack},
		{annotation: ptr("marginal-cost"), want: PackingPolicyMarginalCost},
		{annotation: ptr("Marginal-Cost"), want: PackingPolicyBinpack, wantErr: true},
		{annotation: ptr("cheapest"), want: PackingPolicyBinpack, wantErr: true},
	} {
		np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np"}}
		if tc.annotation != nil {
			np.Annotations = map[string]string{v1.NodePoolPackingPolicyAnnotationKey: *tc.annotation}
		}
		got, err := PackingPolicyForNodePool(np)
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("annotation %v: got (%q, %v), want (%q, err=%v)", tc.annotation, got, err, tc.want, tc.wantErr)
		}
	}
}

func ptr(s string) *string { return &s }

func TestCheaperThan(t *testing.T) {
	if !cheaperThan(1, 2) || cheaperThan(2, 1) {
		t.Fatal("cheaperThan must order distinct prices")
	}
	if cheaperThan(1, 1) || cheaperThan(0, 0) {
		t.Fatal("equal prices must not be cheaper than each other")
	}
	// 0.1+0.2 != 0.3 in binary floating point but is the same price
	if cheaperThan(0.1+0.2, 0.3) || cheaperThan(0.3, 0.1+0.2) {
		t.Fatal("prices within tolerance must tie")
	}
}

// spotAndOnDemand builds an instance type offering the non-zero prices among spot and onDemand.
func spotAndOnDemand(name string, spot, onDemand float64) *cloudprovider.InstanceType {
	var offerings []cloudprovider.Offering
	if spot != 0 {
		offerings = append(offerings, offering(v1.CapacityTypeSpot, spot, true))
	}
	if onDemand != 0 {
		offerings = append(offerings, offering(v1.CapacityTypeOnDemand, onDemand, true))
	}
	return pricedInstanceType(name, offerings...)
}

func TestLaunchPricePrefersSpotThenOnDemandWithinAnInstanceType(t *testing.T) {
	anyCapacity := scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot, v1.CapacityTypeOnDemand))
	price, ok := launchPrice([]*cloudprovider.InstanceType{spotAndOnDemand("a", 3, 1)}, anyCapacity)
	if !ok || price != 3 {
		t.Fatalf("spot must take precedence over on-demand even when dearer, got (%v, %v)", price, ok)
	}
	price, ok = launchPrice([]*cloudprovider.InstanceType{spotAndOnDemand("a", 3, 1), spotAndOnDemand("b", 0, 2)}, anyCapacity)
	if !ok || price != 2 {
		t.Fatalf("the cheapest instance type's launch price wins, got (%v, %v)", price, ok)
	}
	onDemandOnly := scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand))
	price, ok = launchPrice([]*cloudprovider.InstanceType{spotAndOnDemand("a", 3, 1)}, onDemandOnly)
	if !ok || price != 1 {
		t.Fatalf("requirements select the capacity type, got (%v, %v)", price, ok)
	}
	if _, ok := launchPrice(nil, anyCapacity); ok {
		t.Fatal("no instance types must be unpriced")
	}
	spotOnly := scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	if _, ok := launchPrice([]*cloudprovider.InstanceType{spotAndOnDemand("a", 0, 1)}, spotOnly); ok {
		t.Fatal("no compatible offering must be unpriced")
	}
}

func TestMarginalLaunchPrice(t *testing.T) {
	anyCapacity := scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot, v1.CapacityTypeOnDemand))
	small, large := spotAndOnDemand("small", 1, 4), spotAndOnDemand("large", 10, 40)
	nc := &NodeClaim{NodeClaimTemplate: NodeClaimTemplate{InstanceTypeOptions: []*cloudprovider.InstanceType{small, large}, Requirements: anyCapacity}}

	delta, unpriced := marginalLaunchPrice(nc, []*cloudprovider.InstanceType{large}, anyCapacity)
	if unpriced || delta != 9 {
		t.Fatalf("losing the small instance type costs its price difference, got (%v, %v)", delta, unpriced)
	}
	delta, unpriced = marginalLaunchPrice(nc, []*cloudprovider.InstanceType{small, large}, anyCapacity)
	if unpriced || delta != 0 {
		t.Fatalf("keeping every instance type is free, got (%v, %v)", delta, unpriced)
	}
	delta, unpriced = marginalLaunchPrice(nc, nil, anyCapacity)
	if !unpriced || delta != 0 {
		t.Fatalf("an unpriceable grown NodeClaim reports unpriced at zero delta, got (%v, %v)", delta, unpriced)
	}
	unpricedNC := &NodeClaim{NodeClaimTemplate: NodeClaimTemplate{Requirements: anyCapacity}}
	delta, unpriced = marginalLaunchPrice(unpricedNC, []*cloudprovider.InstanceType{small}, anyCapacity)
	if !unpriced || delta != 0 {
		t.Fatalf("an unpriceable current NodeClaim reports unpriced at zero delta, got (%v, %v)", delta, unpriced)
	}
}
