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

package disruption

import (
	"context"
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// reservedPriceScale is how far below on-demand a cloud provider prices reserved offerings so that reserved capacity
// is always preferred: a reserved offering costs its on-demand price times this.
const reservedPriceScale = 1e-7

// reservedReplacementClaim is a replacement that can only launch into a reservation of an instance type whose
// on-demand price is onDemandPrice.
func reservedReplacementClaim(onDemandPrice float64) *pscheduling.NodeClaim {
	it := fake.NewInstanceType("reserved-small",
		fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("100")}),
		fake.WithOfferings(cloudprovider.Offering{
			Available:    true,
			Price:        onDemandPrice * reservedPriceScale,
			Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved, corev1.LabelTopologyZone: "test-zone-1"}),
		}),
	)
	return &pscheduling.NodeClaim{NodeClaimTemplate: pscheduling.NodeClaimTemplate{
		InstanceTypeOptions: []*cloudprovider.InstanceType{it},
		Requirements:        scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeReserved)),
	}}
}

func minSavingsPerHourContext(perHour float64) context.Context {
	return options.ToContext(context.Background(), &options.Options{ConsolidationReplaceMinSavingsPerHour: perHour})
}

// A reserved node of a $3.00/h on-demand type replaced by a reservation of a $1.86/h type looks 38% cheaper - both
// prices are scaled by the same tiny factor - so it clears a 5% fractional floor while saving nothing: both
// reservations are already paid for. Only an absolute floor stops it.
func TestReservedToReservedReplacementNeedsAnAbsoluteSaving(t *testing.T) {
	candidatePrice := 3.00424 * reservedPriceScale
	for _, tc := range []struct {
		name    string
		perHour float64
		ok      bool
		reason  string
	}{
		{name: "a fractional floor alone admits the reservation swap", perHour: 0, ok: true},
		{name: "an absolute floor rejects it as below the minimum savings", perHour: 0.001, ok: false, reason: CandidateSkipBelowMinSavings},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := newPriceBudget(minSavingsPerHourContext(tc.perHour), candidatePrice, 0.05)
			ok, reason, _ := (&consolidation{}).filterReplacementsAndPublish([]*pscheduling.NodeClaim{reservedReplacementClaim(1.861)}, nil, budget, false)
			if ok != tc.ok || reason != tc.reason {
				t.Fatalf("filterReplacementsAndPublish() = (%t, %q), want (%t, %q)", ok, reason, tc.ok, tc.reason)
			}
		})
	}
}

// The absolute floor only binds where the fractional one is weaker: on an ordinarily priced node the 5% margin is
// the larger and the budget is unchanged.
func TestMinSavingsPerHourOnlyBindsBelowTheFractionalFloor(t *testing.T) {
	ctx := minSavingsPerHourContext(0.001)
	if got := newPriceBudget(ctx, 2.0, 0.05).limit(); math.Abs(got-1.9) > 1e-12 {
		t.Errorf("limit() on a $2/h candidate = %g, want 1.9 (the 5%% floor)", got)
	}
	if got := newPriceBudget(ctx, 0.01, 0).limit(); math.Abs(got-0.009) > 1e-12 {
		t.Errorf("limit() on a $0.01/h candidate = %g, want 0.009 (the $0.001/h floor)", got)
	}
	if got := newPriceBudget(minSavingsPerHourContext(0), 2.0, 0.05); got.minSavingsPerHour != 0 {
		t.Errorf("minSavingsPerHour = %g without the option, want 0", got.minSavingsPerHour)
	}
	// On-demand to reserved saves the on-demand price outright and still clears the absolute floor.
	ok, reason, _ := (&consolidation{}).filterReplacementsAndPublish([]*pscheduling.NodeClaim{reservedReplacementClaim(1.861)}, nil, newPriceBudget(ctx, 3.00424, 0.05), false)
	if !ok {
		t.Fatalf("on-demand to reserved rejected with %q", reason)
	}
}
