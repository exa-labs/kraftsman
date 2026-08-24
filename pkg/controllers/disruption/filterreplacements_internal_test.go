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
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func priceFilterInstanceType(name string, price float64) *cloudprovider.InstanceType {
	return fake.NewInstanceType(name,
		fake.WithResources(corev1.ResourceList{
			corev1.ResourceCPU:  resource.MustParse("4"),
			corev1.ResourcePods: resource.MustParse("100"),
		}),
		fake.WithOfferings(cloudprovider.Offering{
			Available:    true,
			Price:        price,
			Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1"}),
		}),
	)
}

func priceFilterNodeClaim(minValues int, its ...*cloudprovider.InstanceType) *pscheduling.NodeClaim {
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(v1.NodeSelectorRequirementWithMinValues{
		Key:       corev1.LabelInstanceTypeStable,
		Operator:  corev1.NodeSelectorOpExists,
		MinValues: ptr.To(minValues),
	})
	return &pscheduling.NodeClaim{
		NodeClaimTemplate: pscheduling.NodeClaimTemplate{
			InstanceTypeOptions: its,
			Requirements:        reqs,
		},
	}
}

// The skip reason a rejected replacement carries is the difference between "wait for cheaper
// offerings" and "loosen minValues", so each rejection has to name the test it actually failed.
func TestFilterReplacementsAndPublishSkipReasons(t *testing.T) {
	cheap := priceFilterInstanceType("cheap", 0.2)
	expensive := priceFilterInstanceType("expensive", 5.0)

	for _, tc := range []struct {
		name           string
		newNodeClaims  []*pscheduling.NodeClaim
		candidatePrice float64
		minSavings     float64
		reason         string
	}{
		{
			// Spot-to-spot narrows options to spot-compatible types before pricing, so an empty
			// claim is a compatibility failure that never reached a price comparison.
			name:           "no options to price at all",
			newNodeClaims:  []*pscheduling.NodeClaim{priceFilterNodeClaim(1)},
			candidatePrice: 1.0,
			reason:         CandidateSkipNoCompatibleReplacement,
		},
		{
			// The ceiling removes the expensive option, leaving one type where minValues wants two:
			// cheaper capacity exists, the surviving set is too narrow.
			name:           "cheaper options exist but minValues is not satisfied",
			newNodeClaims:  []*pscheduling.NodeClaim{priceFilterNodeClaim(2, cheap, expensive)},
			candidatePrice: 1.0,
			reason:         CandidateSkipReplacementFlexibility,
		},
		{
			name:           "single replacement with nothing cheaper",
			newNodeClaims:  []*pscheduling.NodeClaim{priceFilterNodeClaim(1, expensive)},
			candidatePrice: 1.0,
			reason:         CandidateSkipNoCheaperSingleReplacement,
		},
		{
			name:           "replacement set with no cheaper combination",
			newNodeClaims:  []*pscheduling.NodeClaim{priceFilterNodeClaim(1, cheap), priceFilterNodeClaim(1, cheap)},
			candidatePrice: 0.3,
			reason:         CandidateSkipNoCheaperReplacementSet,
		},
		{
			// $0.2 beats $0.22 but not by the 10% the floor asks for: cheaper capacity exists and only
			// the margin stands in the way, so the skip names the floor rather than the offerings.
			name:           "single replacement cheaper but under the savings floor",
			newNodeClaims:  []*pscheduling.NodeClaim{priceFilterNodeClaim(1, cheap)},
			candidatePrice: 0.22,
			minSavings:     0.1,
			reason:         CandidateSkipBelowMinSavings,
		},
		{
			name:           "replacement set cheaper but under the savings floor",
			newNodeClaims:  []*pscheduling.NodeClaim{priceFilterNodeClaim(1, cheap), priceFilterNodeClaim(1, cheap)},
			candidatePrice: 0.42,
			minSavings:     0.1,
			reason:         CandidateSkipBelowMinSavings,
		},
		{
			// Nothing is cheaper even before the margin, so the floor is not what blocked this one.
			name:           "nothing cheaper with a savings floor set",
			newNodeClaims:  []*pscheduling.NodeClaim{priceFilterNodeClaim(1, expensive)},
			candidatePrice: 1.0,
			minSavings:     0.1,
			reason:         CandidateSkipNoCheaperSingleReplacement,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &consolidation{}
			ok, reason, _ := c.filterReplacementsAndPublish(tc.newNodeClaims, nil, priceBudget{candidatePrice: tc.candidatePrice, minSavings: tc.minSavings}, false)
			if ok {
				t.Fatalf("expected the replacements to be rejected")
			}
			if reason != tc.reason {
				t.Errorf("skip reason = %q, want %q", reason, tc.reason)
			}
		})
	}
}

func TestFilterReplacementsAndPublishKeepsCheaperOptions(t *testing.T) {
	c := &consolidation{}
	nc := priceFilterNodeClaim(1, priceFilterInstanceType("cheap", 0.2))
	ok, reason, _ := c.filterReplacementsAndPublish([]*pscheduling.NodeClaim{nc}, nil, priceBudget{candidatePrice: 1.0}, false)
	if !ok || reason != "" {
		t.Fatalf("filterReplacementsAndPublish() = (%t, %q), want (true, \"\")", ok, reason)
	}
	if len(nc.InstanceTypeOptions) != 1 {
		t.Errorf("kept %d instance type options, want 1", len(nc.InstanceTypeOptions))
	}
}

// The floor decides what may launch, not only whether to replace: an option that is cheaper than the
// candidate but inside the margin is trimmed so the worst-case launch always clears the floor.
func TestFilterReplacementsAndPublishTrimsOptionsInsideTheFloor(t *testing.T) {
	c := &consolidation{}
	nc := priceFilterNodeClaim(1, priceFilterInstanceType("cheap", 0.2), priceFilterInstanceType("barely-cheaper", 0.95))
	ok, reason, _ := c.filterReplacementsAndPublish([]*pscheduling.NodeClaim{nc}, nil, priceBudget{candidatePrice: 1.0, minSavings: 0.1}, false)
	if !ok || reason != "" {
		t.Fatalf("filterReplacementsAndPublish() = (%t, %q), want (true, \"\")", ok, reason)
	}
	if len(nc.InstanceTypeOptions) != 1 || nc.InstanceTypeOptions[0].Name != "cheap" {
		t.Errorf("kept %v, want only the option that clears the floor", lo.Map(nc.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name }))
	}
}

func TestPriceBudget(t *testing.T) {
	b := priceBudget{candidatePrice: 2.0, minSavings: 0.25}
	if got := b.limit(); got != 1.5 {
		t.Errorf("limit() = %g, want 1.5", got)
	}
	if got := b.split(3); got != 0.5 {
		t.Errorf("split(3) = %g, want 0.5", got)
	}
	for cheapestTotal, want := range map[float64]bool{1.4: false, 1.5: true, 1.99: true, 2.0: false} {
		if got := b.vetoedByMargin(cheapestTotal); got != want {
			t.Errorf("vetoedByMargin(%g) = %t, want %t", cheapestTotal, got, want)
		}
	}
	if (priceBudget{candidatePrice: 2.0}).vetoedByMargin(1.0) {
		t.Errorf("a zero margin can never veto")
	}
}
