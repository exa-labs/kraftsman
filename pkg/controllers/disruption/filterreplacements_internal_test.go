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
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &consolidation{}
			ok, reason, _ := c.filterReplacementsAndPublish(tc.newNodeClaims, nil, tc.candidatePrice, false)
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
	ok, reason, _ := c.filterReplacementsAndPublish([]*pscheduling.NodeClaim{nc}, nil, 1.0, false)
	if !ok || reason != "" {
		t.Fatalf("filterReplacementsAndPublish() = (%t, %q), want (true, \"\")", ok, reason)
	}
	if len(nc.InstanceTypeOptions) != 1 {
		t.Errorf("kept %d instance type options, want 1", len(nc.InstanceTypeOptions))
	}
}
