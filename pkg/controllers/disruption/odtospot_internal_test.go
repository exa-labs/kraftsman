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
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func odToSpotInstanceType(name string, offerings ...cloudprovider.Offering) *cloudprovider.InstanceType {
	return fake.NewInstanceType(name,
		fake.WithResources(corev1.ResourceList{
			corev1.ResourceCPU:  resource.MustParse("4"),
			corev1.ResourcePods: resource.MustParse("100"),
		}),
		fake.WithOfferings(offerings...),
	)
}

func odToSpotOffering(capacityType, zone string, price float64) cloudprovider.Offering {
	return cloudprovider.Offering{
		Available: true,
		Price:     price,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  capacityType,
			corev1.LabelTopologyZone: zone,
		}),
	}
}

func odToSpotNodeClaim(its ...*cloudprovider.InstanceType) *pscheduling.NodeClaim {
	return &pscheduling.NodeClaim{
		NodeClaimTemplate: pscheduling.NodeClaimTemplate{
			InstanceTypeOptions: its,
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand, v1.CapacityTypeSpot),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "zone-a", "zone-b"),
			),
		},
	}
}

func odToSpotOptionsContext(enabled bool) context.Context {
	return options.ToContext(context.Background(), &options.Options{ODToSpotConsolidation: enabled})
}

// One price-spiked spot zone must not veto the conversion: the retry excludes zones whose spot
// price is at or above the budget and prices against the rest.
func TestRetrySpotOnlyReplacementsExcludesSpikedZones(t *testing.T) {
	it := odToSpotInstanceType("inf",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 13.0),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.35),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 19.5),
	)
	nc := odToSpotNodeClaim(it)
	snapshot := [][]*cloudprovider.InstanceType{append([]*cloudprovider.InstanceType(nil), nc.InstanceTypeOptions...)}

	c := &consolidation{}
	// The ordinary filter rejects: worst spot across zones is 19.5 >= 13.
	if ok, reason, _ := c.filterReplacementsAndPublish([]*pscheduling.NodeClaim{nc}, nil, 13.0, false); ok || reason != CandidateSkipNoCheaperSingleReplacement {
		t.Fatalf("filterReplacementsAndPublish() = (%t, %q), want rejection with %q", ok, reason, CandidateSkipNoCheaperSingleReplacement)
	}
	if !c.retrySpotOnlyReplacements("", consolidationSimulationOptions{}, nil, []*pscheduling.NodeClaim{nc}, snapshot, 13.0) {
		t.Fatal("expected the spot-only retry to succeed")
	}
	if got := nc.Requirements.Get(v1.CapacityTypeLabelKey).Values(); len(got) != 1 || got[0] != v1.CapacityTypeSpot {
		t.Errorf("capacity type requirement = %v, want [spot]", got)
	}
	if got := nc.Requirements.Get(corev1.LabelTopologyZone).Values(); len(got) != 1 || got[0] != "zone-a" {
		t.Errorf("zone requirement = %v, want [zone-a]", got)
	}
	if len(nc.InstanceTypeOptions) != 1 {
		t.Errorf("kept %d instance type options, want 1", len(nc.InstanceTypeOptions))
	}
}

// Two instance types cheap in opposite zones must not veto each other: the zone restriction anchors
// on the cheapest type's cheap zones and drops the types spiked inside them, instead of unioning
// every type's cheap zones and re-importing the spikes.
func TestRetrySpotOnlyReplacementsHandlesDisjointCheapZones(t *testing.T) {
	itA := odToSpotInstanceType("inf-a",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 13.0),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.35),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 19.5),
	)
	itB := odToSpotInstanceType("inf-b",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 13.0),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.5),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 20.0),
	)
	nc := odToSpotNodeClaim(itA, itB)
	snapshot := [][]*cloudprovider.InstanceType{append([]*cloudprovider.InstanceType(nil), nc.InstanceTypeOptions...)}

	c := &consolidation{}
	if !c.retrySpotOnlyReplacements("", consolidationSimulationOptions{}, nil, []*pscheduling.NodeClaim{nc}, snapshot, 13.0) {
		t.Fatal("expected the spot-only retry to succeed")
	}
	if got := nc.Requirements.Get(corev1.LabelTopologyZone).Values(); len(got) != 1 || got[0] != "zone-a" {
		t.Errorf("zone requirement = %v, want [zone-a]", got)
	}
	if len(nc.InstanceTypeOptions) != 1 || nc.InstanceTypeOptions[0].Name != "inf-a" {
		t.Errorf("instance type options = %v, want [inf-a]", lo.Map(nc.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name }))
	}
}

// A split into several replacements must narrow each claim against its share of the aggregate
// budget: a zone that only beats the whole budget would otherwise be pinned in and inflate the
// claim's worst-case price past its share, failing the final aggregate filter.
func TestRetrySpotOnlyReplacementsUsesPerClaimBudgetForSplits(t *testing.T) {
	newIT := func() *cloudprovider.InstanceType {
		return odToSpotInstanceType("inf",
			odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 6.5),
			odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.35),
			odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 12.0),
		)
	}
	nc1, nc2 := odToSpotNodeClaim(newIT()), odToSpotNodeClaim(newIT())
	snapshots := [][]*cloudprovider.InstanceType{
		append([]*cloudprovider.InstanceType(nil), nc1.InstanceTypeOptions...),
		append([]*cloudprovider.InstanceType(nil), nc2.InstanceTypeOptions...),
	}

	c := &consolidation{}
	// Aggregate budget 13: zone-b's 12.0 beats it but not the per-claim share of 6.5. If zone-b were
	// pinned, each claim's worst-case price would be 12.0 and the 24.0 total would fail the final
	// aggregate filter; anchored on the per-claim share, both land in zone-a at 1.35.
	if !c.retrySpotOnlyReplacements("", consolidationSimulationOptions{}, nil, []*pscheduling.NodeClaim{nc1, nc2}, snapshots, 13.0) {
		t.Fatal("expected the spot-only retry to succeed for the split")
	}
	for _, nc := range []*pscheduling.NodeClaim{nc1, nc2} {
		if got := nc.Requirements.Get(corev1.LabelTopologyZone).Values(); len(got) != 1 || got[0] != "zone-a" {
			t.Errorf("zone requirement = %v, want [zone-a]", got)
		}
	}
}

// A minValues floor on the zone (or capacity type) requirement survives the narrowing intersection
// and would make the API server reject the launched NodeClaim, so the retry must bail instead.
func TestRetrySpotOnlyReplacementsRespectsMinValues(t *testing.T) {
	it := odToSpotInstanceType("inf",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 13.0),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.35),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 19.5),
	)
	nc := odToSpotNodeClaim(it)
	nc.Requirements.Add(scheduling.NewRequirementWithFlexibility(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, lo.ToPtr(2), "zone-a", "zone-b"))
	snapshot := [][]*cloudprovider.InstanceType{append([]*cloudprovider.InstanceType(nil), nc.InstanceTypeOptions...)}

	c := &consolidation{}
	if c.retrySpotOnlyReplacements("", consolidationSimulationOptions{}, nil, []*pscheduling.NodeClaim{nc}, snapshot, 13.0) {
		t.Fatal("expected the spot-only retry to bail when pinning would violate the zone minValues")
	}

	nc = odToSpotNodeClaim(it)
	nc.Requirements.Add(scheduling.NewRequirementWithFlexibility(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, lo.ToPtr(2), v1.CapacityTypeOnDemand, v1.CapacityTypeSpot))
	if c.retrySpotOnlyReplacements("", consolidationSimulationOptions{}, nil, []*pscheduling.NodeClaim{nc}, snapshot, 13.0) {
		t.Fatal("expected the spot-only retry to bail when pinning would violate the capacity type minValues")
	}
}

// When every spot offering is at or above the budget there is nothing to convert to.
func TestRetrySpotOnlyReplacementsFailsWithoutCheapSpot(t *testing.T) {
	it := odToSpotInstanceType("inf",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 13.0),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 14.0),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 19.5),
	)
	nc := odToSpotNodeClaim(it)
	snapshot := [][]*cloudprovider.InstanceType{append([]*cloudprovider.InstanceType(nil), nc.InstanceTypeOptions...)}

	c := &consolidation{}
	if c.retrySpotOnlyReplacements("", consolidationSimulationOptions{}, nil, []*pscheduling.NodeClaim{nc}, snapshot, 13.0) {
		t.Fatal("expected the spot-only retry to fail")
	}
}

func TestODToSpotRetryApplies(t *testing.T) {
	c := &consolidation{}
	spotCapable := odToSpotNodeClaim(odToSpotInstanceType("inf", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0)))
	odOnly := &pscheduling.NodeClaim{
		NodeClaimTemplate: pscheduling.NodeClaimTemplate{
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
			),
		},
	}

	for _, tc := range []struct {
		name       string
		ctx        context.Context
		candidates []*Candidate
		claims     []*pscheduling.NodeClaim
		want       bool
	}{
		{
			name:       "applies for on-demand candidates with spot-capable replacements",
			ctx:        odToSpotOptionsContext(true),
			candidates: []*Candidate{{capacityType: v1.CapacityTypeOnDemand}},
			claims:     []*pscheduling.NodeClaim{spotCapable},
			want:       true,
		},
		{
			name:       "disabled by flag",
			ctx:        odToSpotOptionsContext(false),
			candidates: []*Candidate{{capacityType: v1.CapacityTypeOnDemand}},
			claims:     []*pscheduling.NodeClaim{spotCapable},
			want:       false,
		},
		{
			name:       "spot candidates are handled by spot-to-spot instead",
			ctx:        odToSpotOptionsContext(true),
			candidates: []*Candidate{{capacityType: v1.CapacityTypeSpot}},
			claims:     []*pscheduling.NodeClaim{spotCapable},
			want:       false,
		},
		{
			name:       "replacement that cannot launch spot",
			ctx:        odToSpotOptionsContext(true),
			candidates: []*Candidate{{capacityType: v1.CapacityTypeOnDemand}},
			claims:     []*pscheduling.NodeClaim{odOnly},
			want:       false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.odToSpotRetryApplies(tc.ctx, tc.candidates, tc.claims); got != tc.want {
				t.Errorf("odToSpotRetryApplies() = %t, want %t", got, tc.want)
			}
		})
	}
}
