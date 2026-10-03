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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// onDemandZoneNodeClaim is a replacement claim that may launch on-demand in any of the given zones.
func onDemandZoneNodeClaim(zones []string, its ...*cloudprovider.InstanceType) *pscheduling.NodeClaim {
	return &pscheduling.NodeClaim{
		NodeClaimTemplate: pscheduling.NodeClaimTemplate{
			InstanceTypeOptions: its,
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zones...),
			),
		},
	}
}

func snapshotOptions(ncs ...*pscheduling.NodeClaim) [][]*cloudprovider.InstanceType {
	return lo.Map(ncs, func(nc *pscheduling.NodeClaim, _ int) []*cloudprovider.InstanceType {
		return append([]*cloudprovider.InstanceType(nil), nc.InstanceTypeOptions...)
	})
}

// retryOnDemandZones runs the on-demand zone retry the way computeConsolidation does: after the
// ordinary filter rejected the claims, with the skip reason and detail that filter returned.
func retryOnDemandZones(t *testing.T, budget priceBudget, ncs ...*pscheduling.NodeClaim) bool {
	t.Helper()
	snapshots := snapshotOptions(ncs...)
	c := &consolidation{}
	ok, skipReason, priceDetail := c.filterReplacementsAndPublish(ncs, nil, budget, false)
	if ok {
		t.Fatal("expected the ordinary price filter to reject the replacements")
	}
	return c.retryOnDemandZoneNarrowedReplacements(context.Background(), nil, ncs, snapshots, budget, skipReason, priceDetail, false)
}

// A node in an expensive zone must be replaceable by the same instance type in a cheaper zone:
// the type's worst-case price across both zones is the candidate's own, so only pinning the
// launch to the cheaper zone admits the move.
func TestOnDemandZoneRetryMovesTheSameTypeToTheCheaperZone(t *testing.T) {
	it := odToSpotInstanceType("gpu",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
	)
	nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, it)

	if !retryOnDemandZones(t, priceBudget{candidatePrice: 1.2}, nc) {
		t.Fatal("expected the on-demand zone retry to succeed")
	}
	if got := zoneValues(nc); len(got) != 1 || got[0] != "zone-a" {
		t.Errorf("zone requirement = %v, want [zone-a]", got)
	}
	if got := nc.Requirements.Get(v1.CapacityTypeLabelKey).Values(); len(got) != 1 || got[0] != v1.CapacityTypeOnDemand {
		t.Errorf("capacity type requirement = %v, want [on-demand]", got)
	}
	if len(nc.InstanceTypeOptions) != 1 || nc.InstanceTypeOptions[0].Name != "gpu" {
		t.Errorf("instance type options = %v, want [gpu]", instanceTypeNames(nc.InstanceTypeOptions))
	}
}

// The savings margin decides which zones are cheap: a zone that is cheaper than the candidate but
// not by the margin is not worth a replacement.
func TestOnDemandZoneRetryHonorsTheSavingsMargin(t *testing.T) {
	it := odToSpotInstanceType("gpu",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
	)
	nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, it)

	// The limit is 1.2 * (1 - 0.2) = 0.96, below zone-a's 1.0.
	if retryOnDemandZones(t, priceBudget{candidatePrice: 1.2, minSavings: 0.2}, nc) {
		t.Fatal("expected the retry to fail: no zone beats the budget once the margin applies")
	}
}

// Only the types that are cheap everywhere the pinned launch may land are kept: a larger type that
// fits the pods but costs more than the candidate in the cheap zone must not ride along.
func TestOnDemandZoneRetryDropsTypesExpensiveInTheCheapZone(t *testing.T) {
	small := odToSpotInstanceType("small",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
	)
	large := odToSpotInstanceType("large",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.5),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.8),
	)
	nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, small, large)

	if !retryOnDemandZones(t, priceBudget{candidatePrice: 1.2}, nc) {
		t.Fatal("expected the on-demand zone retry to succeed")
	}
	if got := zoneValues(nc); len(got) != 1 || got[0] != "zone-a" {
		t.Errorf("zone requirement = %v, want [zone-a]", got)
	}
	if len(nc.InstanceTypeOptions) != 1 || nc.InstanceTypeOptions[0].Name != "small" {
		t.Errorf("instance type options = %v, want [small]", instanceTypeNames(nc.InstanceTypeOptions))
	}
}

// An offering the provider marked unavailable is not a zone the launch can be pinned to.
func TestOnDemandZoneRetryIgnoresUnavailableCheapZones(t *testing.T) {
	unavailable := odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0)
	unavailable.Available = false
	it := odToSpotInstanceType("gpu",
		unavailable,
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
	)
	nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, it)

	if retryOnDemandZones(t, priceBudget{candidatePrice: 1.2}, nc) {
		t.Fatal("expected the retry to fail while the cheap zone has no available offering")
	}
}

// A minValues floor on the zone requirement survives the narrowing intersection, and the NodeClaim
// CRD rejects fewer values than the floor, so the retry must bail rather than pin below it.
func TestOnDemandZoneRetryRespectsZoneMinValues(t *testing.T) {
	it := odToSpotInstanceType("gpu",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
	)
	nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, it)
	nc.Requirements.Get(corev1.LabelTopologyZone).MinValues = lo.ToPtr(2)

	if outcome, ok := narrowClaimToCheapZones(nc, 1.2); ok || outcome != ODToSpotRetryOutcomeZoneMinValues {
		t.Fatalf("narrowClaimToCheapZones() = (%q, %t), want (%q, false)", outcome, ok, ODToSpotRetryOutcomeZoneMinValues)
	}
}

// A minValues or compatibility rejection fails for reasons no zone subset changes, so the retry
// must not arm for it.
func TestOnDemandZoneRetryOnlyArmsForPriceEmptiedSkips(t *testing.T) {
	it := odToSpotInstanceType("gpu",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
	)
	nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, it)
	c := &consolidation{}
	if c.retryOnDemandZoneNarrowedReplacements(context.Background(), nil, []*pscheduling.NodeClaim{nc}, snapshotOptions(nc), priceBudget{candidatePrice: 1.2}, CandidateSkipReplacementFlexibility, "", false) {
		t.Fatal("expected the retry not to run for a minValues rejection")
	}
	if got := zoneValues(nc); len(got) != 2 {
		t.Errorf("zone requirement = %v, want it left untouched", got)
	}
}

// A split into several replacements narrows each claim against its share of the aggregate budget.
func TestOnDemandZoneRetryNarrowsEachSplitClaimAgainstItsShare(t *testing.T) {
	newClaim := func() *pscheduling.NodeClaim {
		return onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, odToSpotInstanceType("half",
			odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 0.5),
			odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 0.7),
		))
	}
	first, second := newClaim(), newClaim()

	// Each claim's share is 1.2 / 2 = 0.6: zone-a (0.5) is cheap, zone-b (0.7) is not.
	if !retryOnDemandZones(t, priceBudget{candidatePrice: 1.2}, first, second) {
		t.Fatal("expected the on-demand zone retry to succeed")
	}
	for i, nc := range []*pscheduling.NodeClaim{first, second} {
		if got := zoneValues(nc); len(got) != 1 || got[0] != "zone-a" {
			t.Errorf("claim %d zone requirement = %v, want [zone-a]", i, got)
		}
	}
}

// The retry is for claims that cannot launch spot: spot-capable claims are priced on their spot
// offerings and have their own retries, so the two never arm for the same command.
func TestOnDemandZoneRetryAppliesOnlyToClaimsThatExcludeSpot(t *testing.T) {
	it := odToSpotInstanceType("gpu",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
	)
	claim := func(capacityTypes ...string) *pscheduling.NodeClaim {
		nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, it)
		nc.Requirements = scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "zone-a", "zone-b"),
		)
		if len(capacityTypes) != 0 {
			nc.Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityTypes...))
		}
		return nc
	}
	onDemand := claim(v1.CapacityTypeOnDemand)
	reserved := claim(v1.CapacityTypeOnDemand, v1.CapacityTypeReserved)
	mixed := claim(v1.CapacityTypeOnDemand, v1.CapacityTypeSpot)
	unconstrained := claim()

	c := &consolidation{}
	ctx := options.ToContext(context.Background(), &options.Options{ConsolidationOnDemandZoneRetry: true, ODToSpotConsolidation: true})
	for name, tc := range map[string]struct {
		claims []*pscheduling.NodeClaim
		want   bool
	}{
		"on-demand only":                       {claims: []*pscheduling.NodeClaim{onDemand}, want: true},
		"on-demand or reserved":                {claims: []*pscheduling.NodeClaim{reserved}, want: true},
		"spot allowed":                         {claims: []*pscheduling.NodeClaim{mixed}, want: false},
		"no capacity type requirement":         {claims: []*pscheduling.NodeClaim{unconstrained}, want: false},
		"one claim of a split may launch spot": {claims: []*pscheduling.NodeClaim{onDemand, mixed}, want: false},
		"every claim of a split excludes spot": {claims: []*pscheduling.NodeClaim{onDemand, reserved}, want: true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := c.onDemandZoneRetryApplies(ctx, tc.claims); got != tc.want {
				t.Errorf("onDemandZoneRetryApplies() = %t, want %t", got, tc.want)
			}
			// the spot-only retry needs every claim to allow spot, so it never arms alongside
			if spot, _ := c.odToSpotRetryApplies(ctx, []*Candidate{{capacityType: v1.CapacityTypeOnDemand}}, tc.claims); spot && tc.want {
				t.Error("the on-demand zone retry and the spot-only retry both apply")
			}
		})
	}
}

// A reservation that is full prices nothing, so the claim is priced on its on-demand offerings and
// the retry still moves it to the cheaper zone; the launch may only get cheaper if the reservation
// frees up inside the pinned zone.
func TestOnDemandZoneRetryMovesClaimsThatMayAlsoLaunchReserved(t *testing.T) {
	fullReservation := odToSpotOffering(v1.CapacityTypeReserved, "zone-b", 0.0000001)
	fullReservation.Available = false
	it := odToSpotInstanceType("gpu",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
		fullReservation,
	)
	nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, it)
	nc.Requirements = scheduling.NewRequirements(
		scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand, v1.CapacityTypeReserved),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "zone-a", "zone-b"),
	)

	if !retryOnDemandZones(t, priceBudget{candidatePrice: 1.2}, nc) {
		t.Fatal("expected the on-demand zone retry to succeed")
	}
	if got := zoneValues(nc); len(got) != 1 || got[0] != "zone-a" {
		t.Errorf("zone requirement = %v, want [zone-a]", got)
	}
}

func TestOnDemandZoneRetryAppliesOnlyWhenEnabled(t *testing.T) {
	nc := onDemandZoneNodeClaim([]string{"zone-a", "zone-b"}, odToSpotInstanceType("gpu",
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 1.2),
	))
	c := &consolidation{}
	for _, enabled := range []bool{false, true} {
		ctx := options.ToContext(context.Background(), &options.Options{ConsolidationOnDemandZoneRetry: enabled})
		if got := c.onDemandZoneRetryApplies(ctx, []*pscheduling.NodeClaim{nc}); got != enabled {
			t.Errorf("onDemandZoneRetryApplies() with the option %t = %t", enabled, got)
		}
	}
}
