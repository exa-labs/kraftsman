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

// Spot-to-spot consolidation consulting the CloudProvider's SpotReplacementAdvisor: the offerings
// it rejects leave the replacement's options and zones before the replacement is priced and
// pinned, and a replacement left without a launchable offering skips its candidate.

package disruption

import (
	"context"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

// rejectingAdvisor is a CloudProvider whose SpotReplacementAdvisor rejects the listed
// "<instance type>/<zone>" offerings and records every consultation.
type rejectingAdvisor struct {
	*fake.CloudProvider
	rejected  map[string]bool
	consulted []string
}

func newRejectingAdvisor(rejected ...string) *rejectingAdvisor {
	return &rejectingAdvisor{CloudProvider: fake.NewCloudProvider(), rejected: lo.SliceToMap(rejected, func(r string) (string, bool) { return r, true })}
}

func (a *rejectingAdvisor) SpotReplacementLaunchable(nodePool string, instanceType *cloudprovider.InstanceType, offering *cloudprovider.Offering) bool {
	key := instanceType.Name + "/" + offering.Zone()
	a.consulted = append(a.consulted, nodePool+":"+key)
	return !a.rejected[key]
}

func advisorSpotClaim(its ...*cloudprovider.InstanceType) *pscheduling.NodeClaim {
	nc := odToSpotNodeClaim(its...)
	nc.NodePoolName = "pool"
	nc.Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	return nc
}

// advisorCandidates is the one candidate priced at candidatePrice the tests replace.
func advisorCandidates() []*Candidate {
	return []*Candidate{{
		StateNode: &state.StateNode{
			Node:      &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "candidate"}},
			NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "candidate"}},
		},
		NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool"}},
		Price:    candidatePrice,
	}}
}

const candidatePrice = 13.0

func instanceTypeNames(its cloudprovider.InstanceTypes) []string {
	return lo.Map(its, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
}

// The cheapest type's only market is exhausted: the type leaves the options and the next cheapest
// type is what the replacement is priced and pinned on.
func TestDropUnlaunchableSpotReplacementMarketsFallsBackToNextCheapestType(t *testing.T) {
	cheap := odToSpotInstanceType("inf-cheap", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0))
	next := odToSpotInstanceType("inf-next", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 2.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 2.1))
	nc := advisorSpotClaim(cheap, next)
	c := &consolidation{cloudProvider: newRejectingAdvisor("inf-cheap/zone-a")}

	masked, ok, reason := c.dropUnlaunchableSpotReplacementMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false)
	if !ok || reason != "" {
		t.Fatalf("dropUnlaunchableSpotReplacementMarkets() = (%t, %q), want the claim admitted", ok, reason)
	}
	if got := instanceTypeNames(nc.InstanceTypeOptions); len(got) != 1 || got[0] != "inf-next" {
		t.Fatalf("instance type options = %v, want [inf-next]", got)
	}
	if !masked.Has(nc) {
		t.Fatal("a claim that lost an offering must be reported as masked")
	}
	if cheap.Offerings[0].Available != true {
		t.Fatal("the scheduler's instance type must not be mutated")
	}
	pinSpotReplacementToLaunchableZones(nc)
	if got := nc.Requirements.Get(corev1.LabelTopologyZone).Values(); len(got) != 2 {
		t.Fatalf("zone requirement = %v, want both of inf-next's zones", got)
	}
}

// partiallyExhaustedClaim is one type with spot in zone-a and zone-b plus on-demand in zone-b,
// after the advisor rejected the zone-b spot market.
func partiallyExhaustedClaim(t *testing.T) (*cloudprovider.InstanceType, *pscheduling.NodeClaim, *rejectingAdvisor) {
	t.Helper()
	it := odToSpotInstanceType("inf",
		odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0),
		odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.2),
		odToSpotOffering(v1.CapacityTypeOnDemand, "zone-b", 5.0),
	)
	nc := advisorSpotClaim(it)
	advisor := newRejectingAdvisor("inf/zone-b")
	c := &consolidation{cloudProvider: advisor}
	masked, ok, _ := c.dropUnlaunchableSpotReplacementMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false)
	if !ok || !masked.Has(nc) {
		t.Fatalf("dropUnlaunchableSpotReplacementMarkets() = ok %t, masked %t; want the claim admitted and masked", ok, masked.Has(nc))
	}
	return it, nc, advisor
}

// One exhausted zone of a type masks only that offering, on a copy: the type stays, priced on
// its remaining zones.
func TestDropUnlaunchableSpotReplacementMarketsMasksOnlyTheExhaustedOffering(t *testing.T) {
	it, nc, _ := partiallyExhaustedClaim(t)
	if len(nc.InstanceTypeOptions) != 1 || nc.InstanceTypeOptions[0] == it {
		t.Fatal("the masked type must be a copy of the scheduler's instance type")
	}
	if it.Offerings[1].Available != true || nc.InstanceTypeOptions[0].Offerings[1].Available {
		t.Fatal("only the copy's zone-b spot offering must be unavailable")
	}
	if got := nc.InstanceTypeOptions[0].Offerings.Available().WorstLaunchPrice(nc.Requirements); got != 1.0 {
		t.Fatalf("worst launch price = %v, want 1.0 from zone-a alone", got)
	}
}

// The advisor sees the spot offerings the replacement could launch in, in offering order; the
// on-demand offering is outside the spot-only requirements and is never consulted.
func TestDropUnlaunchableSpotReplacementMarketsConsultsOnlySpotOfferings(t *testing.T) {
	_, _, advisor := partiallyExhaustedClaim(t)
	if want := []string{"pool:inf/zone-a", "pool:inf/zone-b"}; len(advisor.consulted) != 2 || advisor.consulted[0] != want[0] || advisor.consulted[1] != want[1] {
		t.Fatalf("advisor consulted %v, want %v", advisor.consulted, want)
	}
}

// The claim's zone requirement is pinned to the surviving zone so the launch cannot land in the
// masked one.
func TestPinSpotReplacementToLaunchableZonesDropsTheMaskedZone(t *testing.T) {
	_, nc, _ := partiallyExhaustedClaim(t)
	pinSpotReplacementToLaunchableZones(nc)
	if got := nc.Requirements.Get(corev1.LabelTopologyZone).Values(); len(got) != 1 || got[0] != "zone-a" {
		t.Fatalf("zone requirement = %v, want [zone-a]", got)
	}
}

// Every offering the replacement could launch in is exhausted: the candidate is skipped with the
// market reason instead of being pinned to a launch that cannot succeed.
func TestDropUnlaunchableSpotReplacementMarketsSkipsWhenNothingLaunchable(t *testing.T) {
	it := odToSpotInstanceType("inf", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.2))
	nc := advisorSpotClaim(it)
	c := &consolidation{cloudProvider: newRejectingAdvisor("inf/zone-a", "inf/zone-b")}

	_, ok, reason := c.dropUnlaunchableSpotReplacementMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false)
	if ok || reason != CandidateSkipSpotMarketExhausted {
		t.Fatalf("dropUnlaunchableSpotReplacementMarkets() = (%t, %q), want skip with %q", ok, reason, CandidateSkipSpotMarketExhausted)
	}
}

// A CloudProvider without the advisor leaves the replacement exactly as spot-to-spot built it.
func TestDropUnlaunchableSpotReplacementMarketsWithoutAdvisorIsANoop(t *testing.T) {
	it := odToSpotInstanceType("inf", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0))
	nc := advisorSpotClaim(it)
	c := &consolidation{cloudProvider: fake.NewCloudProvider()}

	masked, ok, reason := c.dropUnlaunchableSpotReplacementMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false)
	if !ok || reason != "" || masked.Len() != 0 {
		t.Fatalf("dropUnlaunchableSpotReplacementMarkets() = (%d masked, %t, %q), want an untouched claim", masked.Len(), ok, reason)
	}
	if len(nc.InstanceTypeOptions) != 1 || nc.InstanceTypeOptions[0] != it {
		t.Fatal("instance type options must be the scheduler's own")
	}
}

// A zone requirement whose minValues the launchable zones cannot satisfy is left alone: the
// API server would refuse the narrowed claim, and the type-level narrowing already stands.
func TestPinSpotReplacementToLaunchableZonesRespectsMinValues(t *testing.T) {
	it := odToSpotInstanceType("inf", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.2))
	nc := advisorSpotClaim(it)
	nc.Requirements.Add(scheduling.NewRequirementWithFlexibility(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, lo.ToPtr(2), "zone-a", "zone-b"))
	c := &consolidation{cloudProvider: newRejectingAdvisor("inf/zone-b")}

	if _, ok, _ := c.dropUnlaunchableSpotReplacementMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false); !ok {
		t.Fatal("the type still has a launchable offering and must be admitted")
	}
	pinSpotReplacementToLaunchableZones(nc)
	if got := nc.Requirements.Get(corev1.LabelTopologyZone).Values(); len(got) != 2 {
		t.Fatalf("zone requirement = %v, want both zones kept under minValues 2", got)
	}
}

// End to end through spot-to-spot consolidation with the production minimum of one instance type:
// the exhausted cheapest type is not what the single-type launch is pinned to.
func TestComputeSpotToSpotConsolidationPinsPastExhaustedMarket(t *testing.T) {
	cheap := odToSpotInstanceType("inf-cheap", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0))
	next := odToSpotInstanceType("inf-next", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 2.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 9.0))
	nc := odToSpotNodeClaim(cheap, next)
	nc.NodePoolName = "pool"
	ctx := options.ToContext(context.Background(), &options.Options{
		SpotToSpotMinInstanceTypes: 1,
		FeatureGates:               options.FeatureGates{SpotToSpotConsolidation: true},
	})
	c := &consolidation{cloudProvider: newRejectingAdvisor("inf-cheap/zone-a"), recorder: test.NewEventRecorder()}
	candidates := advisorCandidates()

	cmd, reason, err := c.computeSpotToSpotConsolidation(ctx, candidates, pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{nc}}, priceBudget{candidatePrice: candidatePrice}, consolidationSimulationOptions{silent: true})
	if err != nil || reason != "" || len(cmd.Replacements) != 1 {
		t.Fatalf("computeSpotToSpotConsolidation() = (%d replacements, %q, %v), want one replacement", len(cmd.Replacements), reason, err)
	}
	if got := instanceTypeNames(nc.InstanceTypeOptions); len(got) != 1 || got[0] != "inf-next" {
		t.Fatalf("launch pinned to %v, want [inf-next]", got)
	}
	if got := nc.Requirements.Get(v1.CapacityTypeLabelKey).Values(); len(got) != 1 || got[0] != v1.CapacityTypeSpot {
		t.Fatalf("capacity type requirement = %v, want [spot]", got)
	}

	// With the next type exhausted as well the candidate is skipped, not pinned to a dry market.
	nc = odToSpotNodeClaim(cheap, next)
	nc.NodePoolName = "pool"
	c.cloudProvider = newRejectingAdvisor("inf-cheap/zone-a", "inf-next/zone-a", "inf-next/zone-b")
	if _, reason, err := c.computeSpotToSpotConsolidation(ctx, candidates, pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{nc}}, priceBudget{candidatePrice: candidatePrice}, consolidationSimulationOptions{silent: true}); err != nil || reason != CandidateSkipSpotMarketExhausted {
		t.Fatalf("computeSpotToSpotConsolidation() = (%q, %v), want skip with %q", reason, err, CandidateSkipSpotMarketExhausted)
	}
}
