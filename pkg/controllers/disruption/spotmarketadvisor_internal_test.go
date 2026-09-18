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

// Spot-to-spot consolidation consulting the CloudProvider's SpotReplacementAdvisor: the
// replacement is pinned to instance types and zones that only combine into offerings the advisor
// admits and the pods fit, before the replacement is priced, and a replacement left without such
// a market skips its candidate.

package disruption

import (
	"context"
	"slices"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

func zoneValues(nc *pscheduling.NodeClaim) []string {
	zones := nc.Requirements.Get(corev1.LabelTopologyZone).Values()
	slices.Sort(zones)
	return zones
}

// restrict runs the advisor restriction over one claim and fails the test unless it was admitted.
func restrict(t *testing.T, advisor cloudprovider.CloudProvider, nc *pscheduling.NodeClaim) {
	t.Helper()
	c := &consolidation{cloudProvider: advisor}
	if ok, reason := c.restrictSpotReplacementsToLaunchableMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false); !ok {
		t.Fatalf("restrictSpotReplacementsToLaunchableMarkets() = (%t, %q), want the claim admitted", ok, reason)
	}
}

// The cheapest type's only market is exhausted: the type leaves the options and the next cheapest
// type is what the replacement is priced and pinned on, in both of its zones.
func TestRestrictSpotReplacementsFallsBackToNextCheapestType(t *testing.T) {
	cheap := odToSpotInstanceType("inf-cheap", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0))
	next := odToSpotInstanceType("inf-next", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 2.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 2.1))
	nc := advisorSpotClaim(cheap, next)

	restrict(t, newRejectingAdvisor("inf-cheap/zone-a"), nc)
	if got := instanceTypeNames(nc.InstanceTypeOptions); !slices.Equal(got, []string{"inf-next"}) {
		t.Fatalf("instance type options = %v, want [inf-next]", got)
	}
	if got := zoneValues(nc); !slices.Equal(got, []string{"zone-a", "zone-b"}) {
		t.Fatalf("zone requirement = %v, want both of inf-next's zones", got)
	}
	if !cheap.Offerings[0].Available || nc.InstanceTypeOptions[0] != next {
		t.Fatal("the scheduler's instance types must be neither mutated nor copied")
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
	restrict(t, advisor, nc)
	return it, nc, advisor
}

// One exhausted zone of a type pins the claim to the other zone: the type stays, untouched, and is
// priced on the zone the launch may land in.
func TestRestrictSpotReplacementsPinsTheClaimToTheAdmittedZone(t *testing.T) {
	it, nc, _ := partiallyExhaustedClaim(t)
	if len(nc.InstanceTypeOptions) != 1 || nc.InstanceTypeOptions[0] != it || !it.Offerings[1].Available {
		t.Fatal("the type must stay in the options as the scheduler's own, with its offerings intact")
	}
	if got := zoneValues(nc); !slices.Equal(got, []string{"zone-a"}) {
		t.Fatalf("zone requirement = %v, want [zone-a]", got)
	}
	if got := it.Offerings.Available().WorstLaunchPrice(nc.Requirements); got != 1.0 {
		t.Fatalf("worst launch price = %v, want 1.0 from zone-a alone", got)
	}
}

// The advisor sees the spot offerings the replacement could launch in, in offering order; the
// on-demand offering is outside the spot-only requirements and is never consulted.
func TestRestrictSpotReplacementsConsultsOnlySpotOfferings(t *testing.T) {
	_, _, advisor := partiallyExhaustedClaim(t)
	if want := []string{"pool:inf/zone-a", "pool:inf/zone-b"}; !slices.Equal(advisor.consulted, want) {
		t.Fatalf("advisor consulted %v, want %v", advisor.consulted, want)
	}
}

// Two types admitted in different zones cannot share a claim: the launch may pair any type with
// any zone the claim allows, so the zones follow the cheapest admitted type and the other type,
// rejected in that zone, leaves. A type admitted everywhere the anchor is stays.
func TestRestrictSpotReplacementsKeepsTypesAndZonesPairedWithAdmittedOfferings(t *testing.T) {
	a := odToSpotInstanceType("inf-a", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.0))
	b := odToSpotInstanceType("inf-b", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 2.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 2.0))
	wide := odToSpotInstanceType("inf-wide", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 3.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 3.0))
	nc := advisorSpotClaim(a, b, wide)

	restrict(t, newRejectingAdvisor("inf-a/zone-b", "inf-b/zone-a"), nc)
	if got := instanceTypeNames(nc.InstanceTypeOptions); !slices.Equal(got, []string{"inf-a", "inf-wide"}) {
		t.Fatalf("instance type options = %v, want [inf-a inf-wide]: inf-b is rejected in the anchor's zone", got)
	}
	if got := zoneValues(nc); !slices.Equal(got, []string{"zone-a"}) {
		t.Fatalf("zone requirement = %v, want [zone-a]", got)
	}
}

// The cheapest type's own zones decide the pin even when a pricier type is admitted more widely:
// pricier zones would put the cheap type's rejected market back into the claim.
func TestRestrictSpotReplacementsAnchorsZonesOnTheCheapestAdmittedType(t *testing.T) {
	cheap := odToSpotInstanceType("inf-cheap", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.0))
	wide := odToSpotInstanceType("inf-wide", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 2.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 2.0))
	nc := advisorSpotClaim(cheap, wide)

	restrict(t, newRejectingAdvisor("inf-cheap/zone-b"), nc)
	if got := instanceTypeNames(nc.InstanceTypeOptions); !slices.Equal(got, []string{"inf-cheap", "inf-wide"}) {
		t.Fatalf("instance type options = %v, want [inf-cheap inf-wide]", got)
	}
	if got := zoneValues(nc); !slices.Equal(got, []string{"zone-a"}) {
		t.Fatalf("zone requirement = %v, want [zone-a]: inf-wide's zone-b would admit inf-cheap/zone-b too", got)
	}
}

// An admitted offering whose overrides leave it too small for the claim's requests is no market
// for the replacement: with the fitting offering rejected the candidate is skipped rather than
// pinned to an undersized launch.
func TestRestrictSpotReplacementsRequiresTheAdmittedOfferingToFit(t *testing.T) {
	small := odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 0.5)
	small.OverheadOverride = &cloudprovider.InstanceTypeOverhead{KubeReserved: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}}
	it := odToSpotInstanceType("inf", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0), small)
	nc := advisorSpotClaim(it)
	nc.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}
	c := &consolidation{cloudProvider: newRejectingAdvisor("inf/zone-a")}

	if ok, reason := c.restrictSpotReplacementsToLaunchableMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false); ok || reason != CandidateSkipSpotMarketExhausted {
		t.Fatalf("restrictSpotReplacementsToLaunchableMarkets() = (%t, %q), want skip with %q", ok, reason, CandidateSkipSpotMarketExhausted)
	}

	// With the requests fitting the override the smaller zone is a market like any other.
	nc = advisorSpotClaim(it)
	nc.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}
	restrict(t, newRejectingAdvisor("inf/zone-a"), nc)
	if got := zoneValues(nc); !slices.Equal(got, []string{"zone-b"}) {
		t.Fatalf("zone requirement = %v, want [zone-b]", got)
	}
}

// Every offering the replacement could launch in is exhausted: the candidate is skipped with the
// market reason instead of being pinned to a launch that cannot succeed.
func TestRestrictSpotReplacementsSkipsWhenNothingLaunchable(t *testing.T) {
	it := odToSpotInstanceType("inf", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.2))
	nc := advisorSpotClaim(it)
	c := &consolidation{cloudProvider: newRejectingAdvisor("inf/zone-a", "inf/zone-b")}

	if ok, reason := c.restrictSpotReplacementsToLaunchableMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false); ok || reason != CandidateSkipSpotMarketExhausted {
		t.Fatalf("restrictSpotReplacementsToLaunchableMarkets() = (%t, %q), want skip with %q", ok, reason, CandidateSkipSpotMarketExhausted)
	}
}

// A CloudProvider without the advisor leaves the replacement exactly as spot-to-spot built it.
func TestRestrictSpotReplacementsWithoutAdvisorIsANoop(t *testing.T) {
	it := odToSpotInstanceType("inf", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0))
	nc := advisorSpotClaim(it)
	c := &consolidation{cloudProvider: fake.NewCloudProvider()}

	if ok, reason := c.restrictSpotReplacementsToLaunchableMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false); !ok || reason != "" {
		t.Fatalf("restrictSpotReplacementsToLaunchableMarkets() = (%t, %q), want an untouched claim", ok, reason)
	}
	if len(nc.InstanceTypeOptions) != 1 || nc.InstanceTypeOptions[0] != it || len(zoneValues(nc)) != 2 {
		t.Fatal("instance type options and zones must be the scheduler's own")
	}
}

// A zone requirement whose minValues the admitted zones cannot satisfy skips the candidate: the
// API server would refuse the pinned claim, and an unpinned one could launch in the rejected zone.
func TestRestrictSpotReplacementsSkipsWhenAdmittedZonesBreakMinValues(t *testing.T) {
	it := odToSpotInstanceType("inf", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.2))
	nc := advisorSpotClaim(it)
	nc.Requirements.Add(scheduling.NewRequirementWithFlexibility(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, lo.ToPtr(2), "zone-a", "zone-b"))
	c := &consolidation{cloudProvider: newRejectingAdvisor("inf/zone-b")}

	if ok, reason := c.restrictSpotReplacementsToLaunchableMarkets(advisorCandidates(), []*pscheduling.NodeClaim{nc}, false); ok || reason != CandidateSkipReplacementFlexibility {
		t.Fatalf("restrictSpotReplacementsToLaunchableMarkets() = (%t, %q), want skip with %q", ok, reason, CandidateSkipReplacementFlexibility)
	}
	if got := zoneValues(nc); !slices.Equal(got, []string{"zone-a", "zone-b"}) {
		t.Fatalf("zone requirement = %v, want left as built", got)
	}
}

func spotToSpotContext(minInstanceTypes int) context.Context {
	return options.ToContext(context.Background(), &options.Options{
		SpotToSpotMinInstanceTypes: minInstanceTypes,
		FeatureGates:               options.FeatureGates{SpotToSpotConsolidation: true},
	})
}

// End to end through spot-to-spot consolidation with the production minimum of one instance type:
// the exhausted cheapest type is not what the single-type launch is pinned to.
func TestComputeSpotToSpotConsolidationPinsPastExhaustedMarket(t *testing.T) {
	cheap := odToSpotInstanceType("inf-cheap", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0))
	next := odToSpotInstanceType("inf-next", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 2.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 9.0))
	nc := odToSpotNodeClaim(cheap, next)
	nc.NodePoolName = "pool"
	ctx := spotToSpotContext(1)
	c := &consolidation{cloudProvider: newRejectingAdvisor("inf-cheap/zone-a"), recorder: test.NewEventRecorder()}
	candidates := advisorCandidates()

	cmd, reason, err := c.computeSpotToSpotConsolidation(ctx, candidates, pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{nc}}, priceBudget{candidatePrice: candidatePrice}, consolidationSimulationOptions{silent: true})
	if err != nil || reason != "" || len(cmd.Replacements) != 1 {
		t.Fatalf("computeSpotToSpotConsolidation() = (%d replacements, %q, %v), want one replacement", len(cmd.Replacements), reason, err)
	}
	if got := instanceTypeNames(nc.InstanceTypeOptions); !slices.Equal(got, []string{"inf-next"}) {
		t.Fatalf("launch pinned to %v, want [inf-next]", got)
	}
	if got := nc.Requirements.Get(v1.CapacityTypeLabelKey).Values(); !slices.Equal(got, []string{v1.CapacityTypeSpot}) {
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

// End to end with two types surviving to a single-type launch: the launch truncation keeps the
// cheapest type and the zone pin keeps only the zone it is admitted in, so the type the launch is
// pinned to cannot be paired with a zone where it is exhausted.
func TestComputeSpotToSpotConsolidationNeverPairsATypeWithARejectedZone(t *testing.T) {
	a := odToSpotInstanceType("inf-a", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 1.0))
	b := odToSpotInstanceType("inf-b", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 2.0), odToSpotOffering(v1.CapacityTypeSpot, "zone-b", 2.0))
	for _, minInstanceTypes := range []int{1, 2} {
		nc := odToSpotNodeClaim(a, b)
		nc.NodePoolName = "pool"
		c := &consolidation{cloudProvider: newRejectingAdvisor("inf-a/zone-b", "inf-b/zone-a"), recorder: test.NewEventRecorder()}

		cmd, reason, err := c.computeSpotToSpotConsolidation(spotToSpotContext(minInstanceTypes), advisorCandidates(), pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{nc}}, priceBudget{candidatePrice: candidatePrice}, consolidationSimulationOptions{silent: true})
		if minInstanceTypes == 2 {
			if err != nil || reason != CandidateSkipSpotToSpotFlexibility {
				t.Fatalf("min %d: computeSpotToSpotConsolidation() = (%q, %v), want skip with %q: only inf-a pairs with zone-a", minInstanceTypes, reason, err, CandidateSkipSpotToSpotFlexibility)
			}
			continue
		}
		if err != nil || reason != "" || len(cmd.Replacements) != 1 {
			t.Fatalf("min %d: computeSpotToSpotConsolidation() = (%d replacements, %q, %v), want one replacement", minInstanceTypes, len(cmd.Replacements), reason, err)
		}
		if got := instanceTypeNames(nc.InstanceTypeOptions); !slices.Equal(got, []string{"inf-a"}) {
			t.Fatalf("launch pinned to %v, want [inf-a]", got)
		}
		if got := zoneValues(nc); !slices.Equal(got, []string{"zone-a"}) {
			t.Fatalf("zone requirement = %v, want [zone-a]", got)
		}
	}
}
