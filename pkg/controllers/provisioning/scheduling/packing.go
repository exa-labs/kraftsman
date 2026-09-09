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

// Packing policy: how a pending pod is placed once no existing node takes it.
//
// Under the default "binpack" policy the pod joins the first in-flight NodeClaim it fits, and a new NodeClaim is
// opened only when none fits. Under "marginal-cost" the scheduler prices each option: growing an in-flight NodeClaim
// costs the increase in its cheapest launch price (adding a pod can only remove instance types, so the increase is
// never negative), opening a new NodeClaim costs that NodeClaim's cheapest launch price, and the pod takes the cheapest
// option with ties going to the in-flight NodeClaim. Pools whose sizes scale price linearly therefore pack exactly as
// under "binpack", while pools whose small sizes are cheaper per unit of the scarce resource split into several small
// NodeClaims instead of one large one. The rule is greedy per pod: where a large size is cheaper per unit than the
// small ones, the first pod that would step the in-flight NodeClaim up to it sees the whole step as its own cost and
// opens a new small NodeClaim instead, so such volume discounts are only captured by pods that need the large size
// outright.
//
// The policy is per NodePool. A scheduler with no marginal-cost NodePool runs the original binpack code path
// unchanged. Once any NodePool opts in, every pod is priced, but a NodeClaim of a binpack NodePool always has a
// marginal price of zero, so it still absorbs any pod that fits (the first such NodeClaim in the usual
// fewest-pods-first order, exactly as under binpack) and only marginal-cost NodeClaims compete on price.

package scheduling

import (
	"context"
	"fmt"
	"math"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling/dynamicresources"
)

// PackingPolicy is a NodePool's value for v1.NodePoolPackingPolicyAnnotationKey.
type PackingPolicy string

const (
	PackingPolicyBinpack      PackingPolicy = "binpack"
	PackingPolicyMarginalCost PackingPolicy = "marginal-cost"
)

// priceTieTolerance is the relative difference below which two launch prices count as equal, so that price ladders
// that double exactly in decimal but not in binary floating point still tie.
const priceTieTolerance = 1e-6

// PackingPolicyForNodePool reads the NodePool's packing policy annotation. A missing or empty annotation selects
// PackingPolicyBinpack; an unrecognized value also selects it and is reported through the returned error.
func PackingPolicyForNodePool(np *v1.NodePool) (PackingPolicy, error) {
	value := np.Annotations[v1.NodePoolPackingPolicyAnnotationKey]
	switch PackingPolicy(value) {
	case "", PackingPolicyBinpack:
		return PackingPolicyBinpack, nil
	case PackingPolicyMarginalCost:
		return PackingPolicyMarginalCost, nil
	}
	return PackingPolicyBinpack, fmt.Errorf("unrecognized %s annotation value %q, expected %q or %q", v1.NodePoolPackingPolicyAnnotationKey, value, PackingPolicyBinpack, PackingPolicyMarginalCost)
}

// resolvePackingPolicy is PackingPolicyForNodePool with the invalid-annotation fallback surfaced as a NodePool event
// and a log line, for use while building a scheduler.
func resolvePackingPolicy(ctx context.Context, recorder events.Recorder, np *v1.NodePool) PackingPolicy {
	policy, err := PackingPolicyForNodePool(np)
	if err != nil {
		recorder.Publish(InvalidPackingPolicyEvent(np, err))
		log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Error(err, "using the binpack packing policy")
	}
	return policy
}

// placement is a NodeClaim that accepts a pod, together with the state NodeClaim.CanAdd computed for the pod so that
// the decision can be committed later without re-evaluating it.
type placement struct {
	nodeClaim          *NodeClaim
	requirements       scheduling.Requirements
	instanceTypes      []*cloudprovider.InstanceType
	offeringsToReserve []*cloudprovider.Offering
	allocationResult   *dynamicresources.AllocationResult
}

func (p *placement) commit(ctx context.Context, pod *corev1.Pod, podData *PodData, allocator *dynamicresources.Allocator) {
	p.nodeClaim.Add(ctx, pod, podData, p.requirements, p.instanceTypes, p.offeringsToReserve, p.allocationResult, allocator)
}

// inflightPlacement is a placement onto an in-flight NodeClaim priced for the marginal-cost policy. delta is the
// increase in the NodeClaim's launch price if the pod joins it; it is zero for NodeClaims of binpack NodePools, and
// zero with unpriced set when either the current or the grown NodeClaim has no priced offering, so that missing
// pricing data degrades to binpack behavior rather than to arbitrary splitting.
type inflightPlacement struct {
	placement
	delta    float64
	unpriced bool
}

// launchPrice is the cheapest price at which a NodeClaim with these instance types and requirements can launch. A
// reserved offering is priced only when it is among the offerings reserved for the NodeClaim: the ReservationManager
// tracks reservation consumption apart from Offering.Available, and a NodeClaim that could reserve nothing launches
// as spot or on-demand (FinalizeScheduling pins the capacity type to reserved only when reservations were made). ok
// is false when no instance type has a compatible available offering to price.
func launchPrice(instanceTypes []*cloudprovider.InstanceType, requirements scheduling.Requirements, reserved []*cloudprovider.Offering) (price float64, ok bool) {
	reservationIDs := sets.New(lo.Map(reserved, func(o *cloudprovider.Offering, _ int) string { return o.ReservationID() })...)
	price = math.MaxFloat64
	for _, it := range instanceTypes {
		launchable := cloudprovider.Offerings(lo.Filter(it.Offerings.Available(), func(o *cloudprovider.Offering, _ int) bool {
			return o.CapacityType() != v1.CapacityTypeReserved || reservationIDs.Has(o.ReservationID())
		}))
		if p := launchable.CheapestLaunchPrice(requirements); p < price {
			price = p
		}
	}
	return price, price != math.MaxFloat64
}

// marginalLaunchPrice is the increase in nc's launch price if its instance types, requirements and reserved offerings
// become instanceTypes, requirements and reserved. unpriced is set when either side cannot be priced.
func marginalLaunchPrice(nc *NodeClaim, instanceTypes []*cloudprovider.InstanceType, requirements scheduling.Requirements, reserved []*cloudprovider.Offering) (delta float64, unpriced bool) {
	before, okBefore := launchPrice(nc.InstanceTypeOptions, nc.Requirements, nc.reservedOfferings)
	after, okAfter := launchPrice(instanceTypes, requirements, reserved)
	if !okBefore || !okAfter {
		return 0, true
	}
	return math.Max(after-before, 0), false
}

// cheaperThan reports whether price a is lower than price b by more than priceTieTolerance, relative to the larger
// of the two.
func cheaperThan(a, b float64) bool {
	return b-a > priceTieTolerance*math.Max(math.Abs(a), math.Abs(b))
}
