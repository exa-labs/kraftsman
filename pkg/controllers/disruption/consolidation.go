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
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type consolidationTypeContextKey struct{}

func withConsolidationType(ctx context.Context, consolidationType string) context.Context {
	return context.WithValue(ctx, consolidationTypeContextKey{}, consolidationType)
}

func consolidationTypeFromContext(ctx context.Context) string {
	consolidationType, _ := ctx.Value(consolidationTypeContextKey{}).(string)
	return consolidationType
}

// commandValidationDelay is the time we wait between creating a consolidation command and validating that it still works.
const commandValidationDelay = 15 * time.Second

// MinInstanceTypesForSpotToSpotConsolidation is the default minimum number of instanceTypes in a
// NodeClaim needed to trigger spot-to-spot single-node consolidation, and the default cap on how
// many options a spot replacement launches from. The minimum is configurable per deployment
// (SPOT_TO_SPOT_MIN_INSTANCE_TYPES) because a workload pinned to one small instance family can
// never present 15 cheaper types; the launch cap follows the configured minimum so the launched
// type is always within the set consolidation priced against and is never immediately
// re-consolidatable, whatever the minimum is.
const MinInstanceTypesForSpotToSpotConsolidation = 15

// consolidation provides common functionality for single-node and multi-node consolidation.
type consolidation struct {
	// Consolidation needs to be aware of the queue for validation
	queue                  *Queue
	clock                  clock.Clock
	cluster                *state.Cluster
	kubeClient             client.Client
	provisioner            *provisioning.Provisioner
	cloudProvider          cloudprovider.CloudProvider
	recorder               events.Recorder
	lastConsolidationState time.Time
	// evaluator is initialized non-nil at construction. SetNodePoolTotals
	// replaces it with a balancedEvaluator carrying the new totals.
	evaluator Evaluator
}

// NodePoolTotalsSetter is implemented by disruption methods that use balanced scoring.
type NodePoolTotalsSetter interface {
	SetNodePoolTotals(map[string]NodePoolTotals)
}

func (c *consolidation) SetNodePoolTotals(totals map[string]NodePoolTotals) {
	c.evaluator = NewBalancedEvaluator(totals, c.recorder)
}

func MakeConsolidation(clock clock.Clock, cluster *state.Cluster, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cloudProvider cloudprovider.CloudProvider, recorder events.Recorder, queue *Queue) consolidation {
	return consolidation{
		queue:         queue,
		clock:         clock,
		cluster:       cluster,
		kubeClient:    kubeClient,
		provisioner:   provisioner,
		cloudProvider: cloudProvider,
		recorder:      recorder,
		evaluator:     noopEvaluator{},
	}
}

// IsConsolidated returns true if nothing has changed since markConsolidated was called.
func (c *consolidation) IsConsolidated() bool {
	return c.lastConsolidationState.Equal(c.cluster.ConsolidationState())
}

// markConsolidated records the current state of the cluster.
func (c *consolidation) markConsolidated() {
	c.lastConsolidationState = c.cluster.ConsolidationState()
}

// ShouldDisrupt is a predicate used to filter candidates
func (c *consolidation) ShouldDisrupt(ctx context.Context, cn *Candidate) bool {
	// Disable consolidation for static NodePool
	if cn.OwnedByStaticNodePool() {
		return false
	}
	// We need the following to know what the price of the instance for price comparison. If one of these doesn't exist, we can't
	// compute consolidation decisions for this candidate.
	// 1. Instance Type
	// 2. Capacity Type
	// 3. Zone
	if cn.instanceType == nil {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("Instance Type %q not found", cn.Labels()[corev1.LabelInstanceTypeStable]))...)
		return false
	}
	if _, ok := cn.Labels()[v1.CapacityTypeLabelKey]; !ok {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("Node does not have label %q", v1.CapacityTypeLabelKey))...)
		return false
	}
	if _, ok := cn.Labels()[corev1.LabelTopologyZone]; !ok {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("Node does not have label %q", corev1.LabelTopologyZone))...)
		return false
	}
	if cn.NodePool.Spec.Disruption.ConsolidateAfter.Duration == nil {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("NodePool %q has consolidation disabled", cn.NodePool.Name))...)
		return false
	}
	// Empty nodes are handled by Emptiness (reason "Empty") for correct budget accounting.
	if cn.IsEmpty() {
		return false
	}
	// WhenEmpty pools only allow empty-node deletions, which Emptiness handles.
	if cn.NodePool.Spec.Disruption.ConsolidationPolicy == v1.ConsolidationPolicyWhenEmpty {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("NodePool %q has consolidation policy WhenEmpty, but node is not empty", cn.NodePool.Name))...)
		return false
	}
	return cn.NodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable).IsTrue()
}

// sortCandidates sorts candidates by price/disruption ratio descending.
// The binary search in multi-node consolidation tries the first N candidates
// as a batch. Ratio sort means the batch contains the highest-value nodes,
// so budget-limited cycles execute the most impactful moves first.
//
// This changes multi-node behavior for WhenEmptyOrUnderutilized, which
// previously sorted by disruption cost ascending. The old sort found batches
// that were easy to pack (low-disruption nodes fit together). The new sort
// finds batches worth packing (high savings per unit disruption). The binary
// search still converges because it shrinks the window until scheduling
// succeeds.
func (c *consolidation) sortCandidates(_ context.Context, candidates []*Candidate) []*Candidate {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].SavingsRatio() > candidates[j].SavingsRatio()
	})
	return candidates
}

// consolidationSimulationOptions parameterizes a consolidation simulation. The zero value is the
// ordinary path; the split fallback re-runs the same code with a price limit on new capacity, an
// extra savings margin, and its own telemetry instead of the candidate skip counters.
type consolidationSimulationOptions struct {
	// newCapacityPriceLimit caps the price of instance types the simulation may launch new nodes from.
	newCapacityPriceLimit float64
	// minSavings is the fraction of the candidate price a replacement must save beyond being cheaper.
	minSavings float64
	// silent suppresses the per-candidate skip and replacement attempt metrics.
	silent bool
}

// computeConsolidation computes a consolidation action to take
func (c *consolidation) computeConsolidation(ctx context.Context, candidates ...*Candidate) (Command, error) {
	return c.computeConsolidationWithOptions(ctx, consolidationSimulationOptions{}, candidates...)
}

// errCandidateTimedOut reports a candidate abandoned by its own budget rather than by a failure.
var errCandidateTimedOut = errors.New("candidate simulation exceeded its budget")

// computeConsolidationWithinCandidateBudget evaluates one candidate under a deadline of its own.
//
// The pass timeout bounds discovery in aggregate, which leaves one pathological candidate free to
// spend the entire pass: a pass that hits its timeout mid-walk holding nothing returns nothing at
// all. Bounding each candidate makes the failure mode "this pass found fewer commands" rather than
// "this pass found none".
//
// The scheduler honors cancellation between pods, so an abandoned simulation stops promptly and
// its partial results are discarded rather than read as a verdict.
func (c *consolidation) computeConsolidationWithinCandidateBudget(ctx context.Context, candidate *Candidate) (Command, error) {
	budget := options.FromContext(ctx).ConsolidationCandidateTimeout
	if budget <= 0 {
		return c.computeConsolidation(ctx, candidate)
	}
	candidateCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	cmd, err := c.computeConsolidation(candidateCtx, candidate)
	if candidateBudgetExhausted(candidateCtx.Err(), ctx.Err()) {
		return Command{}, errCandidateTimedOut
	}
	return cmd, err
}

// candidateBudgetExhausted reports whether a finished simulation should be discarded because the
// candidate ran out of its own budget, rather than because the pass is shutting down. A canceled
// solve returns whatever it had placed so far, so an exhausted budget invalidates the result even
// when the simulation itself reported no error.
func candidateBudgetExhausted(candidateErr, parentErr error) bool {
	return candidateErr != nil && parentErr == nil
}

// nolint:gocyclo
func (c *consolidation) computeConsolidationWithOptions(ctx context.Context, simOpts consolidationSimulationOptions, candidates ...*Candidate) (Command, error) {
	consolidationType := consolidationTypeFromContext(ctx)
	observeSingleNodeSkip := func(reason string) {
		if len(candidates) == 1 && !simOpts.silent {
			// A no-op path that never named itself still gets counted, under the generic reason.
			observeCandidateSkip(consolidationType, candidates[0], lo.Ternary(reason == "", CandidateSkipNoOp, reason))
		}
	}
	var err error
	// Run scheduling simulation to compute consolidation option
	results, err := SimulateScheduling(ctx, c.kubeClient, c.cluster, c.provisioner, c.clock, c.recorder, consolidationSchedulerOptions(simOpts.newCapacityPriceLimit), candidates...)
	if err != nil {
		// if a candidate node is now deleting, just retry
		if errors.Is(err, errCandidateDeleting) {
			return Command{}, nil
		}
		return Command{}, err
	}

	// if not all of the pods were scheduled, we can't do anything
	if !results.AllNonPendingPodsScheduled() {
		observeSingleNodeSkip(CandidateSkipPodsDidNotSchedule)
		// This method is used by multi-node consolidation as well, so we'll only report in the single node case
		if len(candidates) == 1 && !simOpts.silent {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
		}
		return Command{}, nil
	}

	// were we able to schedule all the pods on the inflight candidates?
	if len(candidates) == 1 && !simOpts.silent {
		ObserveConsolidationReplacementAttempt(consolidationType, candidates[0].NodePool.Name, len(results.NewNodeClaims))
	}
	if len(results.NewNodeClaims) == 0 {
		return Command{
			Candidates:          candidates,
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts(candidates),
		}, nil
	}

	// A single candidate may be split into up to MaxConsolidationReplacements replacement nodes
	// (e.g. one large on-demand node into several smaller spot nodes). Multi-candidate
	// consolidation remains N->1: it merges many nodes into one replacement.
	maxReplacements := 1
	if len(candidates) == 1 {
		// CLI validation enforces >= 1, but clamp so a zero value from a directly-constructed
		// options struct fails safe to the classic 1->1 behavior
		maxReplacements = max(1, options.FromContext(ctx).MaxConsolidationReplacements)
	}
	// record the required replacement count for every single-candidate simulation needing more than one
	// replacement, whether or not it is within the configured limit
	if len(candidates) == 1 && len(results.NewNodeClaims) > 1 && !simOpts.silent {
		ConsolidationRequiredReplacements.Observe(float64(len(results.NewNodeClaims)), map[string]string{
			ConsolidationTypeLabel: consolidationType,
			metrics.NodePoolLabel:  candidates[0].NodePool.Name,
		})
	}
	if len(results.NewNodeClaims) > maxReplacements {
		if len(candidates) == 1 && !simOpts.silent {
			observeCandidateSkip(consolidationType, candidates[0], CandidateSkipMultipleReplacements)
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Can't remove without creating %d candidates", len(results.NewNodeClaims)))...)
		}
		return Command{}, nil
	}

	// get the current node price based on the offering
	// fallback if we can't find the specific zonal pricing data
	candidatePrice := sumCandidatePrices(candidates)
	// the replacement price a split fallback must beat, tightened by its savings margin
	replacementPriceBudget := candidatePrice * (1 - simOpts.minSavings)

	allExistingAreSpot := true
	for _, cn := range candidates {
		if cn.capacityType != v1.CapacityTypeSpot {
			allExistingAreSpot = false
		}
	}

	// sort the instanceTypes by price before we take any actions like truncation for spot-to-spot consolidation or finding the nodeclaim
	// that meets the minimum requirement after filteringByPrice
	for _, nc := range results.NewNodeClaims {
		nc.InstanceTypeOptions = nc.InstanceTypeOptions.OrderByPrice(nc.Requirements)
	}

	// If all candidates are spot and any replacement can be spot, route through the spot-to-spot path so its feature
	// gate and anti-churn protections apply. Replacement claims that can't satisfy the injected spot requirement will
	// end up with no instance type options there and the consolidation is skipped.
	if allExistingAreSpot &&
		lo.SomeBy(results.NewNodeClaims, func(nc *pscheduling.NodeClaim) bool {
			return nc.Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeSpot)
		}) {
		cmd, skipReason, err := c.computeSpotToSpotConsolidation(ctx, candidates, results, replacementPriceBudget, simOpts)
		if err == nil && cmd.Decision() == NoOpDecision {
			if splitCmd, ok := c.trySplitConsolidation(ctx, simOpts, candidatePrice, candidates); ok {
				return splitCmd, nil
			}
			observeSingleNodeSkip(skipReason)
		}
		return cmd, err
	}

	// The price filter can empty every replacement's options, so keep a copy to re-price against
	// spot-only offerings if it does.
	var spotRetrySnapshots [][]*cloudprovider.InstanceType
	if c.odToSpotRetryApplies(ctx, candidates, results.NewNodeClaims) {
		spotRetrySnapshots = lo.Map(results.NewNodeClaims, func(nc *pscheduling.NodeClaim, _ int) []*cloudprovider.InstanceType {
			return append([]*cloudprovider.InstanceType(nil), nc.InstanceTypeOptions...)
		})
	}

	// filterByPrice returns the instanceTypes that are lower priced than the current candidate and any error that indicates the input couldn't be filtered.
	// If we use this directly for spot-to-spot consolidation, we are bound to get repeated consolidations because the strategy that chooses to launch the spot instance from the list does
	// it based on availability and price which could result in selection/launch of non-lowest priced instance in the list. So, we would keep repeating this loop till we get to lowest priced instance
	// causing churns and landing onto lower available spot instance ultimately resulting in higher interruptions.
	// When the spot-only retry is armed, hold the "can't replace" event back until the retry also
	// fails: it may still turn the candidate into a replace command.
	if ok, skipReason, priceDetail := c.filterReplacementsAndPublish(results.NewNodeClaims, candidates, replacementPriceBudget, !simOpts.silent && spotRetrySnapshots == nil); !ok {
		if spotRetrySnapshots != nil {
			if !simOpts.silent {
				ObserveConsolidationODToSpotRetry(consolidationType, candidates, ODToSpotRetryOutcomeArmed)
			}
			if c.retrySpotOnlyReplacements(consolidationType, simOpts, candidates, results.NewNodeClaims, spotRetrySnapshots, replacementPriceBudget, options.FromContext(ctx).SpotToSpotMinInstanceTypes) {
				cmd := Command{
					Candidates:            candidates,
					Replacements:          replacementsFromNodeClaims(results.NewNodeClaims...),
					Results:               results,
					PoolDisruptionCosts:   computePoolDisruptionCosts(candidates),
					NewCapacityPriceLimit: simOpts.newCapacityPriceLimit,
				}
				cmd.EmitCandidateEvents(c.recorder)
				return cmd, nil
			}
			c.publishReplacementSkip(results.NewNodeClaims, candidates, skipReason, priceDetail, !simOpts.silent)
		}
		if splitCmd, ok := c.trySplitConsolidation(ctx, simOpts, candidatePrice, candidates); ok {
			return splitCmd, nil
		}
		observeSingleNodeSkip(skipReason)
		return Command{}, nil
	}

	// We are consolidating a node from OD -> [OD,Spot] but have filtered the instance types by cost based on the
	// assumption, that the spot variant will launch. We also need to add a requirement to the node to ensure that if
	// spot capacity is insufficient we don't replace the node with a more expensive on-demand node.  Instead the launch
	// should fail and we'll just leave the node alone. We don't need to do the same for reserved since the requirements
	// are injected on by the scheduler.
	for _, nc := range results.NewNodeClaims {
		ctReq := nc.Requirements.Get(v1.CapacityTypeLabelKey)
		if ctReq.Has(v1.CapacityTypeSpot) && ctReq.Has(v1.CapacityTypeOnDemand) {
			nc.Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
		}
	}

	cmd := Command{
		Candidates:            candidates,
		Replacements:          replacementsFromNodeClaims(results.NewNodeClaims...),
		Results:               results,
		PoolDisruptionCosts:   computePoolDisruptionCosts(candidates),
		NewCapacityPriceLimit: simOpts.newCapacityPriceLimit,
	}
	cmd.EmitCandidateEvents(c.recorder)

	return cmd, nil
}

// odToSpotRetryApplies reports whether the spot-only re-pricing retry is enabled and applicable:
// every candidate runs on-demand and every replacement claim may launch spot.
func (c *consolidation) odToSpotRetryApplies(ctx context.Context, candidates []*Candidate, newNodeClaims []*pscheduling.NodeClaim) bool {
	if !options.FromContext(ctx).ODToSpotConsolidation {
		return false
	}
	if lo.SomeBy(candidates, func(cd *Candidate) bool { return cd.capacityType != v1.CapacityTypeOnDemand }) {
		return false
	}
	return !lo.SomeBy(newNodeClaims, func(nc *pscheduling.NodeClaim) bool {
		return !nc.Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeSpot)
	})
}

// satisfiesMinValues reports whether pinning a requirement down to n values would still satisfy
// the requirement's declared minValues.
func satisfiesMinValues(r *scheduling.Requirement, n int) bool {
	return r == nil || r.MinValues == nil || *r.MinValues <= n
}

// retrySpotOnlyReplacements re-prices replacement claims that the ordinary filter emptied against
// spot offerings only. The ordinary filter prices each instance type at its worst-case compatible
// offering across every zone the claim allows, so one price-spiked spot pool anywhere in the fleet
// vetoes a replacement even when most zones offer large savings. The retry narrows each claim to
// spot and to the zones whose spot offerings beat the price budget, then re-prices: the launch is
// pinned to those zones and to spot, so the worst case it prices is the worst the launch can do,
// and insufficient spot capacity fails the launch rather than falling back to on-demand.
// It reports whether every claim retained a viable option; claims are mutated only on success
// being meaningful (callers discard them otherwise).
func (c *consolidation) retrySpotOnlyReplacements(consolidationType string, simOpts consolidationSimulationOptions, candidates []*Candidate, newNodeClaims []*pscheduling.NodeClaim, snapshots [][]*cloudprovider.InstanceType, priceBudget float64, spotLaunchCap int) bool {
	observeOutcome := func(outcome string) {
		if !simOpts.silent {
			ObserveConsolidationODToSpotRetry(consolidationType, candidates, outcome)
		}
	}
	// priceBudget is the aggregate across every replacement claim; a zone or type that is only cheap
	// relative to the whole budget would get pinned into one claim and inflate its worst-case price
	// past its share, so the narrowing below uses each claim's equal share. The aggregate filter at
	// the end remains the authoritative price guarantee.
	claimBudget := priceBudget / float64(len(newNodeClaims))
	for i, nc := range newNodeClaims {
		nc.InstanceTypeOptions = snapshots[i]
		// Requirements.Add keeps the larger minValues when intersecting, and the NodeClaim CRD rejects
		// an In requirement carrying fewer values than its minValues — pinning below that floor would
		// produce a command whose launch the API server refuses, so bail to the ordinary skip instead.
		if !satisfiesMinValues(nc.Requirements.Get(v1.CapacityTypeLabelKey), 1) {
			observeOutcome(ODToSpotRetryOutcomeCapacityTypeMinValues)
			return false
		}
		nc.Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
		// Anchor the zone restriction on the cheapest instance type's own cheap zones rather than the
		// union over every type: a zone that is cheap for one type but spiked for another would put
		// the spike right back into the other type's worst-case price and re-empty the claim.
		var zones []string
		for _, it := range nc.InstanceTypeOptions.Compatible(nc.Requirements).OrderByPrice(nc.Requirements) {
			cheap := lo.Uniq(lo.FilterMap(it.Offerings.Available().Compatible(nc.Requirements), func(of *cloudprovider.Offering, _ int) (string, bool) {
				return of.Zone(), of.Price < claimBudget
			}))
			if len(cheap) != 0 {
				zones = cheap
				break
			}
		}
		if len(zones) == 0 {
			observeOutcome(ODToSpotRetryOutcomeNoCheapZone)
			return false
		}
		if !satisfiesMinValues(nc.Requirements.Get(corev1.LabelTopologyZone), len(zones)) {
			observeOutcome(ODToSpotRetryOutcomeZoneMinValues)
			return false
		}
		nc.Requirements.Add(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zones...))
		// Types spiked inside the anchor's zones would fail the worst-case re-pricing for the whole
		// claim, so keep only the types cheap everywhere the launch may land. The anchor type survives
		// by construction: every zone kept is one of its own cheap zones.
		nc.InstanceTypeOptions = lo.Filter(nc.InstanceTypeOptions.Compatible(nc.Requirements), func(it *cloudprovider.InstanceType, _ int) bool {
			return it.Offerings.Available().WorstLaunchPrice(nc.Requirements) < claimBudget
		})
		nc.InstanceTypeOptions = nc.InstanceTypeOptions.OrderByPrice(nc.Requirements)
	}
	if ok, _, _ := c.filterReplacementsAndPublish(newNodeClaims, candidates, priceBudget, false); !ok {
		observeOutcome(ODToSpotRetryOutcomeAggregatePrice)
		return false
	}
	// The replacement lands on spot; cap its options like spot-to-spot consolidation does so the
	// launched type is within the priced set and doesn't churn straight back into consolidation.
	for _, nc := range newNodeClaims {
		truncateSpotInstanceTypeOptions(nc, spotLaunchCap)
	}
	observeOutcome(ODToSpotRetryOutcomeAdmitted)
	return true
}

// consolidationSchedulerOptions returns the scheduler options a consolidation simulation runs under, capping the
// price of the instance types new capacity may be launched from when the caller set a limit.
func consolidationSchedulerOptions(newCapacityPriceLimit float64) []pscheduling.Options {
	opts := []pscheduling.Options{pscheduling.IsConsolidationSimulation}
	if newCapacityPriceLimit > 0 {
		opts = append(opts, pscheduling.NewNodeClaimPriceLimit(newCapacityPriceLimit))
	}
	return opts
}

// Compute command to execute spot-to-spot consolidation if:
//  1. The SpotToSpotConsolidation feature flag is set to true.
//  2. For single-node consolidation:
//     a. There are at least 15 cheapest instance type replacement options to consolidate.
//     b. The current candidate is NOT part of the first 15 cheapest instance types inorder to avoid repeated consolidation.
//
// It returns the skip reason to attribute a no-op command to, so the caller can record why the
// candidate was passed over rather than that it was.
//
// nolint:unparam
func (c *consolidation) computeSpotToSpotConsolidation(ctx context.Context, candidates []*Candidate, results pscheduling.Results, candidatePrice float64, simOpts consolidationSimulationOptions) (Command, string, error) {
	publishEvents := !simOpts.silent

	// Spot consolidation is turned off.
	if !options.FromContext(ctx).FeatureGates.SpotToSpotConsolidation {
		if len(candidates) == 1 && publishEvents {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "SpotToSpotConsolidation is disabled, can't replace a spot node with a spot node")...)
		}
		return Command{}, CandidateSkipSpotToSpotDisabled, nil
	}

	// Since we are sure that the replacement nodeclaims considered for the spot candidates are spot, we will enforce it through the requirements.
	for _, nc := range results.NewNodeClaims {
		nc.Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
		// All possible replacements for the current candidate compatible with spot offerings
		nc.InstanceTypeOptions = nc.InstanceTypeOptions.Compatible(nc.Requirements)
	}

	// filterByPrice returns the instanceTypes that are lower priced than the current candidate and any error that indicates the input couldn't be filtered.
	if ok, skipReason, _ := c.filterReplacementsAndPublish(results.NewNodeClaims, candidates, candidatePrice, publishEvents); !ok {
		return Command{}, skipReason, nil
	}

	// For multi-node consolidation:
	// We don't have any requirement to check the remaining instance type flexibility, so exit early in this case.
	if len(candidates) > 1 {
		cmd := Command{
			Candidates:          candidates,
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts(candidates),
		}
		cmd.EmitCandidateEvents(c.recorder)

		return cmd, "", nil
	}

	// For single-node consolidation:

	// We check whether we have 15 cheaper instances than the current candidate instance. If this is the case, we know the following things:
	//   1) The current candidate is not in the set of the 15 cheapest instance types and
	//   2) There were at least 15 options cheaper than the current candidate.
	minInstanceTypes := options.FromContext(ctx).SpotToSpotMinInstanceTypes
	for _, nc := range results.NewNodeClaims {
		if len(nc.InstanceTypeOptions) < minInstanceTypes {
			if publishEvents {
				c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("SpotToSpotConsolidation requires %d cheaper instance type options than the current candidate to consolidate, got %d",
					minInstanceTypes, len(nc.InstanceTypeOptions)))...)
			}
			return Command{}, CandidateSkipSpotToSpotFlexibility, nil
		}
	}

	// If a user has minValues set in their NodePool requirements, then we cap the number of instancetypes at 100 which would be the actual number of instancetypes sent for launch to enable spot-to-spot consolidation.
	// If no minValues in the NodePool requirement, then we follow the configured minimum (default 15) to cap the instance types for launch to enable a spot-to-spot consolidation.
	// Restrict the InstanceTypeOptions for launch to the configured minimum so we don't get into a continual consolidation situation.
	// For example:
	// 1) Suppose we have 5 instance types, (A, B, C, D, E) in order of price with the minimum flexibility 3 and they’ll all work for our pod.  We send CreateInstanceFromTypes(A,B,C,D,E) and it gives us a E type based on price and availability of spot.
	// 2) We check if E is part of (A,B,C,D) and it isn't, so we will immediately have consolidation send a CreateInstanceFromTypes(A,B,C,D), since they’re cheaper than E.
	// 3) Assuming CreateInstanceFromTypes(A,B,C,D) returned D, we check if D is part of (A,B,C) and it isn't, so will have another consolidation send a CreateInstanceFromTypes(A,B,C), since they’re cheaper than D resulting in continual consolidation.
	// If we had restricted instance types to min flexibility at launch at step (1) i.e CreateInstanceFromTypes(A,B,C), we would have received the instance type part of the list preventing immediate consolidation.
	// The launch cap therefore follows the same configured minimum as the flexibility gate above: the launched
	// instance is always within the cheapest set of that size, so it can never be immediately consolidated again.
	for _, nc := range results.NewNodeClaims {
		truncateSpotInstanceTypeOptions(nc, minInstanceTypes)
	}

	cmd := Command{
		Candidates:            candidates,
		Replacements:          replacementsFromNodeClaims(results.NewNodeClaims...),
		Results:               results,
		PoolDisruptionCosts:   computePoolDisruptionCosts(candidates),
		NewCapacityPriceLimit: simOpts.newCapacityPriceLimit,
	}
	cmd.EmitCandidateEvents(c.recorder)

	return cmd, "", nil
}

// filterReplacementsAndPublish price-filters the replacement NodeClaims against the candidates' total price and
// returns whether all replacements still have viable instance type options, publishing an Unconsolidatable event for
// single-node candidates when they don't and events are enabled (a split fallback retry re-evaluates a candidate the
// ordinary path already reported on, so it stays silent). The second return value is the skip reason for the
// rejection, empty when the replacements survive; the third is the pricing detail captured for a single-node
// candidate before filtering (see cheapestWorstLaunchDetail), so a caller that suppressed events for a retry can
// still replay them with the detail attached.
func (c *consolidation) filterReplacementsAndPublish(newNodeClaims []*pscheduling.NodeClaim, candidates []*Candidate, candidatePrice float64, publishEvents bool) (bool, string, string) {
	noOptions := func() bool {
		return lo.SomeBy(newNodeClaims, func(nc *pscheduling.NodeClaim) bool { return len(nc.InstanceTypeOptions) == 0 })
	}
	// A replacement that arrives with nothing to price was excluded by its own requirements, not by
	// the market. Spot-to-spot narrows options to spot-compatible types just before this call, so
	// pricing an empty claim would report "nothing cheaper exists" for a compatibility failure.
	if noOptions() {
		c.publishCantReplace(newNodeClaims, candidates, publishEvents, "")
		return false, CandidateSkipNoCompatibleReplacement, ""
	}
	// The price filter empties the options it rejects, so the offering it compared against is only
	// recoverable before it runs.
	var priceDetail string
	if len(candidates) == 1 {
		priceDetail = cheapestWorstLaunchDetail(newNodeClaims[0], candidatePrice)
	}
	if err := filterReplacementsByAggregatePrice(newNodeClaims, candidatePrice); err != nil {
		if len(candidates) == 1 && publishEvents {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Filtering by price: %v", err))...)
		}
		// RemoveInstanceTypeOptionsByPriceAndMinValues only errors out of SatisfiesMinValues, and it
		// reaches that check having kept every option under the ceiling: cheaper capacity exists and
		// the surviving set is too narrow for minValues.
		return false, CandidateSkipReplacementFlexibility, ""
	}
	if noOptions() {
		c.publishCantReplace(newNodeClaims, candidates, publishEvents, priceDetail)
		// Nothing survived the price ceiling, so the fleet is waiting on cheaper offerings. One
		// replacement means one node of the candidate's shape fits its pods and no such node is
		// cheaper, which is what the split fallback exists to serve.
		if len(newNodeClaims) == 1 {
			return false, CandidateSkipNoCheaperSingleReplacement, priceDetail
		}
		return false, CandidateSkipNoCheaperReplacementSet, priceDetail
	}
	return true, "", ""
}

// publishCantReplace reports a single-node candidate that consolidation found no replacement for, appending the
// pricing detail describing the offering the comparison was made against when the caller captured one.
func (c *consolidation) publishCantReplace(newNodeClaims []*pscheduling.NodeClaim, candidates []*Candidate, publishEvents bool, priceDetail string) {
	if len(candidates) != 1 || !publishEvents {
		return
	}
	c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Can't replace with %s%s", lo.Ternary(len(newNodeClaims) == 1, "a cheaper node", "cheaper nodes"), priceDetail))...)
}

// worstLaunchOffering returns the offering WorstLaunchPrice prices an instance type at under the given
// requirements: the most expensive compatible offering of the preferred capacity type (reserved, then spot, then
// on-demand), or nil when none is compatible.
func worstLaunchOffering(it *cloudprovider.InstanceType, reqs scheduling.Requirements) *cloudprovider.Offering {
	ofs := it.Offerings.Available().Compatible(reqs)
	for _, ctReqs := range []scheduling.Requirements{cloudprovider.ReservedRequirement, cloudprovider.SpotRequirement, cloudprovider.OnDemandRequirement} {
		if compat := ofs.Compatible(ctReqs); len(compat) != 0 {
			return compat.MostExpensive()
		}
	}
	return nil
}

// cheapestWorstLaunchDetail renders, for a skip event, the best worst-case launch the price filter could have
// accepted for a replacement claim: the instance type whose worst-case offering is cheapest, the capacity type and
// zone of that offering, and the budget it had to beat. This makes the market comparison behind a
// "Can't replace with a cheaper node" event auditable — in particular, which capacity type was priced and which
// zone set the worst case.
func cheapestWorstLaunchDetail(nc *pscheduling.NodeClaim, budget float64) string {
	var best *cloudprovider.Offering
	var bestName string
	for _, it := range nc.InstanceTypeOptions {
		if of := worstLaunchOffering(it, nc.Requirements); of != nil && (best == nil || of.Price < best.Price) {
			best, bestName = of, it.Name
		}
	}
	if best == nil {
		return ""
	}
	return fmt.Sprintf(" (cheapest option %s prices worst-case at $%g for %s in %s, budget $%g)", bestName, best.Price, best.CapacityType(), best.Zone(), budget)
}

// publishReplacementSkip replays the event the ordinary filter would have published, matched to the
// skip reason it returned, once the spot-only retry has also failed to produce a command.
func (c *consolidation) publishReplacementSkip(newNodeClaims []*pscheduling.NodeClaim, candidates []*Candidate, skipReason, priceDetail string, publishEvents bool) {
	if skipReason == CandidateSkipReplacementFlexibility {
		if len(candidates) == 1 && publishEvents {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "Filtering by price: replacement instance type options do not satisfy the minValues requirements")...)
		}
		return
	}
	c.publishCantReplace(newNodeClaims, candidates, publishEvents, priceDetail)
}

// truncateSpotInstanceTypeOptions caps a replacement NodeClaim's instance type options to the maxOptions cheapest
// (or the minimum required to satisfy minValues, if greater) so the launched instance is always within the set
// consolidation priced against, preventing continual spot-to-spot consolidation churn. maxOptions is the same
// configured minimum the flexibility gate checks, so a launch can never land on a type outside the cheapest set of
// that size and become eligible for another immediate cheaper replacement.
func truncateSpotInstanceTypeOptions(nc *pscheduling.NodeClaim, maxOptions int) {
	if nc.Requirements.HasMinValues() {
		// Here we are trying to get the max of the minimum instances required to satisfy the minimum requirement and the configured cap for spot-to-spot consolidation.
		minInstanceTypes, _, _ := nc.InstanceTypeOptions.SatisfiesMinValues(nc.Requirements)
		nc.InstanceTypeOptions = lo.Slice(nc.InstanceTypeOptions, 0, lo.Max([]int{maxOptions, minInstanceTypes}))
	} else {
		nc.InstanceTypeOptions = lo.Slice(nc.InstanceTypeOptions, 0, maxOptions)
	}
}

// filterReplacementsByAggregatePrice filters each replacement NodeClaim's instance type options so that the
// worst-case total launch price across all replacements stays below candidatePrice. Each claim's price budget is
// its cheapest launch price plus an equal share of the surplus budget, so any combination of retained options is
// guaranteed to sum below candidatePrice. With a single replacement this reduces to the classic
// RemoveInstanceTypeOptionsByPriceAndMinValues(reqs, candidatePrice) behavior.
//
// Claims left with no options are how it reports that nothing is cheaper; the only error it returns is a minValues
// failure from a claim whose cheaper options were kept.
func filterReplacementsByAggregatePrice(newNodeClaims []*pscheduling.NodeClaim, candidatePrice float64) error {
	cheapest := make([]float64, len(newNodeClaims))
	cheapestTotal := 0.0
	for i, nc := range newNodeClaims {
		cheapest[i] = math.MaxFloat64
		for _, it := range nc.InstanceTypeOptions {
			if p := it.Offerings.Available().WorstLaunchPrice(nc.Requirements); p < cheapest[i] {
				cheapest[i] = p
			}
		}
		cheapestTotal += cheapest[i]
	}
	if cheapestTotal >= candidatePrice {
		// No combination of replacements can beat the candidate's price; empty all options so the caller
		// reports "can't replace with cheaper node(s)".
		for _, nc := range newNodeClaims {
			nc.InstanceTypeOptions = nil
		}
		return nil
	}
	// Give each claim its cheapest launch price plus an equal share of the surplus budget: the budgets sum to
	// candidatePrice, every claim's budget strictly exceeds its cheapest option (even zero-priced ones), and any
	// combination of retained options sums below candidatePrice.
	surplusShare := (candidatePrice - cheapestTotal) / float64(len(newNodeClaims))
	for i, nc := range newNodeClaims {
		// Use candidatePrice directly for a single replacement: the surplus form is equivalent in exact arithmetic but
		// floating point rounding could let an equally-priced option survive the strict < comparison.
		maxPrice := candidatePrice
		if len(newNodeClaims) > 1 {
			maxPrice = cheapest[i] + surplusShare
		}
		if _, err := nc.RemoveInstanceTypeOptionsByPriceAndMinValues(nc.Requirements, maxPrice); err != nil {
			return err
		}
	}
	return nil
}
