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
	"slices"
	"sort"

	"github.com/awslabs/operatorpkg/status"
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
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// Drift is a subreconciler that deletes drifted candidates.
type Drift struct {
	kubeClient  client.Client
	cluster     *state.Cluster
	provisioner *provisioning.Provisioner
	recorder    events.Recorder
	clock       clock.Clock
}

func NewDrift(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, recorder events.Recorder, clk clock.Clock) *Drift {
	return &Drift{
		kubeClient:  kubeClient,
		cluster:     cluster,
		provisioner: provisioner,
		recorder:    recorder,
		clock:       clk,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return !c.OwnedByStaticNodePool() && c.NodeClaim.StatusConditions().Get(string(d.Reason())).IsTrue()
}

// ComputeCommand generates a disruption command given candidates
func (d *Drift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	// On-demand lease reclaims go first: they swap expensive fallback capacity for spot and would otherwise queue
	// behind every NodeClaim a template change marked drifted at once. Within each tier, oldest drift first.
	sort.Slice(candidates, func(i int, j int) bool {
		ci, cj := candidates[i].NodeClaim.StatusConditions().Get(string(d.Reason())), candidates[j].NodeClaim.StatusConditions().Get(string(d.Reason()))
		if li, lj := isLeaseReclaim(ci), isLeaseReclaim(cj); li != lj {
			return li
		}
		return ci.LastTransitionTime.Time.Before(cj.LastTransitionTime.Time)
	})

	emptyCandidates, nonEmptyCandidates := lo.FilterReject(candidates, func(c *Candidate, _ int) bool {
		return len(c.reschedulablePods) == 0
	})

	// Prioritize empty candidates since we want them to get priority over non-empty candidates if the budget is constrained.
	// Disrupting empty candidates first also helps reduce the overall churn because if a non-empty candidate is disrupted first,
	// the pods from that node can reschedule on the empty nodes and will need to move again when those nodes get disrupted.
	for _, candidate := range slices.Concat(emptyCandidates, nonEmptyCandidates) {
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Check if we need to create any NodeClaims.
		results, err := SimulateScheduling(ctx, d.kubeClient, d.cluster, d.provisioner, d.clock, d.recorder, nil, candidate)
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return []Command{}, err
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}
		// A lease reclaim exists only to swap on-demand for spot: pin its replacements to spot so an
		// insufficient-capacity launch fails the command (and the on-demand node stays) instead of
		// falling back to another on-demand node. Skip candidates whose replacements cannot be pinned.
		if isLeaseReclaim(candidate.NodeClaim.StatusConditions().Get(string(d.Reason()))) && !pinReplacementsToSpot(results.NewNodeClaims) {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, "on-demand lease reclaim requires replacements that can launch spot")...)
			continue
		}

		cmd := Command{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		}
		return []Command{cmd}, nil

	}
	return []Command{}, nil
}

// isLeaseReclaim reports whether a Drifted condition was raised because the node is on-demand capacity whose
// spot-fallback lease expired (cloudprovider.DriftReasonOnDemandLeaseExpired).
func isLeaseReclaim(drifted *status.Condition) bool {
	return drifted != nil && drifted.Reason == string(cloudprovider.DriftReasonOnDemandLeaseExpired)
}

// pinReplacementsToSpot narrows every replacement NodeClaim's capacity type to spot. It reports false, leaving
// the claims untouched, when any replacement cannot launch spot or pinning would violate its minValues.
func pinReplacementsToSpot(newNodeClaims []*pscheduling.NodeClaim) bool {
	for _, nc := range newNodeClaims {
		ctReq := nc.Requirements.Get(v1.CapacityTypeLabelKey)
		if !ctReq.Has(v1.CapacityTypeSpot) || !satisfiesMinValues(ctReq, 1) {
			return false
		}
	}
	for _, nc := range newNodeClaims {
		nc.Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	}
	return true
}

func (d *Drift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *Drift) Class() string {
	return EventualDisruptionClass
}

func (d *Drift) ConsolidationType() string {
	return ""
}
