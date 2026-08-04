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
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

type ValidationError struct {
	error
}

func NewValidationError(err error) *ValidationError {
	return &ValidationError{error: err}
}

func IsValidationError(err error) bool {
	if err == nil {
		return false
	}
	var validationError *ValidationError
	return errors.As(err, &validationError)
}

// BudgetValidationError indicates validation failed due to disruption budget constraints
type BudgetValidationError struct {
	*ValidationError
}

func NewBudgetValidationError(err error) *BudgetValidationError {
	return &BudgetValidationError{ValidationError: NewValidationError(err)}
}

func (e *BudgetValidationError) Unwrap() error {
	return e.ValidationError
}

// SchedulingValidationError indicates validation failed due to scheduling constraints
type SchedulingValidationError struct {
	*ValidationError
}

func NewSchedulingValidationError(err error) *SchedulingValidationError {
	return &SchedulingValidationError{ValidationError: NewValidationError(err)}
}

func (e *SchedulingValidationError) Unwrap() error {
	return e.ValidationError
}

// ChurnValidationError indicates validation failed due to churn detection
type ChurnValidationError struct {
	*ValidationError
}

func NewChurnValidationError(err error) *ChurnValidationError {
	return &ChurnValidationError{ValidationError: NewValidationError(err)}
}

func (e *ChurnValidationError) Unwrap() error {
	return e.ValidationError
}

type Validator interface {
	Validate(context.Context, Command, time.Duration) (Command, error)
}

// Validation is used to perform validation on a consolidation command.  It makes an assumption that when re-used, all
// of the commands passed to IsValid were constructed based off of the same consolidation state.  This allows it to
// skip the validation TTL for all but the first command.
type validation struct {
	clock         clock.Clock
	cluster       *state.Cluster
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	provisioner   *provisioning.Provisioner
	recorder      events.Recorder
	queue         *Queue
	reason        v1.DisruptionReason
}

type EmptinessValidator struct {
	validation
	filter         CandidateFilter
	validationType string
}

func NewEmptinessValidator(c consolidation) *EmptinessValidator {
	e := &Emptiness{consolidation: c}
	return &EmptinessValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonEmpty,
		},
		filter:         e.ShouldDisrupt,
		validationType: e.ConsolidationType(),
	}
}

func (e *EmptinessValidator) Validate(ctx context.Context, cmd Command, validationPeriod time.Duration) (Command, error) {
	if validationPeriod > 0 {
		select {
		case <-ctx.Done():
			return Command{}, errors.New("interrupted")
		case <-e.clock.After(validationPeriod):
		}
	}
	validatedCandidates, err := e.validateCandidates(ctx, cmd.Candidates...)
	if err != nil {
		return Command{}, err
	}
	cmd.Candidates = validatedCandidates
	return cmd, nil
}

type ConsolidationValidator struct {
	validation
	filter         CandidateFilter
	validationType string
}

func NewSingleConsolidationValidator(c consolidation) *ConsolidationValidator {
	s := &SingleNodeConsolidation{consolidation: c}
	return &ConsolidationValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonUnderutilized,
		},
		filter:         s.ShouldDisrupt,
		validationType: s.ConsolidationType(),
	}
}

func NewMultiConsolidationValidator(c consolidation) *ConsolidationValidator {
	m := &MultiNodeConsolidation{consolidation: c}
	return &ConsolidationValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonUnderutilized,
		},
		filter:         m.ShouldDisrupt,
		validationType: m.ConsolidationType(),
	}
}

func (c *ConsolidationValidator) Validate(ctx context.Context, cmd Command, validationPeriod time.Duration) (Command, error) {
	if err := c.isValid(ctx, cmd, validationPeriod); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (c *ConsolidationValidator) isValid(ctx context.Context, cmd Command, validationPeriod time.Duration) error {
	if validationPeriod > 0 {
		endWaitStage := startPassStage(ctx, stageValidationWait)
		select {
		case <-ctx.Done():
			endWaitStage()
			return errors.New("context canceled")
		case <-c.clock.After(validationPeriod):
		}
		endWaitStage()
		// The wait exists to observe churn, so the post-wait re-simulation must not reuse
		// topology pod/node reads pinned earlier in the pass, nor the backlog and budgets the
		// pass read before the walk began.
		if scheduling.TopologyPassCacheFromContext(ctx) != nil {
			ctx = scheduling.WithTopologyPassCache(ctx, scheduling.NewTopologyPassCache())
		}
		if scheduling.InverseAffinityCacheFromContext(ctx) != nil {
			ctx = scheduling.WithInverseAffinityCache(ctx, scheduling.NewInverseAffinityCache())
		}
		if PassReadsFromContext(ctx) != nil {
			ctx = WithPassReads(ctx, NewPassReads())
		}
	}
	candidateValidationStart := time.Now()
	validatedCandidates, err := c.validateCandidates(ctx, cmd.Candidates...)
	observePassStage(ctx, stageValidation, candidateValidationStart)
	if err != nil {
		return err
	}
	// validateCommand runs a nested SimulateScheduling, which accounts for its own time under the
	// state_copy/pod_gather/scheduler_construction/simulation stages.
	if err := c.validateCommand(ctx, cmd, validatedCandidates); err != nil {
		return err
	}
	// Revalidate candidates after validating the command. This mitigates the chance of a race condition outlined in
	// the following GitHub issue: https://github.com/kubernetes-sigs/karpenter/issues/1167.
	revalidationStart := time.Now()
	_, err = c.validateCandidates(ctx, validatedCandidates...)
	observePassStage(ctx, stageValidation, revalidationStart)
	if err != nil {
		return err
	}
	return nil
}

func (e *EmptinessValidator) validateCandidates(ctx context.Context, candidates ...*Candidate) ([]*Candidate, error) {
	// This GetCandidates call filters out nodes that were nominated
	validatedCandidates, err := GetCandidates(ctx, e.cluster, e.kubeClient, e.recorder, e.clock, e.cloudProvider, e.filter, GracefulDisruptionClass, e.queue)
	if err != nil {
		return nil, fmt.Errorf("constructing validation candidates, %w", err)
	}
	validatedCandidates = mapCandidates(candidates, validatedCandidates)
	if len(validatedCandidates) == 0 {
		FailedValidationsTotal.Add(float64(len(candidates)), map[string]string{ConsolidationTypeLabel: e.validationType})
		return nil, NewChurnValidationError(fmt.Errorf("%d candidates are no longer valid", len(candidates)))
	}
	disruptionBudgetMapping, err := BuildDisruptionBudgetMapping(ctx, e.cluster, e.clock, e.kubeClient, e.cloudProvider, e.recorder, e.reason)
	if err != nil {
		return nil, fmt.Errorf("building disruption budgets, %w", err)
	}

	if valid := lo.Filter(validatedCandidates, func(cn *Candidate, _ int) bool {
		if e.cluster.IsNodeNominated(cn.ProviderID()) {
			FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: e.validationType})
			return false
		}
		if disruptionBudgetMapping[cn.NodePool.Name] == 0 {
			FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: e.validationType})
			return false
		}
		disruptionBudgetMapping[cn.NodePool.Name]--
		return true
	}); len(valid) > 0 {
		return valid, nil
	}
	return nil, NewBudgetValidationError(fmt.Errorf("%d candidates failed validation because it they were nominated for a pod or would violate disruption budgets", len(candidates)))
}

// ValidateCandidates gets the current representation of the provided candidates and ensures that they are all still valid.
// For a candidate to still be valid, the following conditions must be met:
//
//	a. It must pass the global candidate filtering logic (no blocking PDBs, no do-not-disrupt annotation, etc)
//	b. It must not have any pods nominated for it
//	c. It must still be disruptable without violating node disruption budgets
//
// If these conditions are met for all candidates, ValidateCandidates returns a slice with the updated representations.
func (c *ConsolidationValidator) validateCandidates(ctx context.Context, candidates ...*Candidate) ([]*Candidate, error) {
	// GracefulDisruptionClass is hardcoded here because ValidateCandidates is only used for consolidation disruption. All consolidation disruption is graceful disruption.
	validatedCandidates, err := GetCandidates(ctx, c.cluster, c.kubeClient, c.recorder, c.clock, c.cloudProvider, c.filter, GracefulDisruptionClass, c.queue)
	if err != nil {
		return nil, fmt.Errorf("constructing validation candidates, %w", err)
	}
	validatedCandidates = mapCandidates(candidates, validatedCandidates)
	// If we filtered out any candidates, return nil as some NodeClaims in the consolidation decision have changed.
	if len(validatedCandidates) != len(candidates) {
		FailedValidationsTotal.Add(float64(len(candidates)), map[string]string{ConsolidationTypeLabel: c.validationType})
		return nil, NewChurnValidationError(fmt.Errorf("%d candidates are no longer valid", len(candidates)-len(validatedCandidates)))
	}
	disruptionBudgetMapping, err := BuildDisruptionBudgetMapping(ctx, c.cluster, c.clock, c.kubeClient, c.cloudProvider, c.recorder, c.reason)
	if err != nil {
		return nil, fmt.Errorf("building disruption budgets, %w", err)
	}
	// Return nil if any candidate meets either of the following conditions:
	//  a. A pod was nominated to the candidate
	//  b. Disrupting the candidate would violate node disruption budgets
	for _, vc := range validatedCandidates {
		if c.cluster.IsNodeNominated(vc.ProviderID()) {
			FailedValidationsTotal.Add(float64(len(candidates)), map[string]string{ConsolidationTypeLabel: c.validationType})
			return nil, NewBudgetValidationError(fmt.Errorf("a candidate was nominated during validation"))
		}
		if disruptionBudgetMapping[vc.NodePool.Name] == 0 {
			FailedValidationsTotal.Add(float64(len(candidates)), map[string]string{ConsolidationTypeLabel: c.validationType})
			return nil, NewBudgetValidationError(fmt.Errorf("a candidate can no longer be disrupted without violating budgets"))
		}
		disruptionBudgetMapping[vc.NodePool.Name]--
	}
	return validatedCandidates, nil
}

// ValidateCommand validates a command for a Method
func (v *validation) validateCommand(ctx context.Context, cmd Command, candidates []*Candidate) error {
	// None of the chosen candidate are valid for execution, so retry
	if len(candidates) == 0 {
		return NewValidationError(fmt.Errorf("no candidates"))
	}
	// Re-simulate under the same new-capacity price ceiling the command was computed with, so a split fallback
	// command is checked against the packing it came from rather than the single-replacement packing the
	// unlimited simulation always produces for those candidates.
	results, err := SimulateScheduling(ctx, v.kubeClient, v.cluster, v.provisioner, v.clock, v.recorder, consolidationSchedulerOptions(cmd.NewCapacityPriceLimit), candidates...)
	if err != nil {
		return fmt.Errorf("simluating scheduling, %w", err)
	}
	if !results.AllNonPendingPodsScheduled() {
		return NewSchedulingValidationError(errors.New(results.NonPendingPodSchedulingErrors()))
	}

	// We want to ensure that the re-simulated scheduling using the current cluster state produces the same result.
	// There are three possible options for the number of new candidates that we need to handle:
	// len(NewNodeClaims) == 0, as long as we weren't expecting a new node, this is valid
	// len(NewNodeClaims) != len(cmd.Replacements), something in the cluster changed so that deleting the candidates
	//                    now requires a different number of replacement nodes than the command was computed with
	// len(NewNodeClaims) == len(cmd.Replacements), as long as the nodes look like what we were expecting, this is valid
	if len(results.NewNodeClaims) == 0 {
		if len(cmd.Replacements) == 0 {
			// scheduling produced zero new NodeClaims and we weren't expecting any, so this is valid.
			return nil
		}
		// if it produced no new NodeClaims, but we were expecting one we should re-simulate as there is likely a better
		// consolidation option now
		return NewSchedulingValidationError(fmt.Errorf("scheduling simulation produced new results"))
	}

	// the simulation must produce exactly the number of replacements the command intends to launch
	if len(results.NewNodeClaims) != len(cmd.Replacements) {
		return NewSchedulingValidationError(fmt.Errorf("scheduling simulation produced new results"))
	}

	// We know that the scheduling simulation wants to create new nodes and that the command we are verifying wants
	// to create the same number of nodes. The scheduling simulation doesn't apply any filtering to instance types, so
	// it may include instance types that we don't want to launch which were filtered out when the lifecycleCommand was
	// created. To check if our lifecycleCommand is valid, we just want to ensure that each replacement's instance
	// types are a subset of what scheduling says we should create for some distinct simulated NodeClaim. We check for
	// a subset since the scheduling simulation here does no price filtering, so it will include more expensive types.
	//
	// This is necessary since consolidation only wants cheaper NodeClaims.  Suppose consolidation determined we should delete
	// a 4xlarge and replace it with a 2xlarge. If things have changed and the scheduling simulation we just performed
	// now says that we need to launch a 4xlarge. It's still launching the correct number of NodeClaims, but it's just
	// as expensive or possibly more so we shouldn't validate.
	if !replacementsMatchSimulation(cmd.Replacements, results.NewNodeClaims) {
		return NewSchedulingValidationError(fmt.Errorf("scheduling simulation produced new results"))
	}
	return nil
}

// replacementsMatchSimulation reports whether there is a one-to-one matching between the command's replacements and
// the simulated NodeClaims such that each replacement could still satisfy its matched simulated NodeClaim: same
// NodePool, same taints, scheduling requirements contained in the simulated claim's, and instance type options that are a subset of the simulated
// NodeClaim's options. Uses Hopcroft-Karp maximum bipartite matching (O(E*sqrt(V))), which is polynomial in the
// number of replacements, so it stays cheap even for large MAX_CONSOLIDATION_REPLACEMENTS values.
func replacementsMatchSimulation(replacements []*Replacement, newNodeClaims []*scheduling.NodeClaim) bool {
	adjacency := make([][]int, len(replacements))
	for i, r := range replacements {
		for j, nc := range newNodeClaims {
			if replacementMatchesSimulatedNodeClaim(r, nc) {
				adjacency[i] = append(adjacency[i], j)
			}
		}
	}
	return hopcroftKarpMatchingSize(adjacency, len(newNodeClaims)) == len(replacements)
}

// hopcroftKarpMatchingSize returns the size of a maximum bipartite matching for the given left-to-right adjacency
// lists. Each phase runs one BFS to layer the graph by shortest alternating path from unmatched left vertices, then
// augments along vertex-disjoint shortest paths with DFS; at most O(sqrt(V)) phases are needed.
func hopcroftKarpMatchingSize(adjacency [][]int, rightSize int) int {
	const unmatched = -1
	unlayered := len(adjacency) + 1 // strictly greater than any reachable BFS layer
	matchLeft := make([]int, len(adjacency))
	matchRight := make([]int, rightSize)
	for i := range matchLeft {
		matchLeft[i] = unmatched
	}
	for j := range matchRight {
		matchRight[j] = unmatched
	}
	layer := make([]int, len(adjacency))
	queue := make([]int, 0, len(adjacency))

	bfs := func() bool {
		queue = queue[:0]
		for u := range adjacency {
			if matchLeft[u] == unmatched {
				layer[u] = 0
				queue = append(queue, u)
			} else {
				layer[u] = unlayered
			}
		}
		foundAugmentingPath := false
		for head := 0; head < len(queue); head++ {
			u := queue[head]
			for _, v := range adjacency[u] {
				w := matchRight[v]
				if w == unmatched {
					foundAugmentingPath = true
				} else if layer[w] == unlayered {
					layer[w] = layer[u] + 1
					queue = append(queue, w)
				}
			}
		}
		return foundAugmentingPath
	}
	var dfs func(u int) bool
	dfs = func(u int) bool {
		for _, v := range adjacency[u] {
			w := matchRight[v]
			if w == unmatched || (layer[w] == layer[u]+1 && dfs(w)) {
				matchLeft[u] = v
				matchRight[v] = u
				return true
			}
		}
		layer[u] = unlayered
		return false
	}

	size := 0
	for bfs() {
		for u := range adjacency {
			if matchLeft[u] == unmatched && dfs(u) {
				size++
			}
		}
	}
	return size
}

// replacementMatchesSimulatedNodeClaim reports whether a command's replacement is still a valid stand-in for a
// simulated NodeClaim. Instance type names alone are not enough: the same instance type can be reachable through
// different NodePools or scheduling constraints (zone, capacity type, taints), so we also require the same NodePool
// (name and UID),
// identical taints, and replacement requirements contained in the simulated claim's requirements (anything the
// replacement is allowed to launch as must satisfy what the fresh simulation demands).
func replacementMatchesSimulatedNodeClaim(replacement *Replacement, newNodeClaim *scheduling.NodeClaim) bool {
	// compare the UID too: a NodePool deleted and recreated under the same name would otherwise validate and then
	// launch NodeClaims owned by (and templated from) the old NodePool
	if replacement.NodePoolName != newNodeClaim.NodePoolName || replacement.NodePoolUUID != newNodeClaim.NodePoolUUID {
		return false
	}
	// compare the NodePool hash annotations too: an in-place NodePool template edit (e.g. NodeClassRef or
	// startup taints) keeps the same UID but changes the hash, and the stale template must not be launched
	if replacement.Annotations[v1.NodePoolHashAnnotationKey] != newNodeClaim.Annotations[v1.NodePoolHashAnnotationKey] ||
		replacement.Annotations[v1.NodePoolHashVersionAnnotationKey] != newNodeClaim.Annotations[v1.NodePoolHashVersionAnnotationKey] {
		return false
	}
	if !taintsAreEqual(replacement.Spec.Taints, newNodeClaim.Spec.Taints) {
		return false
	}
	for key := range newNodeClaim.Requirements {
		// the reservation ID requirement lists whichever reserved offerings a simulation run happened to reserve,
		// so two independent runs can legitimately pick different sets; capacity type is still compared, so a
		// reserved replacement must still match a reserved simulated claim
		if key == cloudprovider.ReservationIDLabel {
			continue
		}
		if !replacement.Requirements.Get(key).SubsetOf(newNodeClaim.Requirements.Get(key)) {
			return false
		}
	}
	return instanceTypesAreSubset(replacement.InstanceTypeOptions, newNodeClaim.InstanceTypeOptions)
}

// taintsAreEqual reports whether two taint lists are equal, ignoring order.
func taintsAreEqual(lhs, rhs []corev1.Taint) bool {
	if len(lhs) != len(rhs) {
		return false
	}
	for _, t := range lhs {
		if !lo.ContainsBy(rhs, func(other corev1.Taint) bool { return t.MatchTaint(&other) && t.Value == other.Value }) {
			return false
		}
	}
	return true
}

// getValidationFailureReason categorizes validation errors into specific failure types
func getValidationFailureReason(err error) string {
	if err == nil {
		return "unknown"
	}

	var budgetErr *BudgetValidationError
	var schedErr *SchedulingValidationError
	var churnErr *ChurnValidationError

	switch {
	case errors.As(err, &budgetErr):
		return "budget"
	case errors.As(err, &schedErr):
		return "scheduling"
	case errors.As(err, &churnErr):
		return "churn"
	default:
		return "unknown"
	}
}
