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
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

var SingleNodeConsolidationTimeoutDuration = 3 * time.Minute

// commandAdmissionReserve is what admitting one held proposal is budgeted to cost: a
// re-simulation and the cloud provider calls that launch its replacements. Admission stops once
// less than a reserve of its budget remains, so validations running slower than budgeted end the
// pass instead of letting it run long. The first proposal is always attempted; today's pass
// admits its single command regardless of the timer, and this preserves that.
var commandAdmissionReserve = 20 * time.Second

// admissionBudget is how long a pass may spend admitting the proposals it holds, measured from
// the moment the walk ends. Admission is budgeted apart from SingleNodeConsolidationTimeoutDuration
// because the two bound different work: the pass timeout bounds discovery, which is the expensive
// half, while admission's cost is set by how many proposals the pass is holding. Spending what is
// left of the pass timeout instead would make admission cheapest exactly when the pass found the
// most to do, and would leave a pass that broke out of its walk on timeout - the case where
// holding proposals matters most - with no budget at all.
func admissionBudget(proposals int) time.Duration {
	return commandValidationDelay + time.Duration(proposals)*commandAdmissionReserve
}

const SingleNodeConsolidationType = "single"

// consolidationProposal is a command a pass has selected but not yet validated or queued.
type consolidationProposal struct {
	cmd Command
	// position is the candidate's index in the sorted candidate list, recorded so accepted
	// commands still report the traversal depth they were found at.
	position int
}

// SingleNodeConsolidation evaluates one node at a time for consolidation.
type SingleNodeConsolidation struct {
	consolidation
	PreviouslyUnseenNodePools sets.Set[string]
	validator                 Validator
}

func NewSingleNodeConsolidation(c consolidation, opts ...option.Function[MethodOptions]) *SingleNodeConsolidation {
	o := option.Resolve(append([]option.Function[MethodOptions]{WithValidator(NewSingleConsolidationValidator(c))}, opts...)...)
	return &SingleNodeConsolidation{
		consolidation:             c,
		PreviouslyUnseenNodePools: sets.New[string](),
		validator:                 o.validator,
	}
}

// ComputeCommand generates a disruption command given candidates
// nolint:gocyclo
func (s *SingleNodeConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	ctx = withConsolidationType(ctx, s.ConsolidationType())
	ctx = scheduling.WithDaemonOverheadCache(ctx, scheduling.NewDaemonOverheadCache())
	ctx = scheduling.WithDomainGroupCache(ctx, scheduling.NewDomainGroupCache())
	ctx = scheduling.WithNodeRequirementsCache(ctx, scheduling.NewNodeRequirementsCache())
	ctx = scheduling.WithReservationCapacityCache(ctx, scheduling.NewReservationCapacityCache())
	ctx = scheduling.WithNodeClaimTemplateCache(ctx, scheduling.NewNodeClaimTemplateCache())
	ctx = scheduling.WithTopologyPassCache(ctx, scheduling.NewTopologyPassCache())
	ctx = scheduling.WithInverseAffinityCache(ctx, scheduling.NewInverseAffinityCache())
	ctx = WithSplitAttemptBudget(ctx, NewSplitAttemptBudget(options.FromContext(ctx).ConsolidationSplitMaxAttempts))
	ctx = WithPassReads(ctx, NewPassReads())
	depth := 0
	evaluatedCandidateDepthByNodePool := map[string]int{}
	outcome := PassOutcomeNoOp
	defer func() {
		ObserveConsolidationPass(s.ConsolidationType(), outcome, depth)
		ObserveConsolidationCandidateDepthByNodePool(s.ConsolidationType(), evaluatedCandidateDepthByNodePool)
	}()
	if s.IsConsolidated() {
		return []Command{}, nil
	}
	candidates = s.SortCandidates(ctx, candidates)

	// Set a timeout
	timeout := s.clock.Now().Add(SingleNodeConsolidationTimeoutDuration)
	constrainedByBudgets := false

	unseenNodePools := sets.New(lo.Map(candidates, func(c *Candidate, _ int) string { return c.NodePool.Name })...)

	// A pass may hold several proposals before admitting any of them. Discovery, not admission,
	// is what a pass is expensive for, so a pass that has already walked the candidate list can
	// carry more than one command out of it. maxCommands of 1 keeps the classic behavior of
	// validating and returning the first accepted command.
	maxCommands := options.FromContext(ctx).MaxConsolidationCommandsPerPass
	proposals := []consolidationProposal{}
	// claimedProviderIDs holds every candidate of every proposal, so two proposals can never
	// name the same node. Multi-candidate commands contribute all of their candidates.
	claimedProviderIDs := sets.New[string]()
	// balancedNodePoolsHeld bounds Balanced pools to one proposal per pass: their scores come
	// from NodePool totals computed once per pass, which the first move invalidates.
	balancedNodePoolsHeld := sets.New[string]()

	timedOut := false
	for i, candidate := range candidates {
		if s.clock.Now().After(timeout) {
			outcome = PassOutcomeTimedOut
			depth = i
			timedOut = true
			ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: s.ConsolidationType()})
			log.FromContext(ctx).V(1).Info("abandoning single-node consolidation due to timeout", "candidates_evaluated", i)

			s.PreviouslyUnseenNodePools = unseenNodePools
			ObserveUnseenNodePools(s.ConsolidationType(), unseenNodePools.UnsortedList())

			if len(proposals) == 0 {
				return []Command{}, nil
			}
			// Proposals the pass already paid to find are still worth admitting. Admission has its
			// own budget, so all of them get their attempt; the pass runs past its timeout by that
			// budget, which buys the commands a timed-out pass used to throw away.
			break
		}
		// Track that we've seen this nodepool
		unseenNodePools.Delete(candidate.NodePool.Name)
		evaluatedCandidateDepthByNodePool[candidate.NodePool.Name]++

		// Candidates an earlier proposal in this pass already claims cannot be part of a second
		// command: the queue admits a node to at most one in-flight command.
		if claimedProviderIDs.Has(candidate.ProviderID()) {
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipClaimedByPendingCommand)
			depth = i + 1
			continue
		}
		if balancedNodePoolsHeld.Has(candidate.NodePool.Name) {
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipPoolCommandHeld)
			depth = i + 1
			continue
		}

		// If the disruption budget doesn't allow this candidate to be disrupted, continue to the
		// next candidate. The mapping is decremented when a proposal is held, so a pass carrying
		// several commands cannot overspend a pool's budget; validation recomputes the budget
		// from live state before each command is queued and remains authoritative.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			constrainedByBudgets = true
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipBudgetExhausted)
			depth = i + 1
			continue
		}
		// Skip candidates whose best-case score (delete ratio) cannot pass the
		// threshold. A DELETE is the upper bound; if it fails, no REPLACE will pass.
		if !s.evaluator.CanPassThreshold(candidate) {
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipThreshold)
			depth = i + 1
			continue
		}

		// compute a possible consolidation option
		cmd, err := s.computeConsolidationWithinCandidateBudget(ctx, candidate)
		depth = i + 1
		if err != nil {
			// A candidate that ran out of its own budget is abandoned, not an error: the walk
			// keeps its remaining time for the candidates behind it.
			if errors.Is(err, errCandidateTimedOut) {
				observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipCandidateTimeout)
				log.FromContext(ctx).V(1).Info("abandoning consolidation candidate that exceeded its simulation budget", "node", candidate.Name())
				continue
			}
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipComputeError)
			log.FromContext(ctx).Error(err, "failed computing consolidation")
			continue
		}
		if cmd.Decision() == NoOpDecision {
			continue
		}
		// Score the move: Balanced pools may reject; other policies pass through.
		if approved, _ := s.evaluator.ApproveCommand(ctx, cmd); !approved {
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipApprovalRejected)
			continue
		}
		if maxCommands <= 1 {
			if _, err = s.validator.Validate(ctx, cmd, commandValidationDelay); err != nil {
				if IsValidationError(err) {
					reason := getValidationFailureReason(err)
					cmd.EmitRejectedEvents(s.recorder, reason)
					return []Command{}, nil
				}
				return []Command{}, fmt.Errorf("validating consolidation, %w", err)
			}
			outcome = PassOutcomeCompleted
			ObserveAcceptedCandidate(cmd, s.ConsolidationType(), i)
			return []Command{cmd}, nil
		}

		proposals = append(proposals, consolidationProposal{cmd: cmd, position: i})
		for _, held := range cmd.Candidates {
			claimedProviderIDs.Insert(held.ProviderID())
		}
		disruptionBudgetMapping[candidate.NodePool.Name]--
		if candidate.NodePool.Spec.Disruption.ConsolidationPolicy.IsBalanced() {
			balancedNodePoolsHeld.Insert(candidate.NodePool.Name)
		}
		if len(proposals) >= maxCommands {
			break
		}
	}

	if len(proposals) > 0 {
		admitted, err := s.admitProposals(ctx, proposals)
		// Only passes that actually held a batch are observed, so the histogram's rate is the rate
		// of batched passes: a pass holding one proposal admits exactly as the unbatched controller
		// would, and would otherwise pile samples of 1 onto the distribution being measured.
		if len(proposals) > 1 {
			ObserveConsolidationCommandsAdmitted(s.ConsolidationType(), len(admitted))
		}
		// A failure partway through admission still leaves the earlier commands queued and running,
		// so the pass acted. The outcome records what the pass did; ConsolidationAdmissionFailuresTotal
		// records that the rest of it did not.
		if len(admitted) > 0 && !timedOut {
			outcome = PassOutcomeCompleted
		}
		if err != nil {
			return admitted, err
		}
		if len(admitted) > 0 {
			return admitted, nil
		}
		// Every proposal was rejected at admission. The cluster still holds candidates worth
		// re-evaluating, so don't mark the fleet consolidated.
		return []Command{}, nil
	}

	if !constrainedByBudgets {
		// if there are no candidates because of a budget, don't mark
		// as consolidated, as it's possible it should be consolidatable
		// the next time we try to disrupt.
		s.markConsolidated()
	}

	s.PreviouslyUnseenNodePools = unseenNodePools

	return []Command{}, nil
}

// admitProposals validates and queues held proposals one at a time.
//
// Sequencing is the safety property, not an implementation detail. StartCommand taints the
// candidates, launches their replacements and marks them for deletion before it returns, so by
// the time the next proposal is validated the cluster state it re-simulates against already
// contains the effects of every command admitted before it. A proposal that depended on capacity
// an earlier command consumed fails validation the same way a plan drifting across the settling
// window does today, instead of double-booking that capacity. Validating the whole batch first
// and starting the commands concurrently would lose exactly that property.
func (s *SingleNodeConsolidation) admitProposals(ctx context.Context, proposals []consolidationProposal) ([]Command, error) {
	admitted := []Command{}
	// Admission runs on its own budget, so a pass that walked right up to its timeout - or past it,
	// having broken out of the walk holding proposals - still admits what it paid to find.
	deadline := s.clock.Now().Add(admissionBudget(len(proposals)))
	// The settling window observes churn in the pass as a whole, so only the first validation
	// waits it out; the rest inherit the elapsed time, as they do within one multi-node command.
	validationDelay := commandValidationDelay
	// The first proposal is always attempted, since a pass has always been allowed to validate
	// the command it found. Every proposal after it costs another re-simulation, so the reserve
	// gates them whether or not the attempts before them produced a command.
	attempted := false
	for _, proposal := range proposals {
		if attempted && !s.clock.Now().Add(commandAdmissionReserve).Before(deadline) {
			ObserveConsolidationAdmissionFailure(s.ConsolidationType(), AdmissionStageDeadline, "admission_reserve")
			continue
		}
		if validationDelay == 0 {
			// The validator only drops pass-scoped reads when it waits. Commands admitted before
			// this one moved pods and launched replacements, so drop them here as well: this
			// proposal has to be judged against the cluster those commands left behind.
			ctx = scheduling.WithTopologyPassCache(ctx, scheduling.NewTopologyPassCache())
			ctx = scheduling.WithInverseAffinityCache(ctx, scheduling.NewInverseAffinityCache())
			ctx = WithPassReads(ctx, NewPassReads())
			if cache := scheduling.DaemonOverheadCacheFromContext(ctx); cache != nil {
				cache.DropStateDerived()
			}
		}
		_, err := s.validator.Validate(ctx, proposal.cmd, validationDelay)
		validationDelay = 0
		attempted = true
		if err != nil {
			if !IsValidationError(err) {
				return admitted, fmt.Errorf("validating consolidation, %w", err)
			}
			reason := getValidationFailureReason(err)
			proposal.cmd.EmitRejectedEvents(s.recorder, reason)
			ObserveConsolidationAdmissionFailure(s.ConsolidationType(), AdmissionStageValidation, reason)
			continue
		}

		cmd := proposal.cmd
		cmd.CreationTimestamp = s.clock.Now()
		cmd.ID = uuid.New()
		cmd.Method = s
		cmd.Admitted = true
		if err := s.queue.StartCommand(ctx, &cmd); err != nil {
			ObserveConsolidationAdmissionFailure(s.ConsolidationType(), AdmissionStageStart, "error")
			return admitted, fmt.Errorf("disrupting candidates, %w", err)
		}
		ObserveAcceptedCandidate(cmd, s.ConsolidationType(), proposal.position)
		admitted = append(admitted, cmd)
	}
	return admitted, nil
}

func (s *SingleNodeConsolidation) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonUnderutilized
}

func (s *SingleNodeConsolidation) Class() string {
	return GracefulDisruptionClass
}

func (s *SingleNodeConsolidation) ConsolidationType() string {
	return SingleNodeConsolidationType
}

// SortCandidates applies the consolidation sort, then interweaves by NodePool.
func (s *SingleNodeConsolidation) SortCandidates(ctx context.Context, candidates []*Candidate) []*Candidate {
	candidates = s.sortCandidates(ctx, candidates)
	return s.shuffleCandidates(ctx, lo.GroupBy(candidates, func(c *Candidate) string { return c.NodePool.Name }))
}

func (s *SingleNodeConsolidation) shuffleCandidates(ctx context.Context, nodePoolCandidates map[string][]*Candidate) []*Candidate {
	var result []*Candidate
	// Log any timed out nodepools that we're prioritizing
	if s.PreviouslyUnseenNodePools.Len() != 0 {
		log.FromContext(ctx).V(1).Info("prioritizing nodepools that have not yet been considered due to timeouts in previous runs", "nodepools", strings.Join(s.PreviouslyUnseenNodePools.UnsortedList(), ", "))
	}
	sortedNodePools := s.PreviouslyUnseenNodePools.UnsortedList()
	sortedNodePools = append(sortedNodePools, lo.Filter(lo.Keys(nodePoolCandidates), func(nodePoolName string, _ int) bool {
		return !s.PreviouslyUnseenNodePools.Has(nodePoolName)
	})...)

	// Find the maximum number of candidates in any nodepool
	maxCandidatesPerNodePool := lo.MaxBy(lo.Values(nodePoolCandidates), func(a, b []*Candidate) bool {
		return len(a) > len(b)
	})

	// Interweave candidates from different nodepools
	for i := range maxCandidatesPerNodePool {
		for _, nodePoolName := range sortedNodePools {
			if i < len(nodePoolCandidates[nodePoolName]) {
				result = append(result, nodePoolCandidates[nodePoolName][i])
			}
		}
	}

	return result
}
