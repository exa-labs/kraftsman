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
	// evaluatedThisCycle holds the provider IDs of candidates a walk has reached since the last
	// full pass, so that consecutive timed-out passes resume coverage at the tail instead of
	// re-walking the head every time. It is a walk-position hint only: nothing simulated or
	// decided in an earlier pass is carried by it, and every candidate a pass reaches is still
	// simulated and validated against current cluster state.
	evaluatedThisCycle sets.Set[string]
	validator          Validator
	// negativeResults persists across passes: a candidate whose simulation ended in a no-op is
	// remembered by fingerprint, so an identical candidate in a later pass can be skipped.
	negativeResults *NegativeResultCache
	// completedCommandsSeen is the queue's completed-command count at the last pass. Any command
	// completing between passes — from any disruption method, not just this one — can free
	// capacity that a cached no-op verdict depended on, so the cache is cleared when it moves.
	completedCommandsSeen uint64
}

func NewSingleNodeConsolidation(c consolidation, opts ...option.Function[MethodOptions]) *SingleNodeConsolidation {
	o := option.Resolve(append([]option.Function[MethodOptions]{WithValidator(NewSingleConsolidationValidator(c))}, opts...)...)
	return &SingleNodeConsolidation{
		consolidation:             c,
		PreviouslyUnseenNodePools: sets.New[string](),
		evaluatedThisCycle:        sets.New[string](),
		validator:                 o.validator,
		negativeResults:           NewNegativeResultCache(c.clock),
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
	skippedOnNegativeCache := false

	unseenNodePools := sets.New(lo.Map(candidates, func(c *Candidate, _ int) string { return c.NodePool.Name })...)

	// A pass may hold several proposals before admitting any of them. Discovery, not admission,
	// is what a pass is expensive for, so a pass that has already walked the candidate list can
	// carry more than one command out of it. maxCommands of 1 keeps the classic behavior of
	// validating and returning the first accepted command.
	maxCommands := options.FromContext(ctx).MaxConsolidationCommandsPerPass
	skipUnchangedNegatives := options.FromContext(ctx).ConsolidationSkipUnchangedNegatives
	negativeCacheTTL := options.FromContext(ctx).ConsolidationNegativeCacheTTL
	fingerprints := newNegativeCacheFingerprints(s.kubeClient, s.cloudProvider)
	// A verdict of "nothing cheaper existed" holds only for the fleet it was computed against.
	// Commands from every disruption method — multi-node consolidation, drift, emptiness,
	// expiration — run through the queue and can free capacity that is invisible to a candidate's
	// own fingerprint, so any completed command since the last pass empties the cache.
	if completed := s.queue.CompletedCommandCount(); completed != s.completedCommandsSeen {
		s.completedCommandsSeen = completed
		s.negativeResults.Clear()
	}
	// Nodes that left the fleet are never looked up again, so their entries only leave the map
	// here. The sweep runs after the candidate walk so a lookup still sees — and reports as
	// expired, not absent — an entry that outlived its TTL between passes.
	defer s.negativeResults.DropExpired()
	proposals := []consolidationProposal{}
	// claimedProviderIDs holds every candidate of every proposal, so two proposals can never
	// name the same node. Multi-candidate commands contribute all of their candidates.
	claimedProviderIDs := sets.New[string]()
	// balancedNodePoolsHeld bounds Balanced pools to one proposal per pass: their scores come
	// from NodePool totals computed once per pass, which the first move invalidates.
	balancedNodePoolsHeld := sets.New[string]()

	// With more than one discovery worker, candidate simulations run concurrently on the walker
	// while this loop remains the single consumer: it applies every skip check, holds proposals,
	// and admits commands strictly in candidate order, exactly as the serial walk does. The gate
	// mirrors the loop's bookkeeping so workers can decline to simulate candidates the loop is
	// already bound to skip.
	var walker *candidateWalker
	var gate *walkGate
	if workers := options.FromContext(ctx).ConsolidationDiscoveryWorkers; workers > 1 {
		gate = newWalkGate(lo.Assign(disruptionBudgetMapping), s.evaluator.CanPassThreshold)
		walker = startCandidateWalker(ctx, candidates, workers, gate, s.computeConsolidationWithinCandidateBudget)
		defer walker.stop()
	}

	timedOut := false
	// The coverage cycle only accumulates across consecutive timed-out walks. Any pass that ends
	// for another reason - it walked every candidate, returned its single command, or filled its
	// proposal batch - resets it, so the next walk starts back at the head of the sorted list and
	// the ranking is only overridden while timeouts are actually starving the tail.
	defer func() {
		if !timedOut {
			s.evaluatedThisCycle = sets.New[string]()
			// The gauge is only written by timed-out walks; drop the series so it does not
			// hold the last timed-out pass's partial fraction after coverage recovered.
			ResetWalkCycleCoverage(s.ConsolidationType())
		}
	}()
	for i, candidate := range candidates {
		if s.clock.Now().After(timeout) {
			outcome = PassOutcomeTimedOut
			depth = i
			timedOut = true
			ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: s.ConsolidationType()})
			log.FromContext(ctx).V(1).Info("abandoning single-node consolidation due to timeout", "candidates_evaluated", i)

			s.PreviouslyUnseenNodePools = unseenNodePools
			ObserveUnseenNodePools(s.ConsolidationType(), unseenNodePools.UnsortedList())
			ObserveWalkCycleCoverage(s.ConsolidationType(), len(s.evaluatedThisCycle), len(candidates))

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
			walker.discard(i)
			continue
		}
		if balancedNodePoolsHeld.Has(candidate.NodePool.Name) {
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipPoolCommandHeld)
			depth = i + 1
			walker.discard(i)
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
			walker.discard(i)
			continue
		}
		// Skip candidates whose best-case score (delete ratio) cannot pass the
		// threshold. A DELETE is the upper bound; if it fails, no REPLACE will pass.
		if !s.evaluator.CanPassThreshold(candidate) {
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipThreshold)
			depth = i + 1
			walker.discard(i)
			continue
		}

		// The coverage cycle only counts candidates that reach a verdict: a candidate skipped
		// above (claimed, pool held, budget exhausted, below threshold) was never evaluated, and
		// marking it reached would demote it behind the tail on the next resumed walk. A cached
		// negative below counts — it stands in for the simulation that produced it.
		s.evaluatedThisCycle.Insert(candidate.ProviderID())

		// A candidate that an earlier pass simulated to a no-op, none of whose fingerprinted
		// inputs have moved since, would re-derive the same answer. The lookup is always recorded
		// so the recurrence rate is measurable; the skip itself is opt-in.
		fingerprint := fingerprints.fingerprint(ctx, candidate)
		if fingerprint != "" && s.negativeResults.ShouldSkip(s.ConsolidationType(), candidate.ProviderID(), fingerprint) && skipUnchangedNegatives {
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipUnchangedNegative)
			skippedOnNegativeCache = true
			depth = i + 1
			walker.discard(i)
			continue
		}

		// compute a possible consolidation option
		var cmd Command
		var err error
		var durability *noOpDurability
		if walker != nil {
			res := walker.result(i)
			if res.gateSkipped {
				// Unreachable in practice: the gate is a subset of the in-order checks above
				// (its state cannot move while this loop is blocked on the result), so a
				// candidate that passed them cannot have been gate-skipped. Skip defensively
				// rather than treat the sentinel as a verdict.
				depth = i + 1
				continue
			}
			cmd, err, durability = res.cmd, res.err, res.durability
		} else {
			var candidateCtx context.Context
			candidateCtx, durability = withNoOpDurability(ctx)
			cmd, err = s.computeConsolidationWithinCandidateBudget(candidateCtx, candidate)
		}
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
			// A no-op decided by pass-scoped exhaustion (spent split budget, deleting candidate,
			// transient error) is not a verdict about the candidate and must not outlive the pass.
			if fingerprint != "" && durability.Conclusive() {
				s.negativeResults.StoreNegative(candidate.ProviderID(), fingerprint, negativeCacheTTL)
			}
			continue
		}
		// Score the move: Balanced pools may reject; other policies pass through.
		if approved, _ := s.evaluator.ApproveCommand(ctx, cmd); !approved {
			observeCandidateSkip(s.ConsolidationType(), candidate, CandidateSkipApprovalRejected)
			continue
		}
		if maxCommands <= 1 {
			// Nothing after validation reads further results, and validation re-simulates
			// against live state: stop speculative work before it starts.
			walker.stop()
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
			// The command frees capacity every stored verdict was computed without; none of them
			// can be trusted against the fleet this command leaves behind.
			s.negativeResults.Clear()
			return []Command{cmd}, nil
		}

		proposals = append(proposals, consolidationProposal{cmd: cmd, position: i})
		for _, held := range cmd.Candidates {
			claimedProviderIDs.Insert(held.ProviderID())
		}
		disruptionBudgetMapping[candidate.NodePool.Name]--
		isBalanced := candidate.NodePool.Spec.Disruption.ConsolidationPolicy.IsBalanced()
		if isBalanced {
			balancedNodePoolsHeld.Insert(candidate.NodePool.Name)
		}
		if gate != nil {
			gate.hold(lo.Map(cmd.Candidates, func(c *Candidate, _ int) string { return c.ProviderID() }), candidate.NodePool.Name, isBalanced)
		}
		if len(proposals) >= maxCommands {
			break
		}
	}

	// Admission observes and mutates live cluster state; no speculative simulation may run
	// behind it.
	walker.stop()

	if len(proposals) > 0 {
		admitted, err := s.admitProposals(ctx, proposals)
		if len(admitted) > 0 {
			// Executed commands change the free capacity every stored verdict was computed against.
			s.negativeResults.Clear()
		}
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

	// A pass that skipped candidates on cached verdicts did not actually evaluate them, so it
	// cannot declare the fleet consolidated: IsConsolidated would then suppress passes until some
	// unrelated state change, holding the skipped entries past their TTL — expiry only runs when
	// a pass looks the entries up. Mirrors how budget-constrained passes are handled.
	if !constrainedByBudgets && !skippedOnNegativeCache {
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

// SortCandidates applies the consolidation sort, interweaves by NodePool, then resumes an
// unfinished coverage cycle by moving candidates earlier timed-out walks already reached behind
// the ones they never got to.
func (s *SingleNodeConsolidation) SortCandidates(ctx context.Context, candidates []*Candidate) []*Candidate {
	candidates = s.sortCandidates(ctx, candidates)
	candidates = s.shuffleCandidates(ctx, lo.GroupBy(candidates, func(c *Candidate) string { return c.NodePool.Name }))
	return s.resumeCoverageCycle(ctx, candidates)
}

// resumeCoverageCycle reorders the sorted candidate list so a walk continues the coverage cycle
// where the last timed-out walk stopped. The cycle only reorders — it persists no simulation
// result or decision, the candidate list and cluster state are rebuilt every pass, and each
// reached candidate is re-simulated and validated against current state. Without it, every
// timed-out pass restarts at the head of the sorted list, so head candidates are re-evaluated
// each pass while the tail past the timeout horizon is starved indefinitely; with it, every
// candidate is reached within a bounded number of passes. A cycle ends when a walk ends for
// any reason other than a timeout, or when every current candidate has been reached; either
// way the next pass starts a fresh cycle in pure sorted order.
func (s *SingleNodeConsolidation) resumeCoverageCycle(ctx context.Context, candidates []*Candidate) []*Candidate {
	if s.evaluatedThisCycle.Len() == 0 {
		return candidates
	}
	// Nodes that left the candidate set (deleted, disrupted, or no longer consolidatable) drop
	// out of the cycle so the set tracks only live coverage and cannot grow without bound.
	current := sets.New(lo.Map(candidates, func(c *Candidate, _ int) string { return c.ProviderID() })...)
	s.evaluatedThisCycle = s.evaluatedThisCycle.Intersection(current)
	if s.evaluatedThisCycle.Len() == 0 || s.evaluatedThisCycle.Len() >= len(candidates) {
		// Every live candidate was reached: the cycle is complete, start the next one at the head.
		s.evaluatedThisCycle = sets.New[string]()
		return candidates
	}
	unreached, reached := lo.FilterReject(candidates, func(c *Candidate, _ int) bool {
		return !s.evaluatedThisCycle.Has(c.ProviderID())
	})
	log.FromContext(ctx).V(1).Info("resuming candidate walk coverage cycle", "already_evaluated", len(reached), "remaining", len(unreached))
	return append(unreached, reached...)
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
