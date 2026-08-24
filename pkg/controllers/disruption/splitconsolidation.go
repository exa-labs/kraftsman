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
	"sync"
	"time"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// SplitAttemptBudget bounds how many split fallback simulations one consolidation pass may run.
// Every attempt costs an extra scheduling simulation on a candidate that already came back as a
// no-op, and no-ops are the dominant outcome, so an unbudgeted fallback would trade candidate
// traversal depth - which is what converts nodes today - for split search on a bad day. The
// budget is pass-scoped: candidates are evaluated in savings-ratio order, so the attempts a pass
// does spend go to its most valuable candidates.
type SplitAttemptBudget struct {
	mu        sync.Mutex
	remaining int
}

type splitAttemptBudgetContextKey struct{}

func NewSplitAttemptBudget(attempts int) *SplitAttemptBudget {
	return &SplitAttemptBudget{remaining: max(0, attempts)}
}

func WithSplitAttemptBudget(ctx context.Context, budget *SplitAttemptBudget) context.Context {
	return context.WithValue(ctx, splitAttemptBudgetContextKey{}, budget)
}

func SplitAttemptBudgetFromContext(ctx context.Context) *SplitAttemptBudget {
	budget, _ := ctx.Value(splitAttemptBudgetContextKey{}).(*SplitAttemptBudget)
	return budget
}

// TryAcquire consumes one attempt, reporting whether the pass had one left.
func (b *SplitAttemptBudget) TryAcquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.remaining <= 0 {
		return false
	}
	b.remaining--
	return true
}

func (b *SplitAttemptBudget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remaining
}

// trySplitConsolidation re-simulates a single-node candidate that no cheaper single replacement
// could absorb, this time forbidding new capacity priced at or above the candidate itself.
//
// The scheduler packs a candidate's pods into the fewest replacement NodeClaims it can, so when
// the NodePool permits an instance type as large as the candidate the simulation always yields one
// replacement claim whose only option is that same type - and price filtering then finds nothing
// cheaper that holds the whole claim, even where several smaller nodes would be cheaper together.
// Removing every instance type priced at or above the candidate makes the same packer open more
// claims on its own, so the split is discovered by the existing search rather than a new one. The
// result still flows through the ordinary replacement limit, aggregate price filter, spot-to-spot
// gating and validation, with an extra savings margin so a node isn't churned into several nodes
// for a negligible difference.
//
// Returns the command and whether the fallback produced one.
func (c *consolidation) trySplitConsolidation(ctx context.Context, simOpts consolidationSimulationOptions, candidatePrice float64, candidates []*Candidate) (Command, bool) {
	opts := options.FromContext(ctx)
	candidate, ok := splitCandidate(opts, simOpts, candidatePrice, candidates)
	if !ok {
		return Command{}, false
	}
	// No budget on the context means this simulation is not part of a budgeted pass at all, which is
	// a different condition from a pass that spent its cap and must not be reported as one.
	budget := SplitAttemptBudgetFromContext(ctx)
	if budget == nil {
		return Command{}, false
	}
	if !budget.TryAcquire() {
		ObserveConsolidationSplitAttempt(ctx, candidate.NodePool.Name, SplitOutcomeAttemptCapExhausted)
		return Command{}, false
	}

	start := time.Now()
	cmd, err := c.computeConsolidationWithOptions(ctx, consolidationSimulationOptions{
		newCapacityPriceLimit: candidatePrice,
		// The split margin guards against trading one node for several; the replace floor is the
		// fleet-wide minimum any replacement must clear, so the stricter of the two applies.
		minSavings: max(opts.ConsolidationSplitMinSavings, opts.ConsolidationReplaceMinSavings),
		silent:     true,
	}, candidate)
	ObserveConsolidationSplitDuration(ctx, candidate.NodePool.Name, time.Since(start))

	switch {
	case err != nil:
		ObserveConsolidationSplitAttempt(ctx, candidate.NodePool.Name, SplitOutcomeError)
		return Command{}, false
	case cmd.Decision() != ReplaceDecision:
		ObserveConsolidationSplitAttempt(ctx, candidate.NodePool.Name, SplitOutcomeNoOp)
		return Command{}, false
	default:
		ObserveConsolidationSplitAttempt(ctx, candidate.NodePool.Name, SplitOutcomeCommand)
		return cmd, true
	}
}

// splitCandidate returns the candidate a split retry may act on, filtering out every case the retry
// cannot turn into a command before it costs an attempt of the pass budget or a simulation.
func splitCandidate(opts *options.Options, simOpts consolidationSimulationOptions, candidatePrice float64, candidates []*Candidate) (*Candidate, bool) {
	// Only the ordinary single-candidate path falls back, and a split retry never recurses.
	if simOpts.newCapacityPriceLimit > 0 || len(candidates) != 1 {
		return nil, false
	}
	// Without room for a second replacement NodeClaim a split can never be accepted.
	if !opts.ConsolidationSplitFallback || opts.MaxConsolidationReplacements < 2 {
		return nil, false
	}
	candidate := candidates[0]
	// A single pod always lands on a single node, and an unpriced candidate gives no ceiling.
	if candidatePrice <= 0 || len(candidate.reschedulablePods) < 2 {
		return nil, false
	}
	// A spot candidate's replacements route back through computeSpotToSpotConsolidation, which rejects everything
	// while its feature gate is off. Spending attempts on a rejection that is decided before any simulation would
	// let spot candidates exhaust the pass budget ahead of on-demand candidates that can still produce a command.
	if candidate.capacityType == v1.CapacityTypeSpot && !opts.FeatureGates.SpotToSpotConsolidation {
		return nil, false
	}
	return candidate, true
}
