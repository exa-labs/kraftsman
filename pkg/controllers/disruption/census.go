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
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
)

// CensusConsolidationType labels census-originated simulation metrics so they
// never mix with the real single-node pass's series.
const CensusConsolidationType = "census"

// CensusInterval is how often the actionable-candidate census sweeps all
// consolidation candidates.
var CensusInterval = 10 * time.Minute

// CensusSweepTimeout bounds a single sweep's wall-clock time. A truncated
// sweep still publishes its partial counts; consolidation_census_candidates_evaluated
// exposes how far it got.
var CensusSweepTimeout = 5 * time.Minute

// CensusController periodically simulates consolidation for every candidate
// without executing anything, publishing how many candidates currently have a
// strictly cheaper delete or replace available. The disruption controller's
// single-node pass stops at the first winner, so its metrics cannot answer
// "how many nodes are actionable right now"; this census can.
type CensusController struct {
	method *SingleNodeConsolidation
}

// noopRecorder suppresses candidate events from census simulations: the census
// executes nothing, so Unconsolidatable/ConsolidationCandidate events would be
// misleading on nodes.
type noopRecorder struct{}

func (noopRecorder) Publish(...events.Event) {}

func NewCensusController(c consolidation) *CensusController {
	c.recorder = noopRecorder{}
	return &CensusController{method: NewSingleNodeConsolidation(c)}
}

func (c *CensusController) Name() string {
	return "disruption.census"
}

func (c *CensusController) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

func (c *CensusController) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	if !c.method.cluster.Synced(ctx) {
		return reconciler.Result{RequeueAfter: time.Second}, nil
	}
	candidates, _, err := GetCandidatesWithTotals(ctx, c.method.cluster, c.method.kubeClient, c.method.recorder, c.method.clock,
		c.method.cloudProvider, c.method.ShouldDisrupt, c.method.Class(), c.method.queue, nil)
	if err != nil {
		return reconciler.Result{}, fmt.Errorf("determining census candidates, %w", err)
	}

	ctx = withConsolidationType(ctx, CensusConsolidationType)
	ctx = scheduling.WithDaemonOverheadCache(ctx, scheduling.NewDaemonOverheadCache())
	ctx = scheduling.WithDomainGroupCache(ctx, scheduling.NewDomainGroupCache())
	ctx = scheduling.WithNodeRequirementsCache(ctx, scheduling.NewNodeRequirementsCache())
	ctx = scheduling.WithReservationCapacityCache(ctx, scheduling.NewReservationCapacityCache())
	ctx = scheduling.WithNodeClaimTemplateCache(ctx, scheduling.NewNodeClaimTemplateCache())
	ctx = scheduling.WithInverseAffinityCache(ctx, scheduling.NewInverseAffinityCache())
	// One split retry per candidate, unlike the real pass's fixed cap: the census exists to count
	// every actionable node, and capping retries would report candidates the pass would split as
	// non-actionable. CensusSweepTimeout still bounds what the extra simulations can cost.
	ctx = WithSplitAttemptBudget(ctx, NewSplitAttemptBudget(len(candidates)))
	ctx = WithPassReads(ctx, NewPassReads())

	start := c.method.clock.Now()
	deadline := start.Add(CensusSweepTimeout)
	actionable := map[string]map[Decision]int{}
	evaluated := 0
	for _, candidate := range candidates {
		if c.method.clock.Now().After(deadline) {
			log.FromContext(ctx).V(1).Info("truncating actionable-candidate census sweep due to timeout", "candidates_evaluated", evaluated, "candidates_total", len(candidates))
			break
		}
		cmd, err := c.method.computeConsolidation(ctx, candidate)
		evaluated++
		if err != nil {
			continue
		}
		if decision := cmd.Decision(); decision != NoOpDecision {
			if actionable[candidate.NodePool.Name] == nil {
				actionable[candidate.NodePool.Name] = map[Decision]int{}
			}
			actionable[candidate.NodePool.Name][decision]++
		}
	}

	ConsolidationActionableCandidates.Reset()
	for nodePool, decisions := range actionable {
		for decision, count := range decisions {
			ConsolidationActionableCandidates.Set(float64(count), map[string]string{
				metrics.NodePoolLabel: nodePool,
				decisionLabel:         string(decision),
			})
		}
	}
	ConsolidationCensusDurationSeconds.Set(c.method.clock.Since(start).Seconds(), nil)
	ConsolidationCensusCandidatesEvaluated.Set(float64(evaluated), nil)

	return reconciler.Result{RequeueAfter: CensusInterval}, nil
}
