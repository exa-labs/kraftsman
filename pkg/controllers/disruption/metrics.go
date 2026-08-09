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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

// Disruption metrics measure six distinct populations, and several upstream names read as a
// verdict when they only record which test a node passed:
//
//	managed     every Karpenter-managed node.
//	candidate   constructible and unblocked for one method. Reported as eligible_nodes.
//	evaluated   walked by a pass before its timeout. Reported as consolidation_candidate_depth.
//	actionable  a strictly cheaper delete or replace exists for it. Reported by the census as
//	            consolidation_actionable_candidates, the only rung that compares prices.
//	admitted    survived revalidation and entered the queue.
//	executed    the node is gone. Reported as consolidation_executed_nodes_total.
//
// Only executed implies the rungs above it: on production the candidate population routinely
// exceeds the actionable one by three orders of magnitude.
const (
	voluntaryDisruptionSubsystem = "voluntary_disruption"
	decisionLabel                = "decision"
	ConsolidationTypeLabel       = "consolidation_type"
	stageLabel                   = "stage"
	CandidatesIneligible         = "candidates_ineligible"
	policyLabel                  = "policy"
	outcomeLabel                 = "outcome"
	reasonLabel                  = "reason"
	replacementCountLabel        = "replacement_count"
	capacityTypeTransitionLabel  = "capacity_type_transition"
	instanceTypeLabel            = "instance_type"
	fromInstanceTypeLabel        = "from_instance_type"
	fromCapacityTypeLabel        = "from_capacity_type"
	toInstanceTypeLabel          = "to_instance_type"
	toCapacityTypeLabel          = "to_capacity_type"
	// unknownTypeValue labels an instance or capacity type the node's labels did not
	// resolve, keeping the series present instead of silently dropping the observation.
	unknownTypeValue = "unknown"
	// multipleTypesValue collapses the source types of a command disrupting nodes of more
	// than one instance type, so multi-node commands cannot multiply label cardinality.
	multipleTypesValue = "multiple"
)

// Pass outcomes partition every pass exactly once: it either acted, ran out of time, or found
// nothing. Budget saturation is deliberately not an outcome here. Budgets are per NodePool while a
// pass is fleet-wide, so one pool's exhausted allowance would label a pass that walked hundreds of
// candidates across every other pool; the CandidateSkipBudgetExhausted reason below carries the
// same fact with the NodePool attached.
const (
	PassOutcomeCompleted = "completed"
	PassOutcomeTimedOut  = "timed_out"
	PassOutcomeNoOp      = "no_op"
	// CandidateSkipBudgetExhausted marks a single-node candidate whose NodePool
	// has zero disruptions currently allowed by its budget.
	CandidateSkipBudgetExhausted = "budget_exhausted"
	// CandidateSkipSimBatchOverBudgetAllowance marks a multi-node candidate that
	// did not fit in the joint simulation batch bounded by its NodePool's per-pass
	// disruption allowance. The budget may be entirely unused; this is batch-size
	// bookkeeping, not active-disruption saturation.
	CandidateSkipSimBatchOverBudgetAllowance = "sim_batch_over_budget_allowance"
	CandidateSkipThreshold                   = "cannot_pass_threshold"
	// The simulation ran and produced a plan that was not worth executing. Which of these fires
	// says what would have to change for the candidate to become actionable, so they are separate
	// reasons rather than one noop_decision bucket: a fleet whose skips are
	// no_cheaper_replacement_set is waiting on cheaper offerings, while one whose skips are
	// no_cheaper_single_replacement is waiting on a search that considers several smaller nodes.
	//
	// CandidateSkipNoCheaperSingleReplacement is that second case: exactly one replacement was
	// needed (one node of the candidate's shape fits all its pods) and nothing of that shape is
	// cheaper. It is the population the split fallback exists to serve.
	CandidateSkipNoCheaperSingleReplacement = "no_cheaper_single_replacement"
	// CandidateSkipNoCheaperReplacementSet is the same verdict for a multi-node replacement plan:
	// no combination of the required replacements beats the candidates' price.
	CandidateSkipNoCheaperReplacementSet = "no_cheaper_replacement_set"
	// CandidateSkipReplacementFlexibility means cheaper capacity does exist but the options that
	// survived the price ceiling could not satisfy the NodePool's minValues requirements. This
	// fleet is waiting on requirements, not on prices.
	CandidateSkipReplacementFlexibility = "replacement_flexibility"
	// CandidateSkipNoCompatibleReplacement means a replacement reached the price filter with no
	// instance type options at all, so no price was ever compared: its requirements excluded
	// everything. Spot-to-spot narrows options to spot-compatible types before pricing, which is
	// the usual way a claim arrives empty.
	CandidateSkipNoCompatibleReplacement = "no_compatible_replacement"
	// CandidateSkipSpotToSpotDisabled means the candidate and its replacement are both spot and the
	// SpotToSpotConsolidation feature gate is off, so price was never consulted.
	CandidateSkipSpotToSpotDisabled = "spot_to_spot_disabled"
	// CandidateSkipSpotToSpotFlexibility means a cheaper spot replacement exists but fewer than
	// MinInstanceTypesForSpotToSpotConsolidation cheaper instance types remain, which upstream
	// requires to avoid consolidating the same node repeatedly.
	CandidateSkipSpotToSpotFlexibility = "spot_to_spot_flexibility"
	// CandidateSkipNoOp remains for a no-op the branches above do not explain.
	CandidateSkipNoOp                 = "noop_decision"
	CandidateSkipComputeError         = "compute_error"
	CandidateSkipPodsDidNotSchedule   = "pods_did_not_schedule"
	CandidateSkipMultipleReplacements = "multiple_replacements_required"
	CandidateSkipApprovalRejected     = "approval_rejected"
	// CandidateSkipClaimedByPendingCommand marks a candidate that an earlier proposal in the
	// same pass already holds. Commands admitted from one pass must not overlap.
	CandidateSkipClaimedByPendingCommand = "claimed_by_pending_command"
	// CandidateSkipPoolCommandHeld marks a candidate whose Balanced NodePool already holds a
	// proposal in this pass. Balanced scoring uses NodePool totals computed once per pass, so
	// a second move in the same pool would be scored against totals the first move invalidates.
	CandidateSkipPoolCommandHeld = "pool_command_held"
	// CandidateSkipCandidateTimeout marks a candidate whose simulation exceeded
	// ConsolidationCandidateTimeout and was abandoned so the walk could continue. It is the
	// per-candidate counterpart of a timed-out pass: the pass survives, this candidate does not.
	CandidateSkipCandidateTimeout = "candidate_timed_out"
)

const (
	// AdmissionStageValidation marks a proposal rejected by its pre-admission validation.
	AdmissionStageValidation = "validation"
	// AdmissionStageStart marks a proposal that failed while being queued, i.e. tainting,
	// launching replacements, or the queue's own overlap guard.
	AdmissionStageStart = "start"
	// AdmissionStageDeadline marks proposals left unattempted because the pass ran out of its
	// admission reserve.
	AdmissionStageDeadline = "deadline"
)

const (
	SplitOutcomeCommand             = "command"
	SplitOutcomeNoOp                = "no_op"
	SplitOutcomeError               = "error"
	SplitOutcomeAttemptCapExhausted = "attempt_cap_exhausted"

	// ODToSpotRetryOutcomeArmed means the ordinary price filter emptied the replacement claims and
	// started the spot-only repricing retry.
	ODToSpotRetryOutcomeArmed = "armed"
	// ODToSpotRetryOutcomeAdmitted means the spot-only repricing retry produced a consolidation command.
	ODToSpotRetryOutcomeAdmitted = "admitted"
	// ODToSpotRetryOutcomeCapacityTypeMinValues means pinning capacity type to spot would violate minValues.
	ODToSpotRetryOutcomeCapacityTypeMinValues = "capacity_type_min_values"
	// ODToSpotRetryOutcomeNoCheapZone means no spot zone beats the retry's per-claim price budget.
	ODToSpotRetryOutcomeNoCheapZone = "no_cheap_zone"
	// ODToSpotRetryOutcomeZoneMinValues means pinning zones to the cheap set would violate minValues.
	ODToSpotRetryOutcomeZoneMinValues = "zone_min_values"
	// ODToSpotRetryOutcomeAggregatePrice means the final aggregate price filter rejected the retry.
	ODToSpotRetryOutcomeAggregatePrice = "aggregate_price"
)

var (
	consolidationCandidateBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 150, 200, 250, 300, 400, 500, 750, 1000}
	durationBuckets               = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 2, 5, 10, 30, 60, 120, 180, 300}
)

func init() {
	ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: MultiNodeConsolidationType})
	ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: SingleNodeConsolidationType})
}

var (
	EvaluationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decision_evaluation_duration_seconds",
			Help:      "Duration of the disruption decision evaluation process in seconds. Labeled by method and consolidation type.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	DecisionsPerformedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_total",
			Help:      "Number of disruption decisions performed. Labeled by disruption decision, reason, and consolidation type.",
		},
		[]string{decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	NodepoolDecisionsPerformed = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_by_nodepool_total",
			Help:      "Number of disruption decisions performed by nodepool. Labeled by nodepool name, disruption decision, reason, and consolidation type.",
		},
		[]string{metrics.NodePoolLabel, decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	// EligibleNodes counts candidates, not nodes worth disrupting: it is set from
	// len(GetCandidates(...)), so a node is counted once it is constructible and unblocked
	// (not queued, no PDB or do-not-disrupt violation, labeled with instance type, capacity
	// type and zone, consolidation enabled on its NodePool). Nothing here compares price, so a
	// node with no cheaper alternative in the fleet is still "eligible". The upstream name is
	// kept as-is to stay rebaseable; consolidation_actionable_candidates below is the gauge that
	// answers "is there anything worth doing".
	EligibleNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "eligible_nodes",
			Help:      "Number of nodes eligible for disruption by Karpenter. Labeled by disruption reason.",
		},
		[]string{metrics.ReasonLabel},
	)
	ConsolidationActionableCandidates = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_actionable_candidates",
			Help:      "Number of consolidation candidates for which the latest census sweep found a strictly cheaper delete or replace. Labeled by nodepool and decision.",
		},
		[]string{metrics.NodePoolLabel, decisionLabel},
	)
	ConsolidationCensusDurationSeconds = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_census_duration_seconds",
			Help:      "Wall-clock duration of the latest actionable-candidate census sweep.",
		},
		[]string{},
	)
	ConsolidationCensusCandidatesEvaluated = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_census_candidates_evaluated",
			Help:      "Number of candidates evaluated by the latest actionable-candidate census sweep. Compare with eligible_nodes to detect truncated sweeps.",
		},
		[]string{},
	)
	ConsolidationTimeoutsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_timeouts_total",
			Help:      "Number of times the Consolidation algorithm has reached a timeout. Labeled by consolidation type.",
		},
		[]string{ConsolidationTypeLabel},
	)
	FailedValidationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "failed_validations_total",
			Help:      "Number of candidates that were selected for disruption but failed validation. Labeled by consolidation type.",
		},
		[]string{ConsolidationTypeLabel},
	)
	// NodePoolAllowedDisruptions is the ceiling the NodePool's spec.disruption.budgets allow,
	// which is why the CRD says "budget" and the metric says "allowed disruptions" for the same
	// quantity. Pair it with NodePoolNodesConsumingBudgets (the current spend) to read saturation;
	// the candidate skip reason budget_exhausted is the per-candidate consequence of the two
	// meeting, not a third measure of the same thing.
	NodePoolAllowedDisruptions = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "allowed_disruptions",
			Help:      "The number of nodes for a given NodePool that can be concurrently disrupting at a point in time. Labeled by NodePool. Note that allowed disruptions can change very rapidly, as new nodes may be created and others may be deleted at any point.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel},
	)
	NodePoolNodesConsumingBudgets = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "nodes_consuming_budgets",
			Help:      "The number of nodes consuming the budget of a nodepool at a point in time. Labeled by NodePool.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel},
	)
	DisruptionQueueFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_failures_total",
			Help:      "The number of times that an enqueued disruption decision failed. Labeled by disruption method.",
		},
		[]string{decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	ConsolidationScoreHistogram = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_score",
			Help:      "Score of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
			Buckets:   []float64{0.1, 0.25, 0.33, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
		[]string{decisionLabel, metrics.NodePoolLabel, policyLabel},
	)
	ConsolidationMovesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_moves_total",
			Help:      "Number of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
		},
		[]string{decisionLabel, metrics.NodePoolLabel, policyLabel},
	)
	ConsolidationCandidateDepth = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_candidate_depth",
			Help:      "Number of candidates evaluated in a single-node consolidation pass, or the deepest batch attempted by a multi-node binary search. Labeled by consolidation type.",
			Buckets:   consolidationCandidateBuckets,
		},
		[]string{ConsolidationTypeLabel},
	)
	ConsolidationCandidateDepthByNodePool = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_candidate_depth_by_nodepool",
			Help:      "Number of candidates evaluated per NodePool in a consolidation pass. Labeled by consolidation type and NodePool.",
			Buckets:   consolidationCandidateBuckets,
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel},
	)
	AcceptedCandidatePosition = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_accepted_candidate_position",
			Help:      "Zero-based position of the candidate that produced an accepted consolidation command. Multi-node batches emit once per candidate NodePool, so the sample count may exceed pass count.",
			Buckets:   consolidationCandidateBuckets,
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel},
	)
	PassStageSecondsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "pass_stage_seconds_total",
			Help:      "Cumulative wall-clock seconds consolidation passes spent per stage. Stages are non-overlapping, so rates across stages show how the pass time budget divides between cluster state copying, pod gathering, scheduler construction, simulation solving, candidate validation, and the deliberate validation wait.",
		},
		[]string{ConsolidationTypeLabel, stageLabel},
	)
	SchedulerConstructionDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "scheduler_construction_duration_seconds",
			Help:      "Duration spent constructing a scheduler during consolidation simulation.",
			Buckets:   durationBuckets,
		},
		[]string{ConsolidationTypeLabel},
	)
	ConsolidationSimulationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_simulation_duration_seconds",
			Help:      "Raw duration spent solving a consolidation simulation, excluding scheduler construction.",
			Buckets:   durationBuckets,
		},
		[]string{ConsolidationTypeLabel},
	)
	ConsolidationPassOutcomesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_pass_outcomes_total",
			Help:      "Number of consolidation passes by outcome and consolidation type.",
		},
		[]string{ConsolidationTypeLabel, outcomeLabel},
	)
	ConsolidationCommandsAdmittedPerPass = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_commands_admitted_per_pass",
			Help:      "Number of disruption commands admitted by a single consolidation pass that held more than one proposal. Only observed when batched admission is enabled, so a zero observation means every held proposal was rejected at admission.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5, 8, 10, 20},
		},
		[]string{ConsolidationTypeLabel},
	)
	ConsolidationAdmissionFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_admission_failures_total",
			Help:      "Number of held consolidation proposals that did not become commands, by the stage that rejected them and the reason.",
		},
		[]string{ConsolidationTypeLabel, stageLabel, reasonLabel},
	)
	ConsolidationCandidateSkipsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_candidate_skips_total",
			Help:      "Number of skipped single-node consolidation candidates by type, NodePool, candidate instance type, candidate capacity type, and reason, plus budget-exhausted candidates from both methods.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, instanceTypeLabel, metrics.CapacityTypeLabel, reasonLabel},
	)
	ConsolidationReplacementAttemptsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_replacement_attempts_total",
			Help:      "Number of single-node consolidation simulations by replacement count.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, replacementCountLabel},
	)
	ConsolidationRequiredReplacements = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_required_replacements",
			Help:      "Number of replacement NodeClaims required by single-node consolidation simulations needing more than one replacement, whether or not the count is within the configured maximum. Compare against candidate skips with reason multiple_replacements_required to see how many were blocked by the limit.",
			Buckets:   []float64{2, 3, 4, 5, 8, 10, 20, 50, 100},
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel},
	)
	ConsolidationExecutedNodesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_executed_nodes_total",
			Help:      "Number of nodes disrupted by consolidation commands that executed successfully, by type, NodePool, decision, the disrupted node's instance and capacity type, and the number of replacement NodeClaims the command launched. Counted per disrupted node rather than per command so a multi-node command attributes to each candidate's own NodePool. Compare against consolidation_replacement_attempts_total to see how many simulated multi-replacement options actually execute.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, decisionLabel, instanceTypeLabel, metrics.CapacityTypeLabel, replacementCountLabel},
	)
	ConsolidationReplacementLaunchesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_replacement_launches_total",
			Help:      "Number of replacement NodeClaims launched by consolidation commands that executed successfully, labeled with the instance and capacity type the command disrupted and the ones the replacement actually launched with. Counted per replacement, so a 1->2 command records both targets. from_instance_type is multiple when a command disrupted more than one instance type; to_instance_type is unknown when the launched NodeClaim's labels could not be read.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, fromInstanceTypeLabel, fromCapacityTypeLabel, toInstanceTypeLabel, toCapacityTypeLabel},
	)
	ConsolidationSplitAttemptsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_split_attempts_total",
			Help:      "Number of split fallback simulations by consolidation type, NodePool, and outcome, where a single-node candidate that no cheaper single replacement could absorb is re-simulated with its own price as a ceiling on new capacity. attempt_cap_exhausted counts candidates the fallback declined to retry because the pass already spent its attempt budget.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, outcomeLabel},
	)
	ConsolidationODToSpotRetryTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_od_to_spot_retries_total",
			Help:      "Number of on-demand to spot repricing retries by outcome, consolidation type, and NodePool. Counted per candidate, so a retry over a multi-node command records every candidate's NodePool. armed means the ordinary price filter emptied the replacements and started the retry; admitted means the retry produced a command; the rejection outcomes identify the retry constraint that prevented admission.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, outcomeLabel},
	)
	ConsolidationSpotZoneRetryTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_spot_zone_retries_total",
			Help:      "Number of spot-to-spot zone-narrowing retries by outcome, consolidation type, and NodePool. Counted per candidate, so a retry over a multi-node command records every candidate's NodePool. armed means the aggregate price filter emptied the spot replacements at their worst-case zone pricing and started the retry; admitted means narrowing the claims to their cheap spot zones produced a command; the rejection outcomes identify the retry constraint that prevented admission.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, outcomeLabel},
	)
	ConsolidationSplitSecondsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_split_seconds_total",
			Help:      "Cumulative wall-clock seconds spent in split fallback simulations, by consolidation type and NodePool. This time is also counted by the pass stage counters it runs inside, so it measures how much of a pass's timeout the fallback consumes at the expense of candidate traversal depth.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel},
	)
	ConsolidationRealizedSavingsDollarsPerHourTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_realized_savings_dollars_per_hour_total",
			Help:      "Cumulative realized hourly savings from successful consolidation commands.",
		},
		[]string{metrics.NodePoolLabel, decisionLabel, capacityTypeTransitionLabel},
	)
	// This gauge measures the same population as upstream's eligible_nodes, one rung below
	// actionable: constructible and unblocked, with no price consulted. Being ours to name, it says
	// candidate rather than inheriting a word that reads as a verdict.
	CandidateNodesByNodePool = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "candidate_nodes_by_nodepool",
			Help:      "Number of nodes that are candidates for disruption by NodePool, disruption reason, and consolidation type. A candidate is constructible and unblocked for the method; nothing here compares price, so read consolidation_actionable_candidates for the population that has a cheaper alternative.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	UnseenNodePoolsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "unseen_nodepools_total",
			Help:      "Number of NodePools with zero candidates evaluated in a timed-out consolidation pass.",
		},
		[]string{metrics.NodePoolLabel, ConsolidationTypeLabel},
	)
	WalkCycleCoverageRatio = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_walk_cycle_coverage_ratio",
			Help:      "Fraction of current candidates the running coverage cycle has reached across its timed-out passes. Reaches 1 (and resets) when every candidate has been evaluated; a ratio pinned below 1 means candidates are entering the set faster than the walk covers them.",
		},
		[]string{ConsolidationTypeLabel},
	)
)

var (
	candidateNodePoolsMu sync.Mutex
	candidateNodePools   = map[string]candidateNodePoolSeries{}
)

type candidateNodePoolSeries struct {
	labels map[string]string
	scope  string
	count  int
}

const (
	stageStateCopy    = "state_copy"
	stagePodGather    = "pod_gather"
	stageConstruction = "scheduler_construction"
	stageSimulation   = "simulation"
	// stageValidation covers candidate revalidation only; the nested command re-simulation accounts
	// for itself under the simulation stages and the deliberate delay under stageValidationWait,
	// keeping the stages additive.
	stageValidation     = "validation"
	stageValidationWait = "validation_wait"
)

// observePassStage accumulates wall-clock time since start into the pass stage counter. It is a
// no-op outside a consolidation pass (no consolidation type on the context).
func observePassStage(ctx context.Context, stage string, start time.Time) {
	if consolidationType := consolidationTypeFromContext(ctx); consolidationType != "" {
		PassStageSecondsTotal.Add(time.Since(start).Seconds(), map[string]string{ConsolidationTypeLabel: consolidationType, stageLabel: stage})
	}
}

// startPassStage starts timing a stage and returns a function that records the elapsed time the
// first time it is invoked; later invocations are no-ops. Calling it at stage completion and also
// deferring it keeps stages non-overlapping while still accounting for the budget consumed by
// stages cut short by an error or timeout.
func startPassStage(ctx context.Context, stage string) func() {
	start := time.Now()
	var once sync.Once
	return func() {
		once.Do(func() { observePassStage(ctx, stage, start) })
	}
}

// ObserveCandidateNodesByNodePool records the number of candidates per NodePool for a single disruption
// method's pass. method distinguishes passes that share the same reason and consolidation type labels (e.g.
// StaticDrift and Drift both report reason=drifted): each method owns its own set of series, so one method's pass
// never deletes or overwrites another's. Methods sharing labels must observe disjoint NodePool sets.
func ObserveCandidateNodesByNodePool(candidates []*Candidate, method, consolidationType, reason string) {
	scope := method + "\x00" + labelKeyWithout(map[string]string{
		metrics.ReasonLabel:    reason,
		ConsolidationTypeLabel: consolidationType,
	}, metrics.NodePoolLabel)
	current := map[string]candidateNodePoolSeries{}
	for _, candidate := range candidates {
		labels := map[string]string{
			metrics.NodePoolLabel:  candidate.NodePool.Name,
			metrics.ReasonLabel:    reason,
			ConsolidationTypeLabel: consolidationType,
		}
		key := method + "\x00" + labelKeyWithout(labels)
		series := current[key]
		series.labels = labels
		series.scope = scope
		series.count++
		current[key] = series
	}

	candidateNodePoolsMu.Lock()
	defer candidateNodePoolsMu.Unlock()
	for key, series := range candidateNodePools {
		if series.scope == scope {
			if _, ok := current[key]; ok {
				continue
			}
			CandidateNodesByNodePool.Delete(series.labels)
			delete(candidateNodePools, key)
		}
	}
	for key, series := range current {
		CandidateNodesByNodePool.Set(float64(series.count), series.labels)
		candidateNodePools[key] = series
	}
}

func labelKeyWithout(labels map[string]string, excluded ...string) string {
	excludedLabels := map[string]struct{}{}
	for _, label := range excluded {
		excludedLabels[label] = struct{}{}
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		if _, excluded := excludedLabels[key]; !excluded {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(labels[key])
		builder.WriteByte(0)
	}
	return builder.String()
}

func ObserveUnseenNodePools(consolidationType string, nodePools []string) {
	for _, nodePool := range nodePools {
		UnseenNodePoolsTotal.Inc(map[string]string{
			metrics.NodePoolLabel:  nodePool,
			ConsolidationTypeLabel: consolidationType,
		})
	}
}

// ObserveConsolidationCandidateSkip records a candidate the pass declined to act on. The
// instance and capacity type identify which shapes a reason is concentrated in, which
// NodePool alone cannot: a pool routinely mixes types whose consolidation outcomes differ.
// ObserveWalkCycleCoverage reports how much of the current candidate set the coverage cycle has
// reached, recorded when a pass times out.
func ObserveWalkCycleCoverage(consolidationType string, evaluated, candidates int) {
	if candidates == 0 {
		return
	}
	WalkCycleCoverageRatio.Set(float64(evaluated)/float64(candidates), map[string]string{ConsolidationTypeLabel: consolidationType})
}

func ObserveConsolidationCandidateSkip(consolidationType, nodePool, instanceType, capacityType, reason string) {
	ConsolidationCandidateSkipsTotal.Inc(map[string]string{
		ConsolidationTypeLabel:    consolidationType,
		metrics.NodePoolLabel:     nodePool,
		instanceTypeLabel:         orUnknown(instanceType),
		metrics.CapacityTypeLabel: orUnknown(capacityType),
		reasonLabel:               reason,
	})
}

func ObserveConsolidationODToSpotRetry(consolidationType string, candidates []*Candidate, outcome string) {
	for _, candidate := range candidates {
		ConsolidationODToSpotRetryTotal.Inc(map[string]string{
			ConsolidationTypeLabel: consolidationType,
			metrics.NodePoolLabel:  candidate.NodePool.Name,
			outcomeLabel:           outcome,
		})
	}
}

func ObserveConsolidationSpotZoneRetry(consolidationType string, candidates []*Candidate, outcome string) {
	for _, candidate := range candidates {
		ConsolidationSpotZoneRetryTotal.Inc(map[string]string{
			ConsolidationTypeLabel: consolidationType,
			metrics.NodePoolLabel:  candidate.NodePool.Name,
			outcomeLabel:           outcome,
		})
	}
}

// observeCandidateSkip records a skip for a candidate, resolving its types.
func observeCandidateSkip(consolidationType string, candidate *Candidate, reason string) {
	ObserveConsolidationCandidateSkip(consolidationType, candidate.NodePool.Name, candidateInstanceTypeName(candidate), candidate.capacityType, reason)
}

// candidateInstanceTypeName resolves a candidate's instance type, preferring the resolved
// InstanceType and falling back to the node's label for candidates whose NodePool no longer
// offers the type they were launched from.
func candidateInstanceTypeName(candidate *Candidate) string {
	if candidate == nil {
		return ""
	}
	if candidate.instanceType != nil {
		return candidate.instanceType.Name
	}
	if candidate.StateNode == nil {
		return ""
	}
	return candidate.Labels()[corev1.LabelInstanceTypeStable]
}

func orUnknown(value string) string {
	if value == "" {
		return unknownTypeValue
	}
	return value
}

// ObserveConsolidationCommandsAdmitted records how many of a batched pass's held proposals
// became commands.
func ObserveConsolidationCommandsAdmitted(consolidationType string, admitted int) {
	ConsolidationCommandsAdmittedPerPass.Observe(float64(admitted), map[string]string{
		ConsolidationTypeLabel: consolidationType,
	})
}

// ObserveConsolidationAdmissionFailure records a held proposal that did not become a command.
func ObserveConsolidationAdmissionFailure(consolidationType, stage, reason string) {
	ConsolidationAdmissionFailuresTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationType,
		stageLabel:             stage,
		reasonLabel:            reason,
	})
}

// ObserveConsolidationSplitAttempt records the outcome of one split fallback attempt, or of a
// candidate the fallback declined because the pass exhausted its attempt budget.
func ObserveConsolidationSplitAttempt(ctx context.Context, nodePool, outcome string) {
	ConsolidationSplitAttemptsTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationTypeFromContext(ctx),
		metrics.NodePoolLabel:  nodePool,
		outcomeLabel:           outcome,
	})
}

// ObserveConsolidationSplitDuration accumulates the wall-clock time a split fallback simulation took.
func ObserveConsolidationSplitDuration(ctx context.Context, nodePool string, duration time.Duration) {
	ConsolidationSplitSecondsTotal.Add(duration.Seconds(), map[string]string{
		ConsolidationTypeLabel: consolidationTypeFromContext(ctx),
		metrics.NodePoolLabel:  nodePool,
	})
}

// executedReplacementCountBucket bounds label cardinality while separating the
// counts a bounded 1->N replacement limit can produce.
func executedReplacementCountBucket(replacementCount int) string {
	switch {
	case replacementCount < 0:
		return "0"
	case replacementCount <= 3:
		return strconv.Itoa(replacementCount)
	default:
		return "4+"
	}
}

// ObserveExecutedConsolidationCommand records a command that finished
// successfully as one observation per node it disrupted, so a multi-node
// command attributes to each candidate's own NodePool instead of having to pick
// one. The counter is named for that unit: consolidation_executed_nodes_total.
func ObserveExecutedConsolidationCommand(cmd Command) {
	if cmd.Method == nil {
		return
	}
	bucket := executedReplacementCountBucket(len(cmd.Replacements))
	for _, candidate := range cmd.Candidates {
		ConsolidationExecutedNodesTotal.Inc(map[string]string{
			ConsolidationTypeLabel:    cmd.ConsolidationType(),
			metrics.NodePoolLabel:     candidate.NodePool.Name,
			decisionLabel:             string(cmd.Decision()),
			instanceTypeLabel:         orUnknown(candidateInstanceTypeName(candidate)),
			metrics.CapacityTypeLabel: orUnknown(candidate.capacityType),
			replacementCountLabel:     bucket,
		})
	}
}

// ObserveExecutedReplacementLaunches records what a successful command actually launched, one
// observation per replacement NodeClaim, so a 1->N command reports every target it created.
// The command only succeeds once its replacements are initialized, so the launched NodeClaim
// carries the instance and capacity type the cloud provider resolved rather than the set of
// options the simulation considered.
func ObserveExecutedReplacementLaunches(ctx context.Context, kubeClient client.Reader, cmd Command) {
	if cmd.Method == nil || len(cmd.Replacements) == 0 {
		return
	}
	fromInstanceType, fromCapacityType := commandSourceTypes(cmd)
	for _, replacement := range cmd.Replacements {
		toInstanceType, toCapacityType := replacementLaunchedTypes(ctx, kubeClient, replacement)
		ConsolidationReplacementLaunchesTotal.Inc(map[string]string{
			ConsolidationTypeLabel: cmd.ConsolidationType(),
			metrics.NodePoolLabel:  replacement.NodePoolName,
			fromInstanceTypeLabel:  fromInstanceType,
			fromCapacityTypeLabel:  fromCapacityType,
			toInstanceTypeLabel:    toInstanceType,
			toCapacityTypeLabel:    toCapacityType,
		})
	}
}

// commandSourceTypes describes the capacity a command removed. A command disrupting several
// distinct types collapses to multiple so multi-node commands cannot multiply cardinality by
// the number of type combinations they happen to cover.
func commandSourceTypes(cmd Command) (instanceType, capacityType string) {
	instanceTypes := make([]string, 0, len(cmd.Candidates))
	capacityTypes := make([]string, 0, len(cmd.Candidates))
	for _, candidate := range cmd.Candidates {
		instanceTypes = append(instanceTypes, orUnknown(candidateInstanceTypeName(candidate)))
		capacityTypes = append(capacityTypes, orUnknown(candidate.capacityType))
	}
	return collapseTypes(instanceTypes), collapseTypes(capacityTypes)
}

func collapseTypes(values []string) string {
	unique := uniqueSorted(values)
	switch len(unique) {
	case 0:
		return unknownTypeValue
	case 1:
		return unique[0]
	default:
		return multipleTypesValue
	}
}

// replacementLaunchedTypes reads the types a replacement launched with from its NodeClaim.
// The instance type is never inferred from the simulation's options: those routinely number in
// the hundreds, and recording an option the cloud provider did not pick would both misreport
// the conversion and make the series unbounded.
func replacementLaunchedTypes(ctx context.Context, kubeClient client.Reader, replacement *Replacement) (instanceType, capacityType string) {
	if replacement == nil {
		return unknownTypeValue, unknownTypeValue
	}
	nodeClaim := &v1.NodeClaim{}
	if replacement.Name != "" && kubeClient.Get(ctx, types.NamespacedName{Name: replacement.Name}, nodeClaim) == nil {
		instanceType = nodeClaim.Labels[corev1.LabelInstanceTypeStable]
		capacityType = nodeClaim.Labels[v1.CapacityTypeLabelKey]
	}
	if capacityType == "" {
		// The capacity type requirement is bounded by the capacity types the NodePool allows, so
		// falling back to it stays low cardinality and still separates spot from on-demand.
		if requirement := replacement.Requirements.Get(v1.CapacityTypeLabelKey); requirement != nil && requirement.Len() == 1 {
			capacityType = requirement.Values()[0]
		}
	}
	return orUnknown(instanceType), orUnknown(capacityType)
}

func ObserveConsolidationReplacementAttempt(consolidationType, nodePool string, replacementCount int) {
	bucket := "2+"
	switch replacementCount {
	case 0:
		bucket = "0"
	case 1:
		bucket = "1"
	}
	ConsolidationReplacementAttemptsTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationType,
		metrics.NodePoolLabel:  nodePool,
		replacementCountLabel:  bucket,
	})
}

func ObserveConsolidationPass(consolidationType, outcome string, depth int) {
	ConsolidationCandidateDepth.Observe(float64(depth), map[string]string{ConsolidationTypeLabel: consolidationType})
	ConsolidationPassOutcomesTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationType,
		outcomeLabel:           outcome,
	})
}

func ObserveConsolidationCandidateDepthByNodePool(consolidationType string, depths map[string]int) {
	for nodePool, depth := range depths {
		ConsolidationCandidateDepthByNodePool.Observe(float64(depth), map[string]string{
			ConsolidationTypeLabel: consolidationType,
			metrics.NodePoolLabel:  nodePool,
		})
	}
}

func ObserveAcceptedCandidate(cmd Command, consolidationType string, position int) {
	for _, candidate := range cmd.Candidates {
		AcceptedCandidatePosition.Observe(float64(position), map[string]string{
			ConsolidationTypeLabel: consolidationType,
			metrics.NodePoolLabel:  candidate.NodePool.Name,
		})
	}
}

func ObserveRealizedSavings(ctx context.Context, kubeClient client.Reader, cmd Command) {
	transition := capacityTypeTransition(ctx, kubeClient, cmd)
	for _, candidate := range cmd.Candidates {
		ConsolidationRealizedSavingsDollarsPerHourTotal.Add(cmd.EstimatedSavings()/float64(len(cmd.Candidates)), map[string]string{
			metrics.NodePoolLabel:       candidate.NodePool.Name,
			decisionLabel:               string(cmd.Decision()),
			capacityTypeTransitionLabel: transition,
		})
	}
}

func capacityTypeTransition(ctx context.Context, kubeClient client.Reader, cmd Command) string {
	sources := make([]string, 0, len(cmd.Candidates))
	for _, candidate := range cmd.Candidates {
		sources = append(sources, candidate.capacityType)
	}
	sources = uniqueSorted(sources)
	destinations := make([]string, 0, len(cmd.Replacements))
	for _, replacement := range cmd.Replacements {
		nodeClaim := &v1.NodeClaim{}
		if replacement.Name != "" && kubeClient.Get(ctx, types.NamespacedName{Name: replacement.Name}, nodeClaim) == nil {
			if capacityType := nodeClaim.Labels[v1.CapacityTypeLabelKey]; capacityType != "" {
				destinations = append(destinations, capacityType)
				continue
			}
		}
		if requirement := replacement.Requirements.Get(v1.CapacityTypeLabelKey); requirement != nil {
			destinations = append(destinations, requirement.Values()...)
		}
	}
	destinations = uniqueSorted(destinations)
	if len(destinations) == 0 {
		destinations = []string{"none"}
	}
	return strings.Join(sources, ",") + "->" + strings.Join(destinations, ",")
}

func uniqueSorted(values []string) []string {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	result := make([]string, 0, len(sorted))
	for _, value := range sorted {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}
