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

package scheduling

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	ControllerLabel    = "controller"
	schedulingIDLabel  = "scheduling_id"
	schedulerSubsystem = "scheduler"
	phaseLabel         = "phase"
	outcomeLabel       = "outcome"

	phaseDomainGroups         = "domain_groups"
	phaseTopologyUpdate       = "topology_update"
	phaseReservationManager   = "reservation_manager"
	phaseExistingNodes        = "existing_nodes"
	phaseNodeClaimTemplates   = "node_claim_templates"
	phaseDaemonOverheadGroups = "daemon_overhead_groups"

	cacheOutcomeHit    = "hit"
	cacheOutcomeMiss   = "miss"
	cacheOutcomeBypass = "bypass"

	cacheOutcomeShadowMatch    = "shadow_match"
	cacheOutcomeShadowMismatch = "shadow_mismatch"

	modeLabel = "mode"

	fingerprintModeRevision = "revision"
	fingerprintModeContent  = "content"
	fingerprintModeMixed    = "mixed"
)

var (
	DurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "scheduling_duration_seconds",
			Help:      "Duration of scheduling simulations used for deprovisioning and provisioning in seconds.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{
			ControllerLabel,
		},
	)
	QueueDepth = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "queue_depth",
			Help:      "The number of pods currently waiting to be scheduled.",
		},
		[]string{
			ControllerLabel,
			schedulingIDLabel,
		},
	)
	UnfinishedWorkSeconds = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "unfinished_work_seconds",
			Help:      "How many seconds of work has been done that is in progress and hasn't been observed by scheduling_duration_seconds.",
		},
		[]string{
			ControllerLabel,
			schedulingIDLabel,
		},
	)
	IgnoredPodCount = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "ignored_pods_count",
			Help:      "Number of pods ignored during scheduling by Karpenter",
		},
		[]string{},
	)
	UnschedulablePodsCount = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "unschedulable_pods_count",
			Help:      "The number of unschedulable Pods.",
		},
		[]string{
			ControllerLabel,
		},
	)
	ConstructionPhaseDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "construction_phase_duration_seconds",
			Help:      "Duration of individual scheduler construction phases, labeled by phase.",
			Buckets:   []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{
			phaseLabel,
		},
	)
	DomainGroupCacheEventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "domain_group_cache_events_total",
			Help:      "Number of pass-scoped domain group cache lookups by outcome (hit, miss, bypass).",
		},
		[]string{
			outcomeLabel,
		},
	)
	NodeRequirementCacheEventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "node_requirement_cache_events_total",
			Help:      "Number of pass-scoped node label requirement cache lookups by outcome (hit, miss, bypass).",
		},
		[]string{
			outcomeLabel,
		},
	)
	ReservationCapacityCacheEventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "reservation_capacity_cache_events_total",
			Help:      "Number of pass-scoped reservation capacity cache lookups by outcome (hit, miss, bypass).",
		},
		[]string{
			outcomeLabel,
		},
	)
	NodeClaimTemplateCacheEventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "node_claim_template_cache_events_total",
			Help:      "Number of pass-scoped NodeClaim template cache lookups by outcome (hit, miss, bypass).",
		},
		[]string{
			outcomeLabel,
		},
	)
	DaemonOverheadGroupCacheEventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "daemon_overhead_group_cache_events_total",
			Help:      "Number of pass-scoped daemon overhead group cache lookups by outcome (hit, miss, bypass).",
		},
		[]string{
			outcomeLabel,
		},
	)
	InverseAffinityCacheEventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "inverse_affinity_cache_events_total",
			Help:      "Number of pass-scoped inverse anti-affinity term cache lookups by outcome (hit, miss, bypass).",
		},
		[]string{
			outcomeLabel,
		},
	)
	TopologyPassCacheEventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "topology_pass_cache_events_total",
			Help:      "Number of pass-scoped topology pod list and node lookup cache events by outcome (hit, miss).",
		},
		[]string{
			outcomeLabel,
		},
	)
	TopologyCountCacheEventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "topology_count_cache_events_total",
			Help:      "Number of pass-scoped topology group pod domain count cache events by outcome (hit, miss, shadow_match, shadow_mismatch). Any shadow_mismatch means the replayed counts diverged from a fresh scan and the cache must stay off.",
		},
		[]string{
			outcomeLabel,
		},
	)
	DomainGroupFingerprintTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "domain_group_fingerprint_total",
			Help:      "Number of domain group cache fingerprints by mode (revision: cheap provider-revision path, content: full requirement content hashing, mixed: some NodePools had revisions and some did not).",
		},
		[]string{
			modeLabel,
		},
	)
	PendingPodsByEffectiveZone = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "pending_pods_by_effective_zone_count",
			Help:      "Pending pods dimensioned by effective zone constraint, or the intersection of pod-level zone signals, volume topology (PVC zones), and topology constraints. Values: specific zone name (e.g., 'us-west-2a'), 'flexible' (multiple zones), or 'none' (no valid intersection).",
		},
		[]string{
			ControllerLabel,
			"zone",
		},
	)
)
