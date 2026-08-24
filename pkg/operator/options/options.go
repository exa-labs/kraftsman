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

package options

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/samber/lo"
	cliflag "k8s.io/component-base/cli/flag"

	"sigs.k8s.io/karpenter/pkg/utils/env"
)

type PreferencePolicy string

const (
	PreferencePolicyIgnore  PreferencePolicy = "Ignore"
	PreferencePolicyRespect PreferencePolicy = "Respect"
)

type MinValuesPolicy string

// TopologyCountCacheMode controls whether one scheduling pass reuses the per-topology-group pod
// domain counts across its candidate simulations. "off" scans the cluster's pods for every group
// of every candidate, "shadow" reuses nothing but computes both and counts divergences, and "on"
// scans once per group per pass and replays the records for every later candidate.
type TopologyCountCacheMode string

const (
	TopologyCountCacheModeOff    TopologyCountCacheMode = "off"
	TopologyCountCacheModeShadow TopologyCountCacheMode = "shadow"
	TopologyCountCacheModeOn     TopologyCountCacheMode = "on"
)

const (
	MinValuesPolicyStrict     MinValuesPolicy = "Strict"
	MinValuesPolicyBestEffort MinValuesPolicy = "BestEffort"
)

var (
	validLogLevels          = []string{"", "debug", "info", "error"}
	validPreferencePolicies = []PreferencePolicy{PreferencePolicyIgnore, PreferencePolicyRespect}

	Injectables = []Injectable{&Options{}}
)

type optionsKey struct{}

type FeatureGates struct {
	inputStr string

	NodeRepair              bool
	ReservedCapacity        bool
	SpotToSpotConsolidation bool
	NodeOverlay             bool
	StaticCapacity          bool
	CapacityBuffer          bool
}

// Options contains all CLI flags / env vars for karpenter-core. It adheres to the options.Injectable interface.
type Options struct {
	ServiceName                         string
	MetricsPort                         int
	HealthProbePort                     int
	KubeClientQPS                       int
	KubeClientBurst                     int
	EnableProfiling                     bool
	DisableControllerWarmup             bool
	DisableLeaderElection               bool
	DisableClusterStateObservability    bool
	LeaderElectionName                  string
	LeaderElectionNamespace             string
	MemoryLimit                         int64
	CPURequests                         int64
	LogLevel                            string
	LogOutputPaths                      string
	LogErrorOutputPaths                 string
	BatchMaxDuration                    time.Duration
	BatchIdleDuration                   time.Duration
	NodeMetricsInterval                 time.Duration
	preferencePolicyRaw                 string
	PreferencePolicy                    PreferencePolicy
	minValuesPolicyRaw                  string
	MinValuesPolicy                     MinValuesPolicy
	IgnoreDRARequests                   bool // NOTE: This flag will be removed once formal DRA support is GA in Karpenter.
	MaxConsolidationReplacements        int
	MaxConsolidationCommandsPerPass     int
	ConsolidationSplitFallback          bool
	ConsolidationSplitMaxAttempts       int
	ConsolidationSplitMinSavings        float64
	ConsolidationReplaceMinSavings      float64
	SpotToSpotMinInstanceTypes          int
	ConsolidationCandidateTimeout       time.Duration
	ConsolidationAttributeReplacements  bool
	ConsolidationSkipUnchangedNegatives bool
	ConsolidationNegativeCacheTTL       time.Duration
	NodeClaimInitializationTimeout      time.Duration
	ODToSpotConsolidation               bool
	topologyCountCacheModeRaw           string
	TopologyCountCacheMode              TopologyCountCacheMode
	FeatureGates                        FeatureGates
}

type FlagSet struct {
	*flag.FlagSet
}

// BoolVarWithEnv defines a bool flag with a specified name, default value, usage string, and fallback environment
// variable.
func (fs *FlagSet) BoolVarWithEnv(p *bool, name string, envVar string, val bool, usage string) {
	*p = env.WithDefaultBool(envVar, val)
	fs.BoolFunc(name, usage, func(val string) error {
		if val != "true" && val != "false" {
			return fmt.Errorf("%q is not a valid value, must be true or false", val)
		}
		*p = (val) == "true"
		return nil
	})
}

func (o *Options) AddFlags(fs *FlagSet) {
	fs.StringVar(&o.ServiceName, "karpenter-service", env.WithDefaultString("KARPENTER_SERVICE", ""), "The Karpenter Service name for the dynamic webhook certificate")
	fs.IntVar(&o.MetricsPort, "metrics-port", env.WithDefaultInt("METRICS_PORT", 8080), "The port the metric endpoint binds to for operating metrics about the controller itself")
	fs.IntVar(&o.HealthProbePort, "health-probe-port", env.WithDefaultInt("HEALTH_PROBE_PORT", 8081), "The port the health probe endpoint binds to for reporting controller health")
	fs.IntVar(&o.KubeClientQPS, "kube-client-qps", env.WithDefaultInt("KUBE_CLIENT_QPS", 200), "The smoothed rate of qps to kube-apiserver")
	fs.IntVar(&o.KubeClientBurst, "kube-client-burst", env.WithDefaultInt("KUBE_CLIENT_BURST", 300), "The maximum allowed burst of queries to the kube-apiserver")
	fs.BoolVarWithEnv(&o.EnableProfiling, "enable-profiling", "ENABLE_PROFILING", false, "Enable the profiling on the metric endpoint")
	fs.BoolVarWithEnv(&o.DisableControllerWarmup, "disable-controller-warmup", "DISABLE_CONTROLLER_WARMUP", true, "Disable controller warmup which starts controller sources before leader election is won. Controller warmup pre-populates caches and improves leader failover time.")
	fs.BoolVarWithEnv(&o.DisableLeaderElection, "disable-leader-election", "DISABLE_LEADER_ELECTION", false, "Disable the leader election client before executing the main loop. Disable when running replicated components for high availability is not desired.")
	fs.BoolVarWithEnv(&o.DisableClusterStateObservability, "disable-cluster-state-observability", "DISABLE_CLUSTER_STATE_OBSERVABILITY", false, "Disable cluster state metrics and events")
	fs.StringVar(&o.LeaderElectionName, "leader-election-name", env.WithDefaultString("LEADER_ELECTION_NAME", "karpenter-leader-election"), "Leader election name to create and monitor the lease if running outside the cluster")
	fs.StringVar(&o.LeaderElectionNamespace, "leader-election-namespace", env.WithDefaultString("LEADER_ELECTION_NAMESPACE", ""), "Leader election namespace to create and monitor the lease if running outside the cluster")
	fs.Int64Var(&o.MemoryLimit, "memory-limit", env.WithDefaultInt64("MEMORY_LIMIT", -1), "Memory limit on the container running the controller. The GC soft memory limit is set to 90% of this value.")
	fs.Int64Var(&o.CPURequests, "cpu-requests", env.WithDefaultInt64("CPU_REQUESTS", 1000), "CPU requests in millicores on the container running the controller.")
	fs.StringVar(&o.LogLevel, "log-level", env.WithDefaultString("LOG_LEVEL", "info"), "Log verbosity level. Can be one of 'debug', 'info', or 'error'")
	fs.StringVar(&o.LogOutputPaths, "log-output-paths", env.WithDefaultString("LOG_OUTPUT_PATHS", "stdout"), "Optional comma separated paths for directing log output")
	fs.StringVar(&o.LogErrorOutputPaths, "log-error-output-paths", env.WithDefaultString("LOG_ERROR_OUTPUT_PATHS", "stderr"), "Optional comma separated paths for logging error output")
	fs.DurationVar(&o.BatchMaxDuration, "batch-max-duration", env.WithDefaultDuration("BATCH_MAX_DURATION", 10*time.Second), "The maximum length of a batch window. The longer this is, the more pods we can consider for provisioning at one time which usually results in fewer but larger nodes.")
	fs.DurationVar(&o.BatchIdleDuration, "batch-idle-duration", env.WithDefaultDuration("BATCH_IDLE_DURATION", time.Second), "The maximum amount of time with no new pending pods that if exceeded ends the current batching window. If pods arrive faster than this time, the batching window will be extended up to the maxDuration. If they arrive slower, the pods will be batched separately.")
	fs.DurationVar(&o.NodeMetricsInterval, "node-metrics-interval", env.WithDefaultDuration("NODE_METRICS_INTERVAL", 30*time.Second), "The interval at which per-node state metrics (allocatable, pod requests/limits, lifetime, cluster utilization) are rebuilt and re-emitted. Larger clusters may want a longer interval since each rebuild walks every node.")
	fs.StringVar(&o.preferencePolicyRaw, "preference-policy", env.WithDefaultString("PREFERENCE_POLICY", string(PreferencePolicyRespect)), "How the Karpenter scheduler should treat preferences. Preferences include preferredDuringSchedulingIgnoreDuringExecution node and pod affinities/anti-affinities and ScheduleAnyways topologySpreadConstraints. Can be one of 'Ignore' and 'Respect'")
	fs.StringVar(&o.minValuesPolicyRaw, "min-values-policy", env.WithDefaultString("MIN_VALUES_POLICY", string(MinValuesPolicyStrict)), "Min values policy for scheduling. Options include 'Strict' for existing behavior where min values are strictly enforced or 'BestEffort' where Karpenter relaxes min values when it isn't satisfied.")
	fs.IntVar(&o.MaxConsolidationReplacements, "max-consolidation-replacements", env.WithDefaultInt("MAX_CONSOLIDATION_REPLACEMENTS", 1), "The maximum number of replacement nodes a single consolidation candidate may be split into. 1 preserves the classic 1->1 behavior; higher values allow bounded 1->N consolidation (e.g. replacing one large on-demand node with several smaller spot nodes) when the aggregate replacement price is lower.")
	fs.IntVar(&o.MaxConsolidationCommandsPerPass, "max-consolidation-commands-per-pass", env.WithDefaultInt("MAX_CONSOLIDATION_COMMANDS_PER_PASS", 1), "The maximum number of disruption commands a single single-node consolidation pass may admit. 1 preserves the classic one-command-per-pass behavior; higher values let a pass that has already paid for candidate discovery admit several non-overlapping commands, each still validated against live cluster state immediately before it is queued.")
	fs.BoolVarWithEnv(&o.ConsolidationSplitFallback, "consolidation-split-fallback", "CONSOLIDATION_SPLIT_FALLBACK", false, "When set, a single-node consolidation candidate that no cheaper single replacement can absorb is re-simulated with the candidate's own price as a ceiling on new capacity, so the scheduler packs its pods onto several cheaper nodes instead. Bounded by max-consolidation-replacements and consolidation-split-max-attempts.")
	fs.IntVar(&o.ConsolidationSplitMaxAttempts, "consolidation-split-max-attempts", env.WithDefaultInt("CONSOLIDATION_SPLIT_MAX_ATTEMPTS", 50), "The maximum number of split fallback simulations a single consolidation pass may run. Each attempt costs an extra scheduling simulation, so this caps how much of the pass timeout the fallback can consume at the expense of candidate traversal depth. 0 disables the fallback.")
	fs.IntVar(&o.SpotToSpotMinInstanceTypes, "spot-to-spot-min-instance-types", env.WithDefaultInt("SPOT_TO_SPOT_MIN_INSTANCE_TYPES", 15), "The minimum number of cheaper instance type options a replacement NodeClaim must have for spot-to-spot single-node consolidation to proceed. The upstream default of 15 assumes broad instance-type flexibility; a fleet whose pods pin a single small instance family can never present that many cheaper types and needs a lower minimum. Replacement launches are capped to this many cheapest options too (or the NodePool's minValues if greater), so the launched type is always within the priced set and cannot be immediately consolidated again.")
	fs.DurationVar(&o.ConsolidationCandidateTimeout, "consolidation-candidate-timeout", env.WithDefaultDuration("CONSOLIDATION_CANDIDATE_TIMEOUT", 10*time.Second), "The maximum time a single consolidation candidate's scheduling simulation may run before it is abandoned and the walk moves on. The pass timeout bounds discovery in aggregate; this bounds one candidate, so a pass degrades into finding fewer commands rather than none. 0 disables the per-candidate bound.")
	fs.BoolVar(&o.ConsolidationAttributeReplacements, "consolidation-attribute-replacements", env.WithDefaultBool("CONSOLIDATION_ATTRIBUTE_REPLACEMENTS", true), "Count only the new NodeClaims that host a disrupted pod as a command's replacements. A consolidation simulation also schedules the cluster's pending pods, and the capacity it opens for them would otherwise be priced against the candidate and counted against the replacement bound. Disable to restore the unattributed behavior.")
	fs.BoolVarWithEnv(&o.ConsolidationSkipUnchangedNegatives, "consolidation-skip-unchanged-negatives", "CONSOLIDATION_SKIP_UNCHANGED_NEGATIVES", false, "When set, a single-node consolidation candidate whose previous simulation ended in a no-op is skipped while its fingerprint - Node, NodeClaim and NodePool resourceVersions, reschedulable pod set, and the NodePool's instance type revision - is unchanged and the verdict is younger than consolidation-negative-cache-ttl. Only no-op verdicts are cached, so a stale entry can only delay a node's consolidation, never disrupt one wrongly; the cache is dropped whenever a pass admits a command. Lookup outcomes are counted regardless of this flag, so the hit rate is measurable before skipping is enabled.")
	fs.DurationVar(&o.ConsolidationNegativeCacheTTL, "consolidation-negative-cache-ttl", env.WithDefaultDuration("CONSOLIDATION_NEGATIVE_CACHE_TTL", 5*time.Minute), "How long a cached no-op consolidation verdict remains valid. The fingerprint covers the candidate's own inputs; the TTL bounds what it cannot see, chiefly capacity elsewhere in the fleet freeing up, so it is the upper bound on how long a consolidatable node can be delayed by a stale verdict.")
	fs.BoolVarWithEnv(&o.ODToSpotConsolidation, "od-to-spot-consolidation", "OD_TO_SPOT_CONSOLIDATION", true, "When set, a consolidation candidate running on-demand whose replacement found nothing cheaper is re-evaluated against spot offerings only, restricted to the zones whose spot price beats the candidate. The replacement launch is pinned to spot and those zones, so insufficient spot capacity fails the launch instead of falling back to on-demand. Enabled by default; set to false to opt out.")
	fs.Float64Var(&o.ConsolidationReplaceMinSavings, "consolidation-replace-min-savings", env.WithDefaultFloat64("CONSOLIDATION_REPLACE_MIN_SAVINGS", 0), "The fraction of the disrupted nodes' price that any consolidation replacement must save before it is accepted, on top of the usual cheaper-than-candidate check. Applies to every replace decision, including spot-to-spot and the split fallback (which uses the larger of this and consolidation-split-min-savings); delete decisions are unaffected. Replacement launches are also restricted to instance types that meet the margin. 0 accepts any cheaper replacement.")
	fs.Float64Var(&o.ConsolidationSplitMinSavings, "consolidation-split-min-savings", env.WithDefaultFloat64("CONSOLIDATION_SPLIT_MIN_SAVINGS", 0.05), "The fraction of a candidate's price that a split replacement must save before it is accepted, on top of the usual cheaper-than-candidate check. Guards against churning a node into several nodes for a negligible price difference.")
	fs.DurationVar(&o.NodeClaimInitializationTimeout, "nodeclaim-initialization-timeout", env.WithDefaultDuration("NODECLAIM_INITIALIZATION_TIMEOUT", 0), "The maximum time a registered NodeClaim may stay uninitialized before it is deleted. Registration only means the kubelet joined; a node whose startup taints are never removed, or whose requested extended resources never appear, stays registered and uninitialized indefinitely, holding an instance that runs no workload and that disruption still models with its full capacity. A bootstrap that fails every time replaces one stranded instance with a delete and reprovision once per timeout, as the registration timeout already does, so set it well above the slowest healthy bootstrap. 0 disables the timeout.")
	fs.StringVar(&o.topologyCountCacheModeRaw, "topology-count-cache-mode", env.WithDefaultString("TOPOLOGY_COUNT_CACHE_MODE", string(TopologyCountCacheModeOff)), "Whether a scheduling pass reuses each topology group's pod domain counts across its candidate simulations. The counts are a pure function of inputs the pass already pins, so the replay is exact; 'shadow' computes both paths, uses the fresh one, and counts divergences in the topology_count_cache_events_total metric, which is the evidence to collect before switching to 'on'. Can be one of 'off', 'shadow', and 'on'.")
	fs.BoolVarWithEnv(&o.IgnoreDRARequests, "ignore-dra-requests", "IGNORE_DRA_REQUESTS", true, "When set, Karpenter will ignore pods' DRA requests during scheduling simulations. NOTE: This flag will be removed once formal DRA support is GA in Karpenter.")
	fs.StringVar(&o.FeatureGates.inputStr, "feature-gates", env.WithDefaultString("FEATURE_GATES", "NodeRepair=false,ReservedCapacity=true,SpotToSpotConsolidation=false,NodeOverlay=false,StaticCapacity=false,CapacityBuffer=false"), "Optional features can be enabled / disabled using feature gates. Current options are: NodeRepair, ReservedCapacity, SpotToSpotConsolidation, NodeOverlay, StaticCapacity, and CapacityBuffer.")
}

func (o *Options) Parse(fs *FlagSet, args ...string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		return fmt.Errorf("parsing flags, %w", err)
	}
	if !lo.Contains(validLogLevels, o.LogLevel) {
		return fmt.Errorf("validating cli flags / env vars, invalid LOG_LEVEL %q", o.LogLevel)
	}
	if !lo.Contains(validPreferencePolicies, PreferencePolicy(o.preferencePolicyRaw)) {
		return fmt.Errorf("validating cli flags / env vars, invalid PREFERENCE_POLICY %q", o.preferencePolicyRaw)
	}
	if !lo.Contains([]MinValuesPolicy{MinValuesPolicyStrict, MinValuesPolicyBestEffort}, MinValuesPolicy(o.minValuesPolicyRaw)) {
		return fmt.Errorf("validating cli flags / env vars, invalid MIN_VALUES_POLICY %q", o.minValuesPolicyRaw)
	}
	if o.CPURequests <= 0 {
		o.CPURequests = 1000
	}
	if err := o.validateConsolidation(); err != nil {
		return err
	}
	if !lo.Contains([]TopologyCountCacheMode{TopologyCountCacheModeOff, TopologyCountCacheModeShadow, TopologyCountCacheModeOn}, TopologyCountCacheMode(o.topologyCountCacheModeRaw)) {
		return fmt.Errorf("validating cli flags / env vars, invalid TOPOLOGY_COUNT_CACHE_MODE %q", o.topologyCountCacheModeRaw)
	}
	if o.NodeClaimInitializationTimeout < 0 {
		return fmt.Errorf("validating cli flags / env vars, NODECLAIM_INITIALIZATION_TIMEOUT must be >= 0, got %s", o.NodeClaimInitializationTimeout)
	}
	gates, err := ParseFeatureGates(o.FeatureGates.inputStr)
	if err != nil {
		return fmt.Errorf("parsing feature gates, %w", err)
	}
	o.FeatureGates = gates
	o.PreferencePolicy = PreferencePolicy(o.preferencePolicyRaw)
	o.MinValuesPolicy = MinValuesPolicy(o.minValuesPolicyRaw)
	o.TopologyCountCacheMode = TopologyCountCacheMode(o.topologyCountCacheModeRaw)
	return nil
}

func (o *Options) validateConsolidation() error {
	if o.MaxConsolidationReplacements < 1 {
		return fmt.Errorf("validating cli flags / env vars, MAX_CONSOLIDATION_REPLACEMENTS must be >= 1, got %d", o.MaxConsolidationReplacements)
	}
	if o.MaxConsolidationCommandsPerPass < 1 {
		return fmt.Errorf("validating cli flags / env vars, MAX_CONSOLIDATION_COMMANDS_PER_PASS must be >= 1, got %d", o.MaxConsolidationCommandsPerPass)
	}
	if o.ConsolidationSplitMaxAttempts < 0 {
		return fmt.Errorf("validating cli flags / env vars, CONSOLIDATION_SPLIT_MAX_ATTEMPTS must be >= 0, got %d", o.ConsolidationSplitMaxAttempts)
	}
	if o.ConsolidationCandidateTimeout < 0 {
		return fmt.Errorf("validating cli flags / env vars, CONSOLIDATION_CANDIDATE_TIMEOUT must be >= 0, got %s", o.ConsolidationCandidateTimeout)
	}
	if o.SpotToSpotMinInstanceTypes < 1 {
		return fmt.Errorf("validating cli flags / env vars, SPOT_TO_SPOT_MIN_INSTANCE_TYPES must be >= 1, got %d", o.SpotToSpotMinInstanceTypes)
	}
	if o.ConsolidationNegativeCacheTTL <= 0 && o.ConsolidationSkipUnchangedNegatives {
		return fmt.Errorf("validating cli flags / env vars, CONSOLIDATION_NEGATIVE_CACHE_TTL must be > 0 when CONSOLIDATION_SKIP_UNCHANGED_NEGATIVES is set, got %s", o.ConsolidationNegativeCacheTTL)
	}
	if o.ConsolidationSplitMinSavings < 0 || o.ConsolidationSplitMinSavings >= 1 {
		return fmt.Errorf("validating cli flags / env vars, CONSOLIDATION_SPLIT_MIN_SAVINGS must be in [0, 1), got %f", o.ConsolidationSplitMinSavings)
	}
	if o.ConsolidationReplaceMinSavings < 0 || o.ConsolidationReplaceMinSavings >= 1 {
		return fmt.Errorf("validating cli flags / env vars, CONSOLIDATION_REPLACE_MIN_SAVINGS must be in [0, 1), got %f", o.ConsolidationReplaceMinSavings)
	}
	return nil
}

func (o *Options) ToContext(ctx context.Context) context.Context {
	return ToContext(ctx, o)
}

func DefaultFeatureGates() FeatureGates {
	return FeatureGates{
		NodeRepair:              false,
		ReservedCapacity:        true,
		SpotToSpotConsolidation: false,
		NodeOverlay:             false,
		StaticCapacity:          false,
		CapacityBuffer:          false,
	}
}

func ParseFeatureGates(gateStr string) (FeatureGates, error) {
	gateMap := map[string]bool{}
	gates := DefaultFeatureGates()

	// Parses feature gates with the upstream mechanism. This is meant to be used with flag directly but this enables
	// simple merging with environment vars.
	if err := cliflag.NewMapStringBool(&gateMap).Set(gateStr); err != nil {
		return gates, err
	}
	if val, ok := gateMap["NodeRepair"]; ok {
		gates.NodeRepair = val
	}
	if val, ok := gateMap["SpotToSpotConsolidation"]; ok {
		gates.SpotToSpotConsolidation = val
	}
	if val, ok := gateMap["ReservedCapacity"]; ok {
		gates.ReservedCapacity = val
	}
	if val, ok := gateMap["NodeOverlay"]; ok {
		gates.NodeOverlay = val
	}
	if val, ok := gateMap["StaticCapacity"]; ok {
		gates.StaticCapacity = val
	}
	if val, ok := gateMap["CapacityBuffer"]; ok {
		gates.CapacityBuffer = val
	}

	return gates, nil
}

func ToContext(ctx context.Context, opts *Options) context.Context {
	return context.WithValue(ctx, optionsKey{}, opts)
}

func FromContext(ctx context.Context) *Options {
	retval := ctx.Value(optionsKey{})
	if retval == nil {
		// This is a developer error if this happens, so we should panic
		panic("options doesn't exist in context")
	}
	return retval.(*Options)
}
