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
	"strings"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/operator/options"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	operatorlogging "sigs.k8s.io/karpenter/pkg/operator/logging"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

var errCandidateDeleting = fmt.Errorf("candidate is deleting")

//nolint:gocyclo
func SimulateScheduling(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, clk clock.Clock, recorder events.Recorder,
	schedulerOpts []scheduling.Options, candidates ...*Candidate,
) (scheduling.Results, error) {
	candidateNames := sets.NewString(lo.Map(candidates, func(t *Candidate, i int) string { return t.Name() })...)
	stateCopyStart := time.Now()
	nodes := cluster.SimulationCopyNodes()
	observePassStage(ctx, stageStateCopy, stateCopyStart)
	deletingNodes := nodes.Deleting()
	stateNodes := lo.Filter(nodes.Active(), func(n *state.StateNode, _ int) bool {
		return !candidateNames.Has(n.Name())
	})

	// We do one final check to ensure that the node that we are attempting to consolidate isn't
	// already handled for deletion by some other controller. This could happen if the node was markedForDeletion
	// between returning the candidates and getting the stateNodes above
	if _, ok := lo.Find(deletingNodes, func(n *state.StateNode) bool {
		return candidateNames.Has(n.Name())
	}); ok {
		return scheduling.Results{}, errCandidateDeleting
	}

	// start by getting all pending pods
	endPodGather := startPassStage(ctx, stagePodGather)
	defer endPodGather()
	pods, err := pendingPodsForPass(ctx, provisioner)
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("determining pending pods, %w", err)
	}

	// Don't provision capacity for pods which will not get evicted due to fully blocking PDBs.
	// Since Karpenter doesn't know when these pods will be successfully evicted, spinning up capacity until
	// these pods are evicted is wasteful.
	pdbs, err := pdbLimitsForPass(ctx, kubeClient)
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	// candidatePods are the pods on consolidation candidate nodes that we will reschedule. Their UIDs feed
	// deletingPodUIDs below so the DRA allocator frees the devices they hold and re-allocates their claims onto the
	// replacement capacity, mirroring how pods on already-deleting nodes are treated.
	var candidatePods []*corev1.Pod
	for _, n := range candidates {
		currentlyReschedulablePods := lo.Filter(n.reschedulablePods, func(p *corev1.Pod, _ int) bool {
			return pdbs.IsCurrentlyReschedulable(p, clk, recorder)
		})
		candidatePods = append(candidatePods, currentlyReschedulablePods...)
	}
	pods = append(pods, candidatePods...)

	// We get the pods that are on nodes that are deleting
	deletingNodePods, err := deletingNodes.CurrentlyReschedulablePods(ctx, kubeClient, clk, recorder)
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("failed to get pods from deleting nodes, %w", err)
	}
	pods = append(pods, deletingNodePods...)
	endPodGather()

	var opts []scheduling.Options
	if options.FromContext(ctx).PreferencePolicy == options.PreferencePolicyIgnore {
		opts = append(opts, scheduling.IgnorePreferences)
	}
	opts = append(opts, scheduling.MinValuesPolicy(options.FromContext(ctx).MinValuesPolicy))
	opts = append(opts, schedulerOpts...)
	// Both consolidation candidate pods and pods on already-deleting nodes are migrating off their current nodes, so
	// the DRA allocator should treat the devices they hold as available for reallocation (and re-allocate their claims).
	deletingPodUIDs := sets.New(lo.Map(append(candidatePods, deletingNodePods...), func(p *corev1.Pod, _ int) types.UID { return p.UID })...)
	schedulerStart := time.Now()
	endConstruction := startPassStage(ctx, stageConstruction)
	defer endConstruction()
	scheduler, err := provisioner.NewScheduler(
		log.IntoContext(ctx, operatorlogging.NopLogger),
		pods,
		stateNodes,
		deletingPodUIDs,
		opts...,
	)
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("creating scheduler, %w", err)
	}
	constructionDuration := time.Since(schedulerStart)
	endConstruction()
	if consolidationType := consolidationTypeFromContext(ctx); consolidationType != "" {
		SchedulerConstructionDurationSeconds.Observe(constructionDuration.Seconds(), map[string]string{
			ConsolidationTypeLabel: consolidationType,
		})
	}
	log.FromContext(ctx).V(1).Info("consolidation scheduler constructed", "duration", time.Since(schedulerStart), "candidates", len(candidates))

	deletingNodePodKeys := lo.SliceToMap(deletingNodePods, func(p *corev1.Pod) (client.ObjectKey, any) {
		return client.ObjectKeyFromObject(p), nil
	})

	simulationStart := time.Now()
	endSimulation := startPassStage(ctx, stageSimulation)
	defer endSimulation()
	results, err := scheduler.Solve(log.IntoContext(ctx, operatorlogging.NopLogger), pods)
	endSimulation()
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("scheduling pods, %w", err)
	}
	if consolidationType := consolidationTypeFromContext(ctx); consolidationType != "" {
		ConsolidationSimulationDurationSeconds.Observe(time.Since(simulationStart).Seconds(), map[string]string{
			ConsolidationTypeLabel: consolidationType,
		})
	}
	results = results.TruncateInstanceTypes(ctx, scheduling.MaxInstanceTypes)
	// Consolidation prices its command from this slice and counts it against the replacement bound,
	// so a claim it did not cause distorts the decision. Drift and the other methods replace their
	// candidate whatever the simulation costs, so they keep the unattributed contract.
	if consolidationTypeFromContext(ctx) != "" && options.FromContext(ctx).ConsolidationAttributeReplacements {
		results.NewNodeClaims = replacementsAttributableToDisruption(results.NewNodeClaims, deletingPodUIDs, candidatePodsWithheld(candidates, candidatePods))
	}
	for _, n := range results.ExistingNodes {
		// We consider existing nodes for scheduling. When these nodes are unmanaged, their taint logic should
		// tell us if we can schedule to them or not; however, if these nodes are managed, we will still schedule to them
		// even if they are still in the middle of their initialization loop. In the case of disruption, we don't want
		// to proceed disrupting if our scheduling decision relies on nodes that haven't entered a terminal state.
		if !n.Initialized() {
			for _, p := range n.Pods {
				// Only add a pod scheduling error if it isn't on an already deleting node.
				// If the pod is on a deleting node, we assume one of two things has already happened:
				// 1. The node was manually terminated, at which the provisioning controller has scheduled or is scheduling a node
				//    for the pod.
				// 2. The node was chosen for a previous disruption command, we assume that the uninitialized node will come up
				//    for this command, and we assume it will be successful. If it is not successful, the node will become
				//    not terminating, and we will no longer need to consider these pods.
				if _, ok := deletingNodePodKeys[client.ObjectKeyFromObject(p)]; !ok {
					results.PodErrors[p] = NewUninitializedNodeError(n)
				}
			}
		}
	}
	return results, nil
}

// replacementsAttributableToDisruption keeps the new NodeClaims a disruption is responsible for and
// drops the ones the simulation created for the pending pod backlog.
//
// A consolidation simulation schedules the candidate's pods alongside every pending pod in the
// cluster, because that backlog competes for the same capacity. The solve does not distinguish
// between them in its output: a claim opened for pods the provisioner is already launching capacity
// for arrives in the same NewNodeClaims slice as the candidate's replacement. Consolidation then
// reads that claim as part of its command, which turns a delete into a replacement, counts against
// MaxConsolidationReplacements, and is priced against the candidate.
//
// The distortion is invisible while the backlog is empty, and on gaia production the
// multiple_replacements_required skip rate per evaluated candidate tracks it directly: ~0.000 below
// 100 pending pods, 0.045 at 2,049 and 0.222 at 2,781.
//
// A claim hosting any disrupted pod is kept whole, including one that also absorbs backlog pods:
// the disruption still needs it. Dropping a backlog-only claim does not strand its pods, which stay
// pending for the provisioning loop that owns them.
//
// Pods on already-deleting nodes count as disrupted here even though another decision evicted them.
// They are not backlog: no provisioning loop owns them until they are actually evicted, and a pass
// that admits several commands re-simulates each proposal against the nodes the earlier ones just
// marked for deletion. Excluding them lets a later proposal spend capacity an earlier command in
// the same pass already claimed.
//
// A simulation whose candidates had pods but contributed none of them is left alone rather than
// reduced to a delete. Nothing there is attributable either, but that shape means the pods were
// withheld from the simulation by a PDB that currently allows no disruption, and turning that into
// a delete of a node the simulation never placed pods for is a change this is not making. An empty
// candidate is not that shape: it has nothing to reschedule, so dropping the backlog's claims
// leaves the delete it already was.
func replacementsAttributableToDisruption(newNodeClaims []*scheduling.NodeClaim, disruptedPodUIDs sets.Set[types.UID], podsWithheld bool) []*scheduling.NodeClaim {
	if len(newNodeClaims) == 0 || podsWithheld {
		return newNodeClaims
	}
	return lo.Filter(newNodeClaims, func(nc *scheduling.NodeClaim, _ int) bool {
		return lo.SomeBy(nc.Pods, func(p *corev1.Pod) bool { return disruptedPodUIDs.Has(p.UID) })
	})
}

// candidatePodsWithheld reports whether the candidates hold reschedulable pods that no longer
// reached the simulation, which happens when their PDBs currently allow no disruption.
//
// The disrupted set the filter runs against also holds every pod on an already-deleting node
// cluster-wide, so it is non-empty in a churning cluster whatever the candidate contributed. Asking
// the candidates directly keeps the escape hatch about the candidate, not about the rest of the
// fleet.
func candidatePodsWithheld(candidates []*Candidate, candidatePods []*corev1.Pod) bool {
	return len(candidatePods) == 0 && lo.SomeBy(candidates, func(c *Candidate) bool { return len(c.reschedulablePods) > 0 })
}

// UninitializedNodeError tracks a special pod error for disruption where pods schedule to a node
// that hasn't been initialized yet, meaning that we can't be confident to make a disruption decision based off of it
type UninitializedNodeError struct {
	*scheduling.ExistingNode
}

func NewUninitializedNodeError(node *scheduling.ExistingNode) *UninitializedNodeError {
	return &UninitializedNodeError{ExistingNode: node}
}

func (u *UninitializedNodeError) Error() string {
	var info []string
	if u.NodeClaim != nil {
		info = append(info, fmt.Sprintf("nodeclaim/%s", u.NodeClaim.Name))
	}
	if u.Node != nil {
		info = append(info, fmt.Sprintf("node/%s", u.Node.Name))
	}
	return fmt.Sprintf("would schedule against uninitialized %s", strings.Join(info, ", "))
}

// instanceTypesAreSubset returns true if the lhs slice of instance types are a subset of the rhs.
func instanceTypesAreSubset(lhs []*cloudprovider.InstanceType, rhs []*cloudprovider.InstanceType) bool {
	rhsNames := sets.NewString(lo.Map(rhs, func(t *cloudprovider.InstanceType, i int) string { return t.Name })...)
	lhsNames := sets.NewString(lo.Map(lhs, func(t *cloudprovider.InstanceType, i int) string { return t.Name })...)
	return len(rhsNames.Intersection(lhsNames)) == len(lhsNames)
}

// GetCandidates returns nodes that appear to be currently deprovisionable based off of their nodePool.
func GetCandidates(ctx context.Context, cluster *state.Cluster, kubeClient client.Client, recorder events.Recorder, clk clock.Clock,
	cloudProvider cloudprovider.CloudProvider, shouldDisrupt CandidateFilter, disruptionClass string, queue *Queue,
) ([]*Candidate, error) {
	candidates, _, err := GetCandidatesWithTotals(ctx, cluster, kubeClient, recorder, clk, cloudProvider, shouldDisrupt, disruptionClass, queue, nil)
	return candidates, err
}

// GetCandidatesWithTotals returns candidates and NodePoolTotals computed from all
// candidates before filtering, so balanced scoring normalizes against the full pool.
// When clusterCost is non-nil, TotalCost is read from precomputed cluster state
// rather than re-summed from candidates.
func GetCandidatesWithTotals(ctx context.Context, cluster *state.Cluster, kubeClient client.Client, recorder events.Recorder, clk clock.Clock,
	cloudProvider cloudprovider.CloudProvider, shouldDisrupt CandidateFilter, disruptionClass string, queue *Queue, clusterCost *cost.ClusterCost,
) ([]*Candidate, map[string]NodePoolTotals, error) {
	nodePoolMap, nodePoolToInstanceTypesMap, err := BuildNodePoolMap(ctx, kubeClient, cloudProvider)
	if err != nil {
		return nil, nil, err
	}
	pdbs, err := pdb.NewLimits(ctx, kubeClient)
	if err != nil {
		return nil, nil, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	allNodes := cluster.DeepCopyNodes()
	allCandidates := lo.FilterMap(allNodes, func(n *state.StateNode, _ int) (*Candidate, bool) {
		cn, e := NewCandidate(ctx, kubeClient, recorder, clk, n, pdbs, nodePoolMap, nodePoolToInstanceTypesMap, queue, disruptionClass)
		return cn, e == nil
	})
	// Compute totals using ALL nodes for disruption cost denominator (RFC requirement:
	// "Non-candidate nodes still contribute to the denominators").
	nodePoolTotals := computeNodePoolTotals(ctx, allCandidates, stateNodesToSlice(allNodes), clusterCost)
	filtered := lo.Filter(allCandidates, func(c *Candidate, _ int) bool { return shouldDisrupt(ctx, c) })
	return filtered, nodePoolTotals, nil
}

// stateNodesToSlice converts StateNodes to []*StateNode for computeNodePoolTotals.
func stateNodesToSlice(nodes state.StateNodes) []*state.StateNode {
	return []*state.StateNode(nodes)
}

// BuildNodePoolMap builds a provName -> nodePool map and a provName -> instanceName -> instance type map
func BuildNodePoolMap(ctx context.Context, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) (map[string]*v1.NodePool, map[string]map[string]*cloudprovider.InstanceType, error) {
	nodePoolMap := map[string]*v1.NodePool{}
	nodePools, err := nodepoolutils.ListManaged(ctx, kubeClient, cloudProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("listing node pools, %w", err)
	}

	nodePoolToInstanceTypesMap := map[string]map[string]*cloudprovider.InstanceType{}
	for _, np := range nodePools {
		nodePoolMap[np.Name] = np

		nodePoolInstanceTypes, err := cloudProvider.GetInstanceTypes(ctx, np)
		if err != nil {
			if cloudprovider.IsUnevaluatedNodePoolError(err) {
				log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Error(err, "skipping, node overlays are not applied")
				continue
			}
			// don't error out on building the node pool, we just won't be able to handle any nodes that
			// were created by it
			log.FromContext(ctx).Error(err, "failed listing instance types", "nodepool", np.Name)
			continue
		}
		if len(nodePoolInstanceTypes) == 0 {
			continue
		}
		nodePoolToInstanceTypesMap[np.Name] = map[string]*cloudprovider.InstanceType{}
		for _, it := range nodePoolInstanceTypes {
			nodePoolToInstanceTypesMap[np.Name][it.Name] = it
		}
	}
	return nodePoolMap, nodePoolToInstanceTypesMap, nil
}

// BuildDisruptionBudgetMapping prepares our disruption budget mapping. The disruption budget maps each disruption reason to the number of allowed disruptions.
// We calculate allowed disruptions by taking the max disruptions allowed by disruption reason and subtracting the number of nodes that are NotReady and already being deleted by that disruption reason.
//
// With pipelined disruption budgets, a candidate whose command is still waiting on its replacements to initialize
// only holds a slot in this mapping, which gates the start of new commands. The budget for actually terminating
// nodes is checked separately by the disruption queue (see BuildTerminationBudgetMapping) right before it deletes a
// command's candidates, so the NodePool's budget bounds how many nodes are draining at once rather than how many
// commands are in flight.
//
//nolint:gocyclo
func BuildDisruptionBudgetMapping(ctx context.Context, cluster *state.Cluster, clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder, reason v1.DisruptionReason) (map[string]int, error) {
	counts := countBudgetConsumers(cluster)
	disruptionBudgetMapping := map[string]int{}
	nodePools, err := nodepoolutils.ListManaged(ctx, kubeClient, cloudProvider)
	if err != nil {
		return disruptionBudgetMapping, fmt.Errorf("listing node pools, %w", err)
	}
	pipelined := options.FromContext(ctx).PipelinedDisruptionBudgets
	for _, nodePool := range nodePools {
		c := counts[nodePool.Name]
		allowedDisruptions := nodePool.MustGetAllowedDisruptions(clk, c.total, reason)
		// Without pipelining, every node that is marked, draining, or unhealthy consumes the single budget. With it,
		// only the nodes still waiting on a replacement hold a slot here; draining nodes are accounted for at the
		// termination gate instead.
		consuming := c.pending + c.terminating
		if pipelined {
			consuming = c.pending
		}
		disruptionBudgetMapping[nodePool.Name] = lo.Max([]int{allowedDisruptions - consuming, 0})
		NodePoolAllowedDisruptions.Set(float64(allowedDisruptions), map[string]string{
			metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: string(reason),
		})
		NodePoolNodesConsumingBudgets.Set(float64(consuming), map[string]string{
			metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: string(reason),
		})
		NodePoolNodesPendingReplacement.Set(float64(c.pending), map[string]string{
			metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: string(reason),
		})
		NodePoolNodesTerminating.Set(float64(c.terminating), map[string]string{
			metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: string(reason),
		})
		if c.total != 0 && allowedDisruptions == 0 {
			recorder.Publish(disruptionevents.NodePoolBlockedForDisruptionReason(nodePool, reason))
		}
	}
	return disruptionBudgetMapping, nil
}

// BuildTerminationBudgetMapping returns, per NodePool, how many more nodes may start draining right now for the
// given reason: the NodePool's allowed disruptions less the nodes that are already terminating or NotReady. It is
// the second stage of pipelined disruption budgets and is consulted by the disruption queue before it deletes a
// command's candidates.
func BuildTerminationBudgetMapping(ctx context.Context, cluster *state.Cluster, clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, reason v1.DisruptionReason) (map[string]int, error) {
	counts := countBudgetConsumers(cluster)
	terminationBudgetMapping := map[string]int{}
	nodePools, err := nodepoolutils.ListManaged(ctx, kubeClient, cloudProvider)
	if err != nil {
		return terminationBudgetMapping, fmt.Errorf("listing node pools, %w", err)
	}
	for _, nodePool := range nodePools {
		c := counts[nodePool.Name]
		allowedDisruptions := nodePool.MustGetAllowedDisruptions(clk, c.total, reason)
		terminationBudgetMapping[nodePool.Name] = lo.Max([]int{allowedDisruptions - c.terminating, 0})
	}
	return terminationBudgetMapping, nil
}

// budgetConsumers splits a NodePool's initialized nodes by how they relate to its disruption budget.
type budgetConsumers struct {
	// total is the number of initialized, non-terminated nodes the budget percentage is computed from.
	total int
	// pending is the number of nodes a disruption command has claimed but not yet deleted: their replacements are
	// still booting, the node is tainted against new pods, and every pod on it is still running.
	pending int
	// terminating is the number of nodes that are actually being drained or are NotReady, i.e. whose pods are (or
	// may be) unavailable.
	terminating int
}

// countBudgetConsumers tallies budgetConsumers per NodePool name. NodePools with no initialized nodes are absent from
// the map; the zero value is the correct count for them.
func countBudgetConsumers(cluster *state.Cluster) map[string]budgetConsumers {
	counts := map[string]budgetConsumers{}
	for _, node := range cluster.DeepCopyNodes() {
		// We only consider nodes that we own and are initialized towards the total.
		// If a node is launched/registered, but not initialized, pods aren't scheduled
		// to the node, and these are treated as unhealthy until they're cleaned up.
		// This prevents odd roundup cases with percentages where replacement nodes that
		// aren't initialized could be counted towards the total, resulting in more disruptions
		// to active nodes than desired, where Karpenter should wait for these nodes to be
		// healthy before continuing.
		if !node.Managed() || !node.Initialized() {
			continue
		}
		// Additionally, don't consider nodeclaims that have the terminating condition. A nodeclaim should have
		// the Terminating condition only when the node is drained and cloudprovider.Delete() was successful
		// on the underlying cloud provider machine.
		if node.NodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			continue
		}
		nodePool := node.Labels()[v1.NodePoolLabelKey]
		c := counts[nodePool]
		c.total++
		// A node is terminating when it is NotReady or its NodeClaim is being deleted (drain in progress). A node the
		// disruption queue has only marked for deletion is pending: nothing on it has been disrupted yet.
		switch {
		case nodeutils.GetCondition(node.Node, corev1.NodeReady).Status != corev1.ConditionTrue || node.Deleted():
			c.terminating++
		case node.MarkedForDeletion():
			c.pending++
		}
		counts[nodePool] = c
	}
	return counts
}

// mapCandidates maps the list of proposed candidates with the current state
func mapCandidates(proposed, current []*Candidate) []*Candidate {
	proposedNames := sets.NewString(lo.Map(proposed, func(c *Candidate, i int) string { return c.Name() })...)
	return lo.Filter(current, func(c *Candidate, _ int) bool {
		return proposedNames.Has(c.Name())
	})
}
