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

package disruption_test

import (
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// These tests cover the exclusion of pending pods every NodePool is incompatible with from
// disruption scheduling simulations (unprovisionablepods.go). The first fixture is the two-node,
// three-pod layout of "can delete nodes with a permanently pending pod": nodes[1] hosts one pod that
// fits on nodes[0], so single-node consolidation deletes nodes[1] regardless of the pending backlog.
// What varies is the backlog: a pod pinned to a capacity type no NodePool offers, which every
// provisioning pass records an incompatibility verdict for, or a pod any NodePool can launch for.
// The second fixture is a NodePool at its node limit, where the provisioner's rejection of a pending
// pod is lifted by the very node removal a disruption simulation performs.
var _ = Describe("Unprovisionable Pending Pods", func() {
	const ttl = 2 * time.Minute
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var rs *appsv1.ReplicaSet
	var labels = map[string]string{"app": "test"}
	var excluded = map[string]string{"disposition": "excluded_unprovisionable"}
	var simulated = map[string]string{"disposition": "simulated"}

	BeforeEach(func() {
		disruption.SimulationPendingPods.Reset()
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{DisruptionUnprovisionablePodTTL: lo.ToPtr(ttl)}))
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})
		nodeClaims, nodes = test.NodeClaimsAndNodes(2, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for _, nc := range nodeClaims {
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		rs = test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())

		pods := test.Pods(3, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         "apps/v1",
					Kind:               "ReplicaSet",
					Name:               rs.Name,
					UID:                rs.UID,
					Controller:         lo.ToPtr(true),
					BlockOwnerDeletion: lo.ToPtr(true),
				}},
			},
		})
		ExpectApplied(ctx, env.Client, pods[0], pods[1], pods[2], nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[1])
	})

	// reservedOnlyPod is pinned to a capacity type the fake cloud provider never offers, so the
	// provisioner can place it nowhere: the shape of a reserved-only pod once every reservation is
	// exhausted.
	reservedOnlyPod := func() *corev1.Pod {
		return test.UnschedulablePod(test.PodOptions{
			NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved},
		})
	}

	// consolidate syncs cluster state and runs one disruption pass, returning the commands it queued.
	consolidate := func() []*disruption.Command {
		GinkgoHelper()
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, disruptionController)
		return queue.GetCommands()
	}

	It("should exclude a pending pod the provisioner could not place and still consolidate", func() {
		pending := reservedOnlyPod()
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeFalse())
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))

		cmds := consolidate()
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, excluded)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, simulated)

		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Candidates).ToNot(BeEmpty())
		Expect(cmds[0].Reason()).To(Equal(v1.DisruptionReasonUnderutilized))
	})
	It("should exclude the pod from drift simulations and still replace the drifted node", func() {
		nodeClaims[1].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		ExpectApplied(ctx, env.Client, nodeClaims[1])
		pending := reservedOnlyPod()
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeFalse())

		cmds := consolidate()
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, excluded)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, simulated)

		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Reason()).To(Equal(v1.DisruptionReasonDrifted))
		Expect(cmds[0].Candidates).To(HaveLen(1))
		Expect(cmds[0].Candidates[0].Name()).To(Equal(nodes[1].Name))
	})
	It("should simulate the pod again once its verdict is older than the TTL", func() {
		pending := reservedOnlyPod()
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		env.Clock.Step(ttl)

		cmds := consolidate()
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, excluded)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, simulated)
		Expect(cmds).To(HaveLen(1))
	})
	It("should simulate every pending pod when the TTL is zero", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{DisruptionUnprovisionablePodTTL: lo.ToPtr(time.Duration(0))}))
		pending := reservedOnlyPod()
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeFalse())

		cmds := consolidate()
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, excluded)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, simulated)
		Expect(cmds).To(HaveLen(1))
	})
	It("should simulate a pending pod the provisioner has not decided on", func() {
		pending := reservedOnlyPod()
		ExpectApplied(ctx, env.Client, pending)

		consolidate()
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, excluded)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, simulated)
	})
	It("should simulate a pending pod the provisioner is launching capacity for", func() {
		pending := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
		})
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeTrue())

		consolidate()
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, excluded)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, simulated)
	})
	It("should keep retrying an excluded pod in provisioning and simulate it again once it is placeable", func() {
		pending := test.UnschedulablePod(test.PodOptions{
			NodeSelector: map[string]string{"example.com/pool": "reserved"},
		})
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeFalse())
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))

		// A NodePool that can launch for the pod appears. The next provisioning pass, which reads the
		// backlog directly, opens a NodeClaim for it and clears the verdict.
		reservedPool := test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Template: v1.NodeClaimTemplate{
					Spec: v1.NodeClaimTemplateSpec{
						Requirements: []v1.NodeSelectorRequirementWithMinValues{{
							Key: "example.com/pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"reserved"},
						}},
					},
				},
			},
		})
		ExpectApplied(ctx, env.Client, reservedPool)
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeTrue())
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(3))

		consolidate()
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, excluded)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, simulated)
	})
})

// A pending pod the provisioner rejected only because its NodePool was at its node limit is placeable
// inside a disruption simulation: SimulateScheduling removes the candidate from the cluster state, the
// NodePool regains one node of headroom and the scheduler opens a replacement that must be sized for
// the pending pod as well as the candidate's pods. Such a rejection records no verdict, so the pod is
// simulated and the replacement is the same one a simulation with the exclusion disabled produces.
// A pod the capped NodePool is incompatible with regardless of limits keeps its verdict and stays out.
// SimulateScheduling is the path every disruption method (single-node and multi-node consolidation,
// drift, and command validation) schedules through, so the check covers all of them.
var _ = Describe("Unprovisionable Pending Pods with NodePool limits", func() {
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node

	BeforeEach(func() {
		disruption.SimulationPendingPods.Reset()
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Limits: v1.Limits(corev1.ResourceList{resources.Node: resource.MustParse("2")}),
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})
		nodeClaims, nodes = test.NodeClaimsAndNodes(2, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for _, nc := range nodeClaims {
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		// nodes[0] is full; nodes[1], the candidate, hosts one small pod that cannot move to nodes[0].
		full := test.Pods(2, test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16")}}})
		candidatePod := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}})
		ExpectApplied(ctx, env.Client, full[0], full[1], candidatePod, nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)
		ExpectManualBinding(ctx, env.Client, full[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, full[1], nodes[0])
		ExpectManualBinding(ctx, env.Client, candidatePod, nodes[1])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
	})

	// simulateRemoving runs the disruption scheduling simulation for the removal of the given nodes under the given TTL.
	simulateRemoving := func(ttl time.Duration, removed ...*corev1.Node) scheduling.Results {
		GinkgoHelper()
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{DisruptionUnprovisionablePodTTL: lo.ToPtr(ttl)}))
		nodePoolMap, nodePoolToInstanceTypesMap, err := disruption.BuildNodePoolMap(ctx, env.Client, cloudProvider)
		Expect(err).To(Succeed())
		pdbs, err := pdb.NewLimits(ctx, env.Client)
		Expect(err).To(Succeed())
		candidates := lo.Map(removed, func(n *corev1.Node, _ int) *disruption.Candidate {
			candidate, err := disruption.NewCandidate(ctx, env.Client, recorder, env.Clock, ExpectStateNodeExists(cluster, n), pdbs, nodePoolMap, nodePoolToInstanceTypesMap, queue, disruption.GracefulDisruptionClass)
			Expect(err).To(Succeed())
			return candidate
		})
		results, err := disruption.SimulateScheduling(ctx, env.Client, cluster, prov, env.Clock, recorder, nil, candidates...)
		Expect(err).To(Succeed())
		return results
	}
	// simulate runs the disruption scheduling simulation for the removal of nodes[1], the candidate, under the given TTL.
	simulate := func(ttl time.Duration) scheduling.Results {
		GinkgoHelper()
		return simulateRemoving(ttl, nodes[1])
	}
	podNames := func(pods []*corev1.Pod) []string {
		return lo.Map(pods, func(p *corev1.Pod, _ int) string { return p.Name })
	}
	// replacementPodSets returns the sorted pod names of each new NodeClaim, ordered by NodeClaim size.
	replacementPodSets := func(results scheduling.Results) [][]string {
		sets := lo.Map(results.NewNodeClaims, func(nc *scheduling.NodeClaim, _ int) []string {
			names := podNames(nc.Pods)
			slices.Sort(names)
			return names
		})
		slices.SortFunc(sets, func(a, b []string) int { return len(a) - len(b) })
		return sets
	}

	It("should simulate a pod the provisioner rejected on NodePool limits and size the replacement for it", func() {
		// Too big for the 31 free CPUs on the candidate, fits a fresh node, but the NodePool is at its two-node
		// limit while both nodes exist, so the provisioner rejects it.
		pending := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("40")}},
		})
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeTrue())

		baseline := simulate(0)
		Expect(baseline.PodErrors).NotTo(HaveKey(pending))
		Expect(baseline.NewNodeClaims).To(HaveLen(1))
		Expect(podNames(baseline.NewNodeClaims[0].Pods)).To(ContainElement(pending.Name))

		filtered := simulate(2 * time.Minute)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, map[string]string{"disposition": "excluded_unprovisionable"})
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, map[string]string{"disposition": "simulated"})
		Expect(filtered.PodErrors).NotTo(HaveKey(pending))
		Expect(filtered.NewNodeClaims).To(HaveLen(1))
		Expect(podNames(filtered.NewNodeClaims[0].Pods)).To(ConsistOf(podNames(baseline.NewNodeClaims[0].Pods)))
		Expect(len(filtered.NewNodeClaims[0].InstanceTypeOptions)).To(Equal(len(baseline.NewNodeClaims[0].InstanceTypeOptions)))
	})
	It("should simulate a pod the provisioner rejected on NodePool limits when several nodes are removed at once", func() {
		// Removing both nodes frees the whole two-node limit: the full node's pods, the candidate's pod and the pending
		// pod all need replacements, as in a multi-node consolidation simulation.
		pending := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("40")}},
		})
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeTrue())

		baseline := simulateRemoving(0, nodes[0], nodes[1])
		Expect(baseline.PodErrors).NotTo(HaveKey(pending))
		Expect(lo.Flatten(replacementPodSets(baseline))).To(ContainElement(pending.Name))

		filtered := simulateRemoving(2*time.Minute, nodes[0], nodes[1])
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, map[string]string{"disposition": "excluded_unprovisionable"})
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, map[string]string{"disposition": "simulated"})
		Expect(filtered.PodErrors).NotTo(HaveKey(pending))
		Expect(replacementPodSets(filtered)).To(Equal(replacementPodSets(baseline)))
	})
	It("should still exclude a pod the NodePool at its limit is incompatible with", func() {
		// Pinned to a capacity type no instance type offers: the limit is not what keeps it pending, and the
		// headroom the candidate's removal frees changes nothing for it.
		pending := test.UnschedulablePod(test.PodOptions{
			NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved},
		})
		ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending)
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))
		Expect(cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pending)).IsZero()).To(BeFalse())

		baseline := simulate(0)
		Expect(baseline.PodErrors).To(HaveKey(pending))
		Expect(baseline.NewNodeClaims).To(HaveLen(1))

		filtered := simulate(2 * time.Minute)
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, map[string]string{"disposition": "excluded_unprovisionable"})
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, map[string]string{"disposition": "simulated"})
		Expect(filtered.PodErrors).To(BeEmpty())
		Expect(filtered.NewNodeClaims).To(HaveLen(1))
		Expect(podNames(filtered.NewNodeClaims[0].Pods)).To(ConsistOf(podNames(baseline.NewNodeClaims[0].Pods)))
	})
})
