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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// A pass that skips an unlaunched NodeClaim must not launch capacity for the pods that NodeClaim is
// already launching for, whether they are pending or on a deleting node. A replacement that bundled
// them with a candidate's pods would launch them twice, and the command's savings, priced against
// the candidate alone, would never show it.
var _ = Describe("Disruption Sync Policy: capacity in flight", func() {
	var nodePool *v1.NodePool
	var candidateClaim, interruptedClaim *v1.NodeClaim
	var candidateNode, interruptedNode *corev1.Node
	var candidatePod *corev1.Pod

	withPolicy := func(policy options.DisruptionSyncPolicy) {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{DisruptionSyncPolicy: lo.ToPtr(policy)}))
	}
	rsPod := func(cpu string) *corev1.Pod {
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		return test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
			}}},
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
		})
	}
	// displacedOnInterruptedNode binds a 10-CPU pod to a node that a spot interruption marked for
	// deletion. Its pod is the provisioner's to place.
	displacedOnInterruptedNode := func() *corev1.Pod {
		displaced := rsPod("10")
		ExpectApplied(ctx, env.Client, nodePool, candidateClaim, candidateNode, interruptedClaim, interruptedNode, candidatePod, displaced)
		ExpectManualBinding(ctx, env.Client, candidatePod, candidateNode)
		ExpectManualBinding(ctx, env.Client, displaced, interruptedNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController,
			[]*corev1.Node{candidateNode, interruptedNode}, []*v1.NodeClaim{candidateClaim, interruptedClaim})
		cluster.MarkForDeletion(interruptedClaim.Status.ProviderID)
		Expect(cluster.Synced(ctx)).To(BeTrue())
		return displaced
	}
	// pendingBacklog applies a pending 10-CPU pod.
	pendingBacklog := func() *corev1.Pod {
		pending := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10")}},
		})
		ExpectApplied(ctx, env.Client, nodePool, candidateClaim, candidateNode, candidatePod, pending)
		ExpectManualBinding(ctx, env.Client, candidatePod, candidateNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController,
			[]*corev1.Node{candidateNode}, []*v1.NodeClaim{candidateClaim})
		Expect(cluster.Synced(ctx)).To(BeTrue())
		return pending
	}
	// inFlight creates the provisioner's NodeClaim for pods, not yet launched and without the
	// replacement-origin annotation. With record, the pods are recorded against it as the
	// provisioner records them when it creates a NodeClaim; without, as after a restart.
	inFlight := func(mutate func(*v1.NodeClaim), record bool, pods ...*corev1.Pod) *v1.NodeClaim {
		nc := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
		nc.Status.ProviderID = ""
		if mutate != nil {
			mutate(nc)
		}
		ExpectApplied(ctx, env.Client, nc)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
		if record {
			cluster.UpdatePodToNodeClaimMapping(map[string][]*corev1.Pod{nc.Name: pods})
		}
		return nc
	}
	deferred := func(nc *v1.NodeClaim) {
		nc.StatusConditions().SetUnknownWithReason(v1.ConditionTypeLaunched, "CapacityEvidencePending", "the cloud provider deferred the launch")
	}
	failed := func(nc *v1.NodeClaim) {
		nc.StatusConditions().SetUnknownWithReason(v1.ConditionTypeLaunched, "LaunchFailed", "creating instance, throttled")
	}
	// replacementPods names every pod the queued commands' replacements were sized for.
	replacementPods := func() []string {
		var names []string
		for _, cmd := range queue.GetCommands() {
			for _, r := range cmd.Replacements {
				for _, p := range r.Pods {
					names = append(names, p.Name)
				}
			}
		}
		return names
	}
	checks := func(reason, outcome string) map[string]string {
		return map[string]string{"reason": reason, "outcome": outcome}
	}

	BeforeEach(func() {
		disruption.UnlaunchedNodeClaimSyncChecksTotal.Reset()
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			Budgets:             []v1.Budget{{Nodes: "100%"}},
		}}})
		candidateClaim, candidateNode = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
				v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			}},
			Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")}},
		})
		candidateClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		interruptedClaim, interruptedNode = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
				v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			}},
			Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourcePods: resource.MustParse("100")}},
		})
		candidatePod = rsPod("1")
	})

	DescribeTable("replaces the candidate without the pods a skipped NodeClaim is launching for",
		func(setup func() *corev1.Pod, policy options.DisruptionSyncPolicy, mutate func(*v1.NodeClaim), class string) {
			withPolicy(policy)
			covered := setup()
			inFlight(mutate, true, covered)

			ExpectSingletonReconciled(ctx, disruptionController)
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks(class, "proceeded"))
			Expect(queue.GetCommands()).To(HaveLen(1))
			Expect(queue.GetCommands()[0].Decision()).To(Equal(disruption.ReplaceDecision))
			Expect(replacementPods()).To(ConsistOf(candidatePod.Name))
			ExpectMetricGaugeValue(disruption.SimulationPendingPods, 1, map[string]string{"disposition": "excluded_capacity_in_flight"})
			// The pending pod left out is not also counted as simulated.
			ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, map[string]string{"disposition": "simulated"})
		},
		Entry("an interrupted node's pod, IgnoreNonReplacements", displacedOnInterruptedNode, options.DisruptionSyncPolicyIgnoreNonReplacements, nil, "in_flight"),
		Entry("an interrupted node's pod, deferred launch, IgnoreFailedOrDeferred", displacedOnInterruptedNode, options.DisruptionSyncPolicyIgnoreFailedOrDeferred, deferred, "launch_deferred"),
		Entry("an interrupted node's pod, failed launch, IgnoreFailedOrDeferred", displacedOnInterruptedNode, options.DisruptionSyncPolicyIgnoreFailedOrDeferred, failed, "launch_failed"),
		Entry("a pending pod, IgnoreNonReplacements", pendingBacklog, options.DisruptionSyncPolicyIgnoreNonReplacements, nil, "in_flight"),
		Entry("a pending pod, deferred launch, IgnoreFailedOrDeferred", pendingBacklog, options.DisruptionSyncPolicyIgnoreFailedOrDeferred, deferred, "launch_deferred"),
		Entry("a pending pod, failed launch, IgnoreFailedOrDeferred", pendingBacklog, options.DisruptionSyncPolicyIgnoreFailedOrDeferred, failed, "launch_failed"),
	)

	DescribeTable("waits for a NodeClaim whose pods are not recorded",
		func(setup func() *corev1.Pod, policy options.DisruptionSyncPolicy, mutate func(*v1.NodeClaim), class string) {
			withPolicy(policy)
			inFlight(mutate, false, setup())

			ExpectSingletonReconciled(ctx, disruptionController)
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks(class, "waited"))
			Expect(queue.GetCommands()).To(BeEmpty())
		},
		Entry("an interrupted node's pod, Strict", displacedOnInterruptedNode, options.DisruptionSyncPolicyStrict, nil, "pods_unrecorded"),
		Entry("an interrupted node's pod, IgnoreNonReplacements", displacedOnInterruptedNode, options.DisruptionSyncPolicyIgnoreNonReplacements, nil, "pods_unrecorded"),
		Entry("an interrupted node's pod, deferred launch, IgnoreFailedOrDeferred", displacedOnInterruptedNode, options.DisruptionSyncPolicyIgnoreFailedOrDeferred, deferred, "pods_unrecorded"),
		Entry("a pending pod, Strict", pendingBacklog, options.DisruptionSyncPolicyStrict, nil, "pods_unrecorded"),
		Entry("a pending pod, IgnoreNonReplacements", pendingBacklog, options.DisruptionSyncPolicyIgnoreNonReplacements, nil, "pods_unrecorded"),
		Entry("a pending pod, deferred launch, IgnoreFailedOrDeferred", pendingBacklog, options.DisruptionSyncPolicyIgnoreFailedOrDeferred, deferred, "pods_unrecorded"),
	)

	It("waits for a recorded NodeClaim under Strict", func() {
		withPolicy(options.DisruptionSyncPolicyStrict)
		inFlight(nil, true, pendingBacklog())
		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("in_flight", "waited"))
		Expect(queue.GetCommands()).To(BeEmpty())
	})

	It("simulates the pods again once the skipped NodeClaim is deleting", func() {
		withPolicy(options.DisruptionSyncPolicyIgnoreNonReplacements)
		pending := pendingBacklog()
		nc := inFlight(nil, true, pending)
		// An insufficient-capacity launch deletes the NodeClaim; a finalizer holds it in cluster state.
		live := ExpectExists(ctx, env.Client, nc)
		live.Finalizers = append(live.Finalizers, v1.TerminationFinalizer)
		ExpectApplied(ctx, env.Client, live)
		Expect(env.Client.Delete(ctx, live)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(live))

		// The pod has no capacity coming, which is what a strict pass sees once the NodeClaim is gone.
		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("deleting", "proceeded"))
		Expect(replacementPods()).To(ContainElement(pending.Name))
		ExpectMetricGaugeValue(disruption.SimulationPendingPods, 0, map[string]string{"disposition": "excluded_capacity_in_flight"})
	})

	It("still simulates a candidate's pod recorded against a skipped NodeClaim", func() {
		// The provisioner planned the pod onto the NodeClaim while it was pending; the kube-scheduler
		// then bound it to the candidate. Its capacity is the candidate's now, so it moves with it.
		withPolicy(options.DisruptionSyncPolicyIgnoreNonReplacements)
		pendingBacklog()
		inFlight(nil, true, candidatePod)

		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("in_flight", "proceeded"))
		Expect(replacementPods()).To(ContainElement(candidatePod.Name))
	})
})
