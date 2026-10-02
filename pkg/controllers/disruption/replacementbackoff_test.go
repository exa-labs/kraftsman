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
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Replacement failure back-off", func() {
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var singleNode *disruption.SingleNodeConsolidation

	// skipsFor reads how many times single-node consolidation skipped the given node on a back-off.
	skipsFor := func() float64 {
		GinkgoHelper()
		metric, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_consolidation_candidate_skips_total", map[string]string{
			"consolidation_type": disruption.SingleNodeConsolidationType,
			"nodepool":           nodePool.Name,
			"reason":             disruption.CandidateSkipReplacementBackoff,
		})
		if !found {
			return 0
		}
		return metric.GetCounter().GetValue()
	}

	candidates := func() []*disruption.Candidate {
		GinkgoHelper()
		cs, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, singleNode.ShouldDisrupt, singleNode.Class(), queue)
		Expect(err).To(Succeed())
		return cs
	}

	// failReplacementOf queues a single-node replace of the node's candidate and fails it the way an
	// insufficient-capacity launch does: the replacement NodeClaim is deleted before it initializes.
	failReplacementOf := func(providerID string) {
		GinkgoHelper()
		candidate, ok := lo.Find(candidates(), func(c *disruption.Candidate) bool { return c.ProviderID() == providerID })
		Expect(ok).To(BeTrue())
		nct := scheduling.NewNodeClaimTemplate(nodePool)
		nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
		cmd := &disruption.Command{
			Method:            singleNode,
			CreationTimestamp: env.Clock.Now(),
			ID:                uuid.New(),
			Candidates:        []*disruption.Candidate{candidate},
			Replacements:      []*disruption.Replacement{{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}}},
		}
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		replacement := &v1.NodeClaim{}
		Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd.Replacements[0].Name}, replacement)).To(Succeed())
		ExpectDeleted(ctx, env.Client, replacement)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(replacement))
		ExpectReconcileSucceeded(ctx, queue, client.ObjectKeyFromObject(candidate.NodeClaim))
		Expect(queue.HasAny(providerID)).To(BeFalse())
		Expect(cmd.Succeeded).To(BeFalse())
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{ConsolidationReplacementFailureBackoff: lo.ToPtr(5 * time.Minute)}))
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			Budgets:             []v1.Budget{{Nodes: "100%"}},
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		}}})
		// One loaded node: its pod fits nowhere else, so the only command for it is a replace.
		nodeClaims, nodes = test.NodeClaimsAndNodes(1, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
				v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			}},
			Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")}},
		})
		nodeClaims[0].StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, nodePool, rs)
		pod := test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
			}}},
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
		})
		ExpectApplied(ctx, env.Client, nodeClaims[0], nodes[0], pod)
		ExpectManualBinding(ctx, env.Client, pod, nodes[0])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		singleNode = disruption.NewSingleNodeConsolidation(
			disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(&scriptedValidator{}),
		)
	})
	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
	})

	It("holds a candidate off after its replacement failed to launch, then proposes it again", func() {
		Expect(candidates()).To(HaveLen(1))
		failReplacementOf(nodeClaims[0].Status.ProviderID)
		skipped := skipsFor()

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		Expect(cmds).To(BeEmpty())
		Expect(skipsFor()).To(Equal(skipped + 1))
		// The held candidate was never evaluated, so the pass must not mark the fleet consolidated.
		Expect(singleNode.IsConsolidated()).To(BeFalse())

		env.Clock.Step(5*time.Minute + time.Second)
		cmds, err = singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		Expect(skipsFor()).To(Equal(skipped + 1))
	})

	It("doubles the hold when the candidate fails again", func() {
		failReplacementOf(nodeClaims[0].Status.ProviderID)
		env.Clock.Step(5*time.Minute + time.Second)
		failReplacementOf(nodeClaims[0].Status.ProviderID)

		// Five minutes would have released the first hold; the second lasts ten.
		env.Clock.Step(5*time.Minute + time.Second)
		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		Expect(cmds).To(BeEmpty())

		env.Clock.Step(5 * time.Minute)
		cmds, err = singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
	})

	It("still deletes a held candidate once its pod fits elsewhere", func() {
		failReplacementOf(nodeClaims[0].Status.ProviderID)
		skipped := skipsFor()

		// Room appears on another node while the hold is active. A delete launches nothing, so the
		// hold does not apply to it.
		spareClaim, spareNode := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
				v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			}},
			Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")}},
		})
		ExpectApplied(ctx, env.Client, spareClaim, spareNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{spareNode}, []*v1.NodeClaim{spareClaim})

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
		Expect(cmds[0].Candidates[0].ProviderID()).To(Equal(nodeClaims[0].Status.ProviderID))
		Expect(skipsFor()).To(Equal(skipped))
	})

	It("proposes the candidate again on the next pass when the back-off is off", func() {
		ctx = options.ToContext(ctx, test.Options())
		failReplacementOf(nodeClaims[0].Status.ProviderID)
		skipped := skipsFor()

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		Expect(skipsFor()).To(Equal(skipped))
	})
})
