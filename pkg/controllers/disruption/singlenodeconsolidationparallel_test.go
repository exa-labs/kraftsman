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
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// These specs re-run the batched single-node consolidation scenarios with discovery workers
// enabled: candidate simulations run concurrently, and the pass's proposals, admission behavior,
// and safety properties must be indistinguishable from the serial walk.
var _ = Describe("Parallel Discovery Single-Node Consolidation", func() {
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var rs *appsv1.ReplicaSet
	var validator *scriptedValidator
	var singleNode *disruption.SingleNodeConsolidation
	labels := map[string]string{"app": "parallel-consolidation"}

	newSingleNodeConsolidation := func(v disruption.Validator) *disruption.SingleNodeConsolidation {
		return disruption.NewSingleNodeConsolidation(
			disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(v),
		)
	}

	podOn := func(cpu string) *corev1.Pod {
		return test.Pod(test.PodOptions{
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
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			},
		})
	}

	applyNodes := func(count int, cpu string) {
		for i := range count {
			pod := podOn(cpu)
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i], pod)
			ExpectManualBinding(ctx, env.Client, pod, nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes[:count], nodeClaims[:count])
	}

	candidatesFor := func(m *disruption.SingleNodeConsolidation) []*disruption.Candidate {
		GinkgoHelper()
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, m.ShouldDisrupt, m.Class(), queue)
		Expect(err).To(Succeed())
		return candidates
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			MaxConsolidationCommandsPerPass: lo.ToPtr(3),
			ConsolidationDiscoveryWorkers:   lo.ToPtr(4),
		}))
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})
		nodeClaims, nodes = test.NodeClaimsAndNodes(5, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            leastExpensiveInstance.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
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
			nc.Labels[v1.NodePoolLabelKey] = nodePool.Name
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		for _, n := range nodes {
			n.Labels[v1.NodePoolLabelKey] = nodePool.Name
		}
		rs = test.ReplicaSet()
		ExpectApplied(ctx, env.Client, nodePool, rs)

		validator = &scriptedValidator{}
		singleNode = newSingleNodeConsolidation(validator)
	})

	AfterEach(func() {
		disruption.SingleNodeConsolidationTimeoutDuration = 3 * time.Minute
		ExpectCleanedUp(ctx, env.Client)
	})

	It("admits several non-overlapping commands from one pass", func() {
		applyNodes(4, "1")
		candidates := candidatesFor(singleNode)
		Expect(candidates).To(HaveLen(4))

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(3))
		Expect(queue.GetCommands()).To(HaveLen(3))
		claimed := sets.New[string]()
		for _, cmd := range cmds {
			Expect(cmd.Admitted).To(BeTrue())
			for _, c := range cmd.Candidates {
				Expect(claimed.Has(c.ProviderID())).To(BeFalse())
				claimed.Insert(c.ProviderID())
			}
		}
		Expect(validator.periods).To(Equal([]time.Duration{15 * time.Second, 0, 0}))
	})

	It("keeps the one-command-per-pass behavior when the cap is 1", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			MaxConsolidationCommandsPerPass: lo.ToPtr(1),
			ConsolidationDiscoveryWorkers:   lo.ToPtr(4),
		}))
		applyNodes(4, "1")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Admitted).To(BeFalse())
		Expect(queue.GetCommands()).To(BeEmpty())
	})

	It("does not admit more commands than the NodePool's remaining budget", func() {
		applyNodes(4, "1")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 2}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(2))
		Expect(queue.GetCommands()).To(HaveLen(2))
	})

	It("admits the remaining commands when one proposal is rejected at admission", func() {
		applyNodes(4, "1")
		validator.errs = []error{disruption.NewSchedulingValidationError(errors.New("stale plan"))}

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(2))
		Expect(queue.GetCommands()).To(HaveLen(2))
	})

	It("admits every proposal it holds when the walk times out", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			MaxConsolidationCommandsPerPass: lo.ToPtr(5),
			ConsolidationDiscoveryWorkers:   lo.ToPtr(4),
		}))
		applyNodes(5, "1")
		disruption.SingleNodeConsolidationTimeoutDuration = 3 * time.Second
		method := disruption.NewSingleNodeConsolidation(
			disruption.MakeConsolidation(&steppingClock{FakeClock: env.Clock, step: time.Second}, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(validator),
		)

		cmds, err := method.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(method)...)
		Expect(err).To(Succeed())
		Expect(len(cmds)).To(BeNumerically(">", 1))
		Expect(queue.GetCommands()).To(HaveLen(len(cmds)))
	})

	It("rejects a proposal whose headroom an earlier command in the same pass consumed", func() {
		// Speculative simulations run against pre-admission state; validation against live state
		// is what rejects the second, now-stale proposal.
		applyNodes(2, "20")
		spareClaim, spareNode := nodeClaims[2], nodes[2]
		spareClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		ExpectApplied(ctx, env.Client, spareClaim, spareNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{spareNode}, []*v1.NodeClaim{spareClaim})

		real := newSingleNodeConsolidation(immediateValidator{
			inner: disruption.NewSingleConsolidationValidator(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)),
		})
		candidates := lo.Filter(candidatesFor(real), func(c *disruption.Candidate, _ int) bool {
			return c.ProviderID() != spareClaim.Status.ProviderID
		})
		Expect(candidates).To(HaveLen(2))

		cmds, err := real.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	It("starts every command exactly once when the controller runs the pass", func() {
		applyNodes(4, "1")
		controller := disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(newSingleNodeConsolidation(&scriptedValidator{})))

		ExpectSingletonReconciled(ctx, controller)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(3))
		providerIDs := sets.New[string]()
		for _, cmd := range cmds {
			for _, c := range cmd.Candidates {
				Expect(providerIDs.Has(c.ProviderID())).To(BeFalse(), fmt.Sprintf("%s was claimed twice", c.ProviderID()))
				providerIDs.Insert(c.ProviderID())
			}
		}
	})
})
