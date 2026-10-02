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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Multi-Node Consolidation Pass Interval", func() {
	var nodePool *v1.NodePool

	// runPass builds three underutilized nodes whose pods all fit on one of them, so the search
	// always finds a delete, and runs one multi-node pass with the given validator.
	runPass := func(validator *scriptedValidator) *disruption.MultiNodeConsolidation {
		GinkgoHelper()
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					Budgets:             []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
		nodeClaims, nodes := test.NodeClaimsAndNodes(3, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
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
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, nodePool, rs)
		pods := test.Pods(3, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app": "multi-node-interval"},
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
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			},
		})
		for i := range nodeClaims {
			nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i], pods[i])
			ExpectManualBinding(ctx, env.Client, pods[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		multi := disruption.NewMultiNodeConsolidation(
			disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(validator),
		)
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, multi.ShouldDisrupt, multi.Class(), queue)
		Expect(err).ToNot(HaveOccurred())
		Expect(candidates).To(HaveLen(3))
		_, err = multi.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		Expect(err).ToNot(HaveOccurred())
		return multi
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{MultiNodeConsolidationInterval: lo.ToPtr(10 * time.Minute)}))
	})

	It("leaves the next pass due after a command is found and executed", func() {
		validator := &scriptedValidator{}
		multi := runPass(validator)
		Expect(validator.calls).To(Equal(1))
		Expect(multi.DueForPass(ctx)).To(BeTrue())
	})

	It("leaves the next pass due after a found command is rejected at validation", func() {
		// Churn during the settling window rejects the command; the fleet still has a
		// consolidatable batch, so the next loop iteration should search again.
		validator := &scriptedValidator{errs: []error{disruption.NewSchedulingValidationError(errors.New("pods churned"))}}
		multi := runPass(validator)
		Expect(validator.calls).To(Equal(1))
		Expect(multi.DueForPass(ctx)).To(BeTrue())
	})
})
