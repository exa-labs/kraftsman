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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Scheduler metrics from disruption simulations", func() {
	It("labels construction phases with the disruption controller", func() {
		scheduling.ConstructionPhaseDurationSeconds.Reset()
		nodePool := test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
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
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		pod := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
			Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("1")},
		}})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))
		for _, phase := range []string{"domain_groups", "topology_update", "existing_nodes", "node_claim_templates"} {
			_, found := FindMetricWithLabelValues("karpenter_scheduler_construction_phase_duration_seconds", map[string]string{"controller": "disruption", "phase": phase})
			Expect(found).To(BeTrue(), phase)
		}
		_, found := FindMetricWithLabelValues("karpenter_scheduler_construction_phase_duration_seconds", map[string]string{"controller": "provisioner"})
		Expect(found).To(BeFalse())
	})
})
