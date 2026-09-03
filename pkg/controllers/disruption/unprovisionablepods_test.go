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
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// These tests cover the exclusion of pending pods the provisioner could place nowhere from
// disruption scheduling simulations (unprovisionablepods.go). The fixture is the two-node,
// three-pod layout of "can delete nodes with a permanently pending pod": nodes[1] hosts one pod that
// fits on nodes[0], so single-node consolidation deletes nodes[1] regardless of the pending backlog.
// What varies is the backlog: a pod pinned to a capacity type no NodePool offers, which every
// provisioning pass records an error for, or a pod any NodePool can launch for.
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
