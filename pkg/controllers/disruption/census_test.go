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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

const splitAttemptsMetric = "karpenter_voluntary_disruption_consolidation_split_attempts_total"

var _ = Describe("Census", func() {
	var censusController *disruption.CensusController
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node

	BeforeEach(func() {
		censusController = disruption.NewCensusController(
			disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue))
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})
		nodeClaims, nodes = test.NodeClaimsAndNodes(2, v1.NodeClaim{
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
		for _, nc := range nodeClaims {
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		disruption.ConsolidationActionableCandidates.Reset()
	})

	It("counts candidates with a cheaper option without executing anything", func() {
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		pods := test.Pods(3, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app": "census-test"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "apps/v1",
						Kind:               "ReplicaSet",
						Name:               rs.Name,
						UID:                rs.UID,
						Controller:         new(true),
						BlockOwnerDeletion: new(true),
					},
				},
			}})
		ExpectApplied(ctx, env.Client, rs, pods[0], pods[1], pods[2], nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)

		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[1])

		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})
		ExpectSingletonReconciled(ctx, censusController)

		// Both nodes' pods fit on the other node, so both are actionable deletes.
		ExpectMetricGaugeValue(disruption.ConsolidationActionableCandidates, 2, map[string]string{
			"nodepool": nodePool.Name,
			"decision": string(disruption.DeleteDecision),
		})
		ExpectMetricGaugeValue(disruption.ConsolidationCensusCandidatesEvaluated, 2, nil)

		// The census must not execute: no queue commands, nothing deleted.
		Expect(queue.GetCommands()).To(HaveLen(0))
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))
		Expect(ExpectNodes(ctx, env.Client)).To(HaveLen(2))
	})

	It("sweeps again after the configured interval", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{ConsolidationCensusInterval: lo.ToPtr(time.Hour)}))
		ExpectApplied(ctx, env.Client, nodeClaims[0], nodes[0], nodePool)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{nodes[0]}, []*v1.NodeClaim{nodeClaims[0]})

		result, err := censusController.Reconcile(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(time.Hour))
	})

	It("never sweeps or requeues when disabled", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{ConsolidationCensusInterval: lo.ToPtr(time.Duration(0))}))
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		pods := test.Pods(2, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app": "census-test"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "apps/v1",
						Kind:               "ReplicaSet",
						Name:               rs.Name,
						UID:                rs.UID,
						Controller:         new(true),
						BlockOwnerDeletion: new(true),
					},
				},
			}})
		ExpectApplied(ctx, env.Client, rs, pods[0], pods[1], nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[1])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})

		result, err := censusController.Reconcile(ctx)
		Expect(err).ToNot(HaveOccurred())
		// A zero result is how a singleton stops: it is never reconciled again.
		Expect(result).To(BeZero())
		// Both nodes are actionable deletes, so an enabled census would have published them.
		_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_consolidation_actionable_candidates", map[string]string{
			"nodepool": nodePool.Name,
		})
		Expect(found).To(BeFalse())
	})

	It("publishes zero actionable candidates when nodes cannot be consolidated", func() {
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		// Pods too large to fit anywhere else, so neither node has a cheaper option.
		pods := test.Pods(2, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app": "census-test"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "apps/v1",
						Kind:               "ReplicaSet",
						Name:               rs.Name,
						UID:                rs.UID,
						Controller:         new(true),
						BlockOwnerDeletion: new(true),
					},
				},
			},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("30")},
			}})
		ExpectApplied(ctx, env.Client, rs, pods[0], pods[1], nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)

		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[1])

		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})
		ExpectSingletonReconciled(ctx, censusController)

		ExpectMetricGaugeValue(disruption.ConsolidationCensusCandidatesEvaluated, 2, nil)
		Expect(queue.GetCommands()).To(HaveLen(0))
	})

	It("does not report its split retries as a pass that ran out of attempts", func() {
		// the sweep runs outside the real pass, so its retries are its own: reporting them against the
		// pass's attempt cap would drown the fallback's telemetry in read-only survey nodes
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			MaxConsolidationReplacements: lo.ToPtr(3),
			ConsolidationSplitFallback:   lo.ToPtr(true),
		}))
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		podOptions := test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app": "census-test"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "apps/v1",
						Kind:               "ReplicaSet",
						Name:               rs.Name,
						UID:                rs.UID,
						Controller:         new(true),
						BlockOwnerDeletion: new(true),
					},
				},
			},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("15")},
			}}
		// two pods on the first node keep it eligible for a split retry, and neither node's pods fit
		// on the other, so both candidates reach the fallback as ordinary no-ops
		pods := test.Pods(2, podOptions)
		bigPod := test.Pod(test.PodOptions{
			ObjectMeta: podOptions.ObjectMeta,
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("30")},
			}})
		ExpectApplied(ctx, env.Client, rs, pods[0], pods[1], bigPod, nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)

		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[0])
		ExpectManualBinding(ctx, env.Client, bigPod, nodes[1])

		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})
		ExpectSingletonReconciled(ctx, censusController)

		// scoped to the census type: the single-node suite legitimately exhausts a real budget, and the
		// registry is process-global
		_, found := FindMetricWithLabelValues(splitAttemptsMetric, map[string]string{
			"consolidation_type": disruption.CensusConsolidationType,
			"outcome":            disruption.SplitOutcomeAttemptCapExhausted,
		})
		Expect(found).To(BeFalse())
		// the sweep retries under its own budget, and what it records stays on its own consolidation type
		_, found = FindMetricWithLabelValues(splitAttemptsMetric, map[string]string{
			"consolidation_type": disruption.CensusConsolidationType,
			"nodepool":           nodePool.Name,
		})
		Expect(found).To(BeTrue())
	})
})
