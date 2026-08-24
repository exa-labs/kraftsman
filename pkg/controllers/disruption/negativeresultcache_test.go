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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Negative Result Skip Cache", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	var pod *corev1.Pod
	var rs *appsv1.ReplicaSet
	var singleNode *disruption.SingleNodeConsolidation
	labels := map[string]string{"app": "negative-cache"}

	// negativeSkips reads the running total of candidates skipped as unchanged negatives, and
	// lookups the running total of cache lookups with the given outcome. Both are process-wide,
	// so the tests compare deltas. The skip metric also carries NodePool and instance labels, and
	// FindMetricWithLabelValues returns the first series matching a label subset, so the lookup
	// pins this test's NodePool or an earlier spec's series would shadow this one's.
	negativeSkips := func() float64 {
		GinkgoHelper()
		metric, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_consolidation_candidate_skips_total", map[string]string{
			"consolidation_type": disruption.SingleNodeConsolidationType,
			"reason":             disruption.CandidateSkipUnchangedNegative,
			"nodepool":           nodePool.Name,
		})
		if !found {
			return 0
		}
		return metric.GetCounter().GetValue()
	}

	lookups := func(outcome string) float64 {
		GinkgoHelper()
		metric, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_consolidation_negative_cache_lookups_total", map[string]string{
			"consolidation_type": disruption.SingleNodeConsolidationType,
			"outcome":            outcome,
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

	// runPass walks the candidate list once, from live cluster state.
	runPass := func() {
		GinkgoHelper()
		// A pass that found nothing marks the cluster consolidated and the next pass returns
		// immediately; in production any state change reopens it, which is also when a stored
		// verdict is worth consulting. The state is a timestamp, so the fake clock has to move
		// for the mark to register as a change.
		env.Clock.Step(time.Second)
		cluster.MarkUnconsolidated()
		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		// The fixture is deliberately unconsolidatable: one node, whose pod has nowhere else to
		// go and whose instance type is already the cheapest that fits it.
		Expect(cmds).To(BeEmpty())
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			ConsolidationSkipUnchangedNegatives: lo.ToPtr(true),
			ConsolidationNegativeCacheTTL:       lo.ToPtr(5 * time.Minute),
		}))
		// Without a revision the candidate cannot be fingerprinted and is never skipped, which is
		// the fail-closed path a provider that cannot version its offerings takes.
		cloudProvider.InstanceTypesRevision = 1

		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
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
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		rs = test.ReplicaSet()
		// The pod's owner reference needs the ReplicaSet's server-assigned UID.
		ExpectApplied(ctx, env.Client, nodePool, rs)
		pod = test.Pod(test.PodOptions{
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
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("30")},
			},
		})
		ExpectApplied(ctx, env.Client, nodeClaim, node, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

		// The scripted validator with no script accepts every command immediately, which the
		// clear-on-admission test needs; the other tests never produce one.
		singleNode = disruption.NewSingleNodeConsolidation(
			disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(&scriptedValidator{}),
		)
	})

	AfterEach(func() {
		cloudProvider.InstanceTypesRevision = 0
		ExpectCleanedUp(ctx, env.Client)
	})

	It("skips a candidate whose no-op verdict cannot have changed", func() {
		skipped := negativeSkips()
		absent, hit := lookups(disruption.NegativeCacheLookupAbsent), lookups(disruption.NegativeCacheLookupHit)

		runPass()
		// Nothing was stored yet, so the first pass simulates and reports the lookup as absent.
		Expect(lookups(disruption.NegativeCacheLookupAbsent)).To(BeNumerically(">", absent))
		Expect(negativeSkips()).To(Equal(skipped))

		runPass()
		Expect(lookups(disruption.NegativeCacheLookupHit)).To(BeNumerically(">", hit))
		Expect(negativeSkips()).To(BeNumerically(">", skipped))
	})

	It("does not mark the cluster consolidated when a pass skipped candidates on cached verdicts", func() {
		// A pass that simulated every candidate may mark the fleet consolidated; a pass that
		// served even one candidate from the cache did not actually examine it, so the mark
		// would stop future passes from ever revisiting the entry as it ages out.
		runPass()
		Expect(singleNode.IsConsolidated()).To(BeTrue())

		runPass()
		Expect(negativeSkips()).To(BeNumerically(">", 0))
		Expect(singleNode.IsConsolidated()).To(BeFalse())
	})

	It("re-simulates a candidate whose pod changed under it", func() {
		runPass()
		changed := lookups(disruption.NegativeCacheLookupChanged)
		skipped := negativeSkips()

		// A toleration a pod gains is invisible to its UID but can change where it fits, so the
		// verdict computed before it must not be reused.
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Operator: corev1.TolerationOpExists})
		ExpectApplied(ctx, env.Client, pod)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

		runPass()
		Expect(lookups(disruption.NegativeCacheLookupChanged)).To(BeNumerically(">", changed))
		Expect(negativeSkips()).To(Equal(skipped))
	})

	It("re-simulates every candidate once the verdict ages out", func() {
		runPass()
		expired := lookups(disruption.NegativeCacheLookupExpired)
		skipped := negativeSkips()

		env.Clock.Step(6 * time.Minute)

		runPass()
		// The sweep runs at the end of the pass, so the lookup still sees the aged entry and
		// reports it as expired rather than as one that was never stored.
		Expect(lookups(disruption.NegativeCacheLookupExpired)).To(BeNumerically(">", expired))
		Expect(negativeSkips()).To(Equal(skipped))
	})

	It("re-simulates every candidate after a pass admits a command", func() {
		runPass()
		absent := lookups(disruption.NegativeCacheLookupAbsent)
		hit := lookups(disruption.NegativeCacheLookupHit)

		// The stored verdict serves a hit while nothing has changed.
		runPass()
		Expect(lookups(disruption.NegativeCacheLookupHit)).To(BeNumerically(">", hit))

		// A second node whose pod fits on the first is a real delete: admitting it frees
		// capacity every stored verdict was computed without, so the cache must be dropped.
		extraClaim, extraNode := test.NodeClaimAndNode(v1.NodeClaim{
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
		extraClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		extraPod := test.Pod(test.PodOptions{
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
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			},
		})
		ExpectApplied(ctx, env.Client, extraClaim, extraNode, extraPod)
		ExpectManualBinding(ctx, env.Client, extraPod, extraNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{extraNode}, []*v1.NodeClaim{extraClaim})

		// The pass that admits the delete may or may not reach the original candidate first —
		// candidate order between equal-cost candidates is not fixed — so this pass only asserts
		// the command; what matters is that admitting it drops every stored verdict.
		env.Clock.Step(time.Second)
		cluster.MarkUnconsolidated()
		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))

		// The admitted command cleared the cache, so the next pass simulates the unconsolidatable
		// node afresh rather than serving a verdict from a fleet that no longer exists. The
		// admitted delete is not executed by ComputeCommands, so the pass proposes it again.
		env.Clock.Step(time.Second)
		cluster.MarkUnconsolidated()
		_, err = singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates()...)
		Expect(err).To(Succeed())
		Expect(lookups(disruption.NegativeCacheLookupAbsent)).To(BeNumerically(">", absent))
	})

	It("ages a verdict out on schedule even when observation passes keep re-storing it", func() {
		// Observation mode: lookups are counted but the candidate is still simulated, so every
		// pass ends in the same no-op and re-stores the verdict. The re-store must not refresh
		// the expiry, or the reported hit rate would overstate what enabling the skip delivers.
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			ConsolidationSkipUnchangedNegatives: lo.ToPtr(false),
			ConsolidationNegativeCacheTTL:       lo.ToPtr(5 * time.Minute),
		}))
		runPass()
		expired := lookups(disruption.NegativeCacheLookupExpired)

		env.Clock.Step(2 * time.Minute)
		runPass()
		env.Clock.Step(2 * time.Minute)
		runPass()
		// 4+ minutes of re-storing passes later, the original 5-minute expiry still stands.
		env.Clock.Step(2 * time.Minute)
		runPass()
		Expect(lookups(disruption.NegativeCacheLookupExpired)).To(BeNumerically(">", expired))
	})

	It("re-simulates every candidate after another method's command completes", func() {
		runPass()
		hit := lookups(disruption.NegativeCacheLookupHit)
		runPass()
		Expect(lookups(disruption.NegativeCacheLookupHit)).To(BeNumerically(">", hit))
		absent := lookups(disruption.NegativeCacheLookupAbsent)

		// Drift, emptiness, expiration, and multi-node consolidation all execute through the
		// same queue, and a node any of them removes frees capacity no candidate fingerprint
		// can see. A command completing between passes must therefore empty the cache.
		queue.CompleteCommand(&disruption.Command{Succeeded: true})

		runPass()
		Expect(lookups(disruption.NegativeCacheLookupAbsent)).To(BeNumerically(">", absent))
	})

	It("never skips a candidate when the provider cannot version its offerings", func() {
		cloudProvider.InstanceTypesRevision = 0
		absent := lookups(disruption.NegativeCacheLookupAbsent)
		skipped := negativeSkips()

		runPass()
		runPass()
		// An unfingerprintable candidate never reaches the cache at all: no lookup, no skip.
		Expect(lookups(disruption.NegativeCacheLookupAbsent)).To(Equal(absent))
		Expect(negativeSkips()).To(Equal(skipped))
	})
})
