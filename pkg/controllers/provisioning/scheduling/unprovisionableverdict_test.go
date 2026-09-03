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

package scheduling_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	pscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// These tests pin down which provisioning failures become an unprovisionable verdict in the cluster
// state (state.Cluster.PodUnprovisionableTime), the verdict disruption simulations use to leave a
// pending pod out. Only a rejection every NodePool issues on the pod and the NodePool alone - taints,
// requirements, or no instance type for the pod's own requirements - qualifies
// (scheduling.IsIncompatibleWithAllNodePools). Anything that can come out differently in another
// scheduling pass over the same NodePools records nothing: NodePool limits, which a node removal lifts;
// a reserved offering the strict provisioning mode deferred; topology, which depends on what else is
// running; and a pass the Solve deadline cut short before the pod was retried. Limits only shield a pod
// the NodePool could otherwise take: a NodePool at its limits still reports an incompatibility it has
// with the pod, so a capped NodePool does not keep every pod in every simulation.
var _ = Describe("Unprovisionable Verdicts", func() {
	var nodePool *v1.NodePool
	verdict := func(pod *corev1.Pod) bool {
		return !cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(pod)).IsZero()
	}

	BeforeEach(func() {
		nodePool = test.NodePool()
	})

	Context("incompatible with every NodePool", func() {
		It("should record a verdict for a pod that tolerates no NodePool's taints", func() {
			nodePool.Spec.Template.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule}}
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod()
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeTrue())
		})
		It("should record a verdict for a pod whose requirements contradict every NodePool's", func() {
			nodePool.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{{
				Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand},
			}}
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeSpot}})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeTrue())
		})
		It("should record a verdict for a pod pinned to a capacity type no instance type offers", func() {
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved}})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeTrue())
		})
		It("should record a verdict for a pod no instance type is large enough for", func() {
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10000")}},
			})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeTrue())
		})
		It("should not record a verdict when a second NodePool rejects the pod for a pass-dependent reason", func() {
			nodePool.Spec.Template.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule}}
			limited := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Limits: v1.Limits(corev1.ResourceList{resources.Node: resource.MustParse("0")})}})
			ExpectApplied(ctx, env.Client, nodePool, limited)
			pod := test.UnschedulablePod()
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeFalse())
		})
	})

	Context("NodePool limits", func() {
		It("should not record a verdict for a pod rejected because the NodePool's node limit is exhausted", func() {
			nodePool.Spec.Limits = v1.Limits(corev1.ResourceList{resources.Node: resource.MustParse("0")})
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod()
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeFalse())
		})
		It("should not record a verdict for a pod every remaining instance type exceeds the NodePool's limits for", func() {
			nodePool.Spec.Limits = v1.Limits(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")})
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod()
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeFalse())
		})
		It("should not record a verdict when limits trimmed the instance types the pod was filtered against", func() {
			// The 4-CPU limit leaves only the smallest instance types, none of which fits the pod; with the full set the
			// pod would fit, so the filter failure is a consequence of the limit and not of the pod's requirements.
			nodePool.Spec.Limits = v1.Limits(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")})
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}},
			})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeFalse())
		})
		It("should record a verdict for a pod that tolerates no taint of a NodePool at its node limit", func() {
			nodePool.Spec.Limits = v1.Limits(corev1.ResourceList{resources.Node: resource.MustParse("0")})
			nodePool.Spec.Template.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule}}
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod()
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeTrue())
		})
		It("should record a verdict for a pod whose requirements contradict those of a NodePool at its node limit", func() {
			nodePool.Spec.Limits = v1.Limits(corev1.ResourceList{resources.Node: resource.MustParse("0")})
			nodePool.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{{
				Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand},
			}}
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeSpot}})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeTrue())
		})
		It("should record a verdict for a pod no instance type of a NodePool at its limits could take", func() {
			nodePool.Spec.Limits = v1.Limits(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")})
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved}})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeTrue())
		})
		It("should record a verdict when the instance types limits trimmed away would reject the pod too", func() {
			nodePool.Spec.Limits = v1.Limits(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")})
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved}})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeTrue())
		})
		It("should not record a verdict for a topology-constrained pod a NodePool at its limits rejects", func() {
			nodePool.Spec.Limits = v1.Limits(corev1.ResourceList{resources.Node: resource.MustParse("0")})
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{
				ObjectMeta:           metav1.ObjectMeta{Labels: map[string]string{"app": "spread"}},
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10000")}},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelTopologyZone,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "spread"}},
				}},
			})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeFalse())
		})
	})

	Context("topology", func() {
		It("should not record a verdict for a pod whose affinity no other pod satisfies", func() {
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{
				PodRequirements: []corev1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "absent"}},
					TopologyKey:   corev1.LabelHostname,
				}},
			})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeFalse())
		})
		It("should not record a verdict for a topology-constrained pod no instance type is large enough for", func() {
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod(test.PodOptions{
				ObjectMeta:           metav1.ObjectMeta{Labels: map[string]string{"app": "spread"}},
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10000")}},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelTopologyZone,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "spread"}},
				}},
			})
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pod)
			Expect(ExpectNodeClaims(ctx, env.Client)).To(BeEmpty())
			Expect(verdict(pod)).To(BeFalse())
		})
	})

	Context("reserved offerings", func() {
		BeforeEach(func() {
			nodePool.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{{
				Key:      v1.CapacityTypeLabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand, v1.CapacityTypeReserved},
			}}
			cloudProvider.Reset()
			cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{
				fake.NewInstanceType("large-instance-type", fake.WithResources(map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("6Gi")})),
				fake.NewInstanceType("medium-instance-type", fake.WithResources(map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("3"), corev1.ResourceMemory: resource.MustParse("3Gi")})),
				fake.NewInstanceType("small-instance-type", fake.WithResources(map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")})),
			}
			for _, it := range cloudProvider.InstanceTypes[1:] {
				it.Requirements.Get(v1.CapacityTypeLabelKey).Insert(v1.CapacityTypeReserved)
				it.Offerings = append(it.Offerings, &cloudprovider.Offering{
					ReservationCapacity: 1,
					Available:           true,
					Requirements: pscheduling.NewLabelRequirements(map[string]string{
						v1.CapacityTypeLabelKey:     v1.CapacityTypeReserved,
						corev1.LabelTopologyZone:    "test-zone-1",
						v1alpha1.LabelReservationID: fmt.Sprintf("r-%s", it.Name),
					}),
					Price: fake.PriceFromResources(it.Capacity) / 100_000.0,
				})
			}
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{ReservedCapacity: lo.ToPtr(true)}}))
		})
		It("should not record a verdict for a pod deferred by reservation contention within the pass", func() {
			// Three pods compete for two reservations of capacity one. The strict provisioning mode schedules one pod per
			// pass and defers the others with a ReservedOfferingError; the next pass places another of them.
			ExpectApplied(ctx, env.Client, nodePool)
			pods := lo.Times(3, func(_ int) *corev1.Pod {
				return test.UnschedulablePod(test.PodOptions{
					ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1800m")}},
				})
			})
			result := ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, pods...)
			Expect(result.Bindings).To(HaveLen(1))
			deferred := lo.Filter(pods, func(p *corev1.Pod, _ int) bool { return result.Get(p) == nil })
			Expect(deferred).To(HaveLen(2))
			for _, p := range deferred {
				Expect(verdict(p)).To(BeFalse())
			}

			result = ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, deferred...)
			Expect(result.Bindings).To(HaveLen(1))
		})
	})

	Context("Solve deadline", func() {
		placed := func(results scheduling.Results, p *corev1.Pod) bool {
			return lo.ContainsBy(results.NewNodeClaims, func(nc *scheduling.NodeClaim) bool {
				return lo.ContainsBy(nc.Pods, func(np *corev1.Pod) bool { return np.UID == p.UID })
			})
		}

		It("should not classify a pod as incompatible when the deadline cut the pass before its retry", func() {
			// a (popped first, 2 CPU) has a required hostname affinity to b (1 CPU). a fails its first attempt because no b
			// exists yet, is requeued, b opens a NodeClaim, and a's retry lands next to b. Cutting the pass between a's
			// first failure and its retry leaves a in PodErrors with a topology error, which is not an incompatibility.
			ExpectApplied(ctx, env.Client, nodePool)
			bLabels := map[string]string{"app": "b"}
			b := test.UnschedulablePod(test.PodOptions{
				ObjectMeta:           metav1.ObjectMeta{Labels: bLabels},
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
			})
			a := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}},
				PodRequirements: []corev1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{MatchLabels: bLabels},
					TopologyKey:   corev1.LabelHostname,
				}},
			})
			ExpectApplied(ctx, env.Client, a, b)

			s, err := prov.NewScheduler(ctx, []*corev1.Pod{a, b}, nil, nil)
			Expect(err).ToNot(HaveOccurred())
			results, err := s.Solve(ctx, []*corev1.Pod{a, b})
			Expect(err).ToNot(HaveOccurred())
			Expect(results.PodErrors).To(BeEmpty())
			Expect(placed(results, a)).To(BeTrue())
			Expect(placed(results, b)).To(BeTrue())

			for n := int64(1); n <= 500; n++ {
				s, err := prov.NewScheduler(ctx, []*corev1.Pod{a, b}, nil, nil)
				Expect(err).ToNot(HaveOccurred())
				results, err := s.Solve(deadlineAfterErrCalls(ctx, n), []*corev1.Pod{a, b})
				if !errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if _, aErrored := results.PodErrors[a]; !aErrored || placed(results, a) || !placed(results, b) {
					continue
				}
				Expect(results.PodsIncompatibleWithAllNodePools()).To(BeEmpty())
				return
			}
			Fail("found no deadline point between a's first failure and its retry")
		})
		It("should classify a pod every NodePool is incompatible with even when the deadline cut the pass", func() {
			nodePool.Spec.Template.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule}}
			ExpectApplied(ctx, env.Client, nodePool)
			pod := test.UnschedulablePod()
			ExpectApplied(ctx, env.Client, pod)

			for n := int64(1); n <= 500; n++ {
				s, err := prov.NewScheduler(ctx, []*corev1.Pod{pod}, nil, nil)
				Expect(err).ToNot(HaveOccurred())
				results, err := s.Solve(deadlineAfterErrCalls(ctx, n), []*corev1.Pod{pod})
				if !errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if _, errored := results.PodErrors[pod]; !errored {
					continue
				}
				Expect(results.PodsIncompatibleWithAllNodePools()).To(ConsistOf(pod))
				return
			}
			Fail("found no deadline point after the pod's first failure")
		})
	})
})

// deadlineContext reports context.DeadlineExceeded from Err() once it has been consulted remaining times. Done() is
// inherited and never closes; Solve and trySchedule only poll Err().
type deadlineContext struct {
	context.Context
	remaining atomic.Int64
}

func deadlineAfterErrCalls(parent context.Context, n int64) *deadlineContext {
	c := &deadlineContext{Context: parent}
	c.remaining.Store(n)
	return c
}

func (c *deadlineContext) Err() error {
	if c.remaining.Add(-1) <= 0 {
		return context.DeadlineExceeded
	}
	return c.Context.Err()
}
