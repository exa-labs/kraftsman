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

// End-to-end scheduling of self-selecting pod affinities that bootstrap their domain: the scheduler tries the cheapest
// domain first and falls back to the next one when the cheapest leaves nothing to launch.

package scheduling_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	pscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Pod Affinity Bootstrap", func() {
	var nodePool *v1.NodePool
	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Template: v1.NodeClaimTemplate{
					Spec: v1.NodeClaimTemplateSpec{
						Requirements: []v1.NodeSelectorRequirementWithMinValues{{
							Key:      v1.CapacityTypeLabelKey,
							Operator: corev1.NodeSelectorOpExists,
						}},
					},
				},
			},
		})
	})

	// zonalInstanceType is an on-demand instance type with cpu CPUs offered at the given price in each zone.
	zonalInstanceType := func(name, cpu string, prices map[string]float64) *cloudprovider.InstanceType {
		var offerings []cloudprovider.Offering
		for zone, price := range prices {
			offerings = append(offerings, cloudprovider.Offering{
				Available: true,
				Price:     price,
				Requirements: pscheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
					corev1.LabelTopologyZone: zone,
				}),
			})
		}
		return fake.NewInstanceType(name,
			fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse("16Gi")}),
			fake.WithOfferings(offerings...))
	}

	// gangPod is a pod requesting cpu with a required pod affinity to its own gang on the zone key.
	gangPod := func(gang, cpu string) *corev1.Pod {
		labels := map[string]string{"gang": gang}
		return test.UnschedulablePod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			},
			PodRequirements: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
				TopologyKey:   corev1.LabelTopologyZone,
			}},
		})
	}

	It("should bootstrap in the cheapest zone", func() {
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{
			zonalInstanceType("default", "4", map[string]float64{"test-zone-1": 3, "test-zone-2": 1, "test-zone-3": 2}),
		}
		ExpectApplied(ctx, env.Client, nodePool)
		pod := gangPod("cheapest", "1")
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, pod)
		node := ExpectScheduled(ctx, env.Client, pod)
		Expect(node.Labels).To(HaveKeyWithValue(corev1.LabelTopologyZone, "test-zone-2"))
	})
	It("should fall back to the next zone when daemon overhead leaves the cheapest one without a fitting instance type", func() {
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{
			zonalInstanceType("small", "2", map[string]float64{"test-zone-1": 0.1}),
			zonalInstanceType("big", "8", map[string]float64{"test-zone-2": 1}),
		}
		ds := test.DaemonSet(test.DaemonSetOptions{PodOptions: test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
		}})
		ExpectApplied(ctx, env.Client, nodePool, ds)
		for i := range 20 {
			pod := gangPod(fmt.Sprintf("daemon-overhead-%d", i), "1.5")
			ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, pod)
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(corev1.LabelTopologyZone, "test-zone-2"))
		}
	})
	It("should fall back to the next zone when the cheapest one cannot satisfy strict minValues", func() {
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{
			zonalInstanceType("type-a", "4", map[string]float64{"test-zone-1": 0.1}),
			zonalInstanceType("type-b", "4", map[string]float64{"test-zone-2": 1}),
			zonalInstanceType("type-c", "4", map[string]float64{"test-zone-2": 1.1}),
		}
		nodePool.Spec.Template.Spec.Requirements = append(nodePool.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
			Key:       corev1.LabelInstanceTypeStable,
			Operator:  corev1.NodeSelectorOpExists,
			MinValues: new(2),
		})
		ExpectApplied(ctx, env.Client, nodePool)
		pod := gangPod("min-values", "1")
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, pod)
		node := ExpectScheduled(ctx, env.Client, pod)
		Expect(node.Labels).To(HaveKeyWithValue(corev1.LabelTopologyZone, "test-zone-2"))
	})
	It("should spread gangs across equally priced zones", func() {
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{
			zonalInstanceType("default", "4", map[string]float64{"test-zone-1": 1, "test-zone-2": 1, "test-zone-3": 1}),
		}
		ExpectApplied(ctx, env.Client, nodePool)
		// Each pod needs most of a node, so no two gangs share one.
		var pods []*corev1.Pod
		for i := range 8 {
			pods = append(pods, gangPod(fmt.Sprintf("uniform-%d", i), "3"))
		}
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, pods...)
		zones := sets.New[string]()
		for _, pod := range pods {
			zones.Insert(ExpectScheduled(ctx, env.Client, pod).Labels[corev1.LabelTopologyZone])
		}
		Expect(zones.Len()).To(BeNumerically(">", 1))
	})
})
