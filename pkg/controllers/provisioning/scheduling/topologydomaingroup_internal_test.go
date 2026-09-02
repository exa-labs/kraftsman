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

package scheduling

import (
	"slices"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func zonalNodePool(name string, zones []string, taints ...corev1.Taint) *v1.NodePool {
	return &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.NodePoolSpec{
			Template: v1.NodeClaimTemplate{
				Spec: v1.NodeClaimTemplateSpec{
					Taints: taints,
					Requirements: []v1.NodeSelectorRequirementWithMinValues{{
						Key:      corev1.LabelTopologyZone,
						Operator: corev1.NodeSelectorOpIn,
						Values:   zones,
					}},
				},
			},
		},
	}
}

func zonalInstanceTypes(zones []string) []*cloudprovider.InstanceType {
	return []*cloudprovider.InstanceType{{
		Name: "default-instance-type",
		Requirements: scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zones...),
		),
	}}
}

func zoneSpreadPod(nodeSelector map[string]string, tolerations ...corev1.Toleration) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Labels: map[string]string{"app": "mimir"}},
		Spec: corev1.PodSpec{
			NodeSelector: nodeSelector,
			Tolerations:  tolerations,
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				TopologyKey:       corev1.LabelTopologyZone,
				WhenUnsatisfiable: corev1.DoNotSchedule,
				MaxSkew:           1,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mimir"}},
			}},
		},
	}
}

func spreadDomains(t *testing.T, pod *corev1.Pod, nodePools []*v1.NodePool, taintPolicy *corev1.NodeInclusionPolicy) []string {
	t.Helper()
	instanceTypes := map[string][]*cloudprovider.InstanceType{}
	for _, np := range nodePools {
		zones := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...).Get(corev1.LabelTopologyZone)
		instanceTypes[np.Name] = zonalInstanceTypes(zones.Values())
	}
	domainGroups := buildDomainGroups(nodePools, instanceTypes)
	group := NewTopologyGroup(
		TopologyTypeSpread,
		corev1.LabelTopologyZone,
		pod,
		sets.New(pod.Namespace),
		pod.Spec.TopologySpreadConstraints[0].LabelSelector,
		pod.Spec.TopologySpreadConstraints[0].MaxSkew,
		nil,
		taintPolicy,
		nil,
		domainGroups[corev1.LabelTopologyZone],
	)
	domains := sets.List(sets.KeySet(group.domains))
	sort.Strings(domains)
	return domains
}

// A pod pinned to one NodePool must not have the domains of NodePools it can never select counted
// in its spread: those domains hold no pods, so they would hold the group's minimum at zero and
// leave every reachable domain outside maxSkew, making the spread permanently unsatisfiable.
func TestTopologyGroupSpreadSkipsUnselectableNodePoolDomains(t *testing.T) {
	nodePools := []*v1.NodePool{
		zonalNodePool("monitoring", []string{"us-west-2a", "us-west-2b", "us-west-2c"}),
		zonalNodePool("accelerators", []string{"us-west-2a", "ap-northeast-1a"}),
	}
	pod := zoneSpreadPod(map[string]string{v1.NodePoolLabelKey: "monitoring"})

	domains := spreadDomains(t, pod, nodePools, nil)
	if want := []string{"us-west-2a", "us-west-2b", "us-west-2c"}; !slices.Equal(domains, want) {
		t.Fatalf("expected only the domains of the selected nodepool %v, got %v", want, domains)
	}
}

func TestTopologyGroupSpreadCountsAllDomainsWithoutNodeSelector(t *testing.T) {
	nodePools := []*v1.NodePool{
		zonalNodePool("monitoring", []string{"us-west-2a", "us-west-2b"}),
		zonalNodePool("accelerators", []string{"ap-northeast-1a"}),
	}
	pod := zoneSpreadPod(nil)

	domains := spreadDomains(t, pod, nodePools, nil)
	if want := []string{"ap-northeast-1a", "us-west-2a", "us-west-2b"}; !slices.Equal(domains, want) {
		t.Fatalf("expected every domain %v, got %v", want, domains)
	}
}

// Required node affinity restricts the domains the same way a node selector does, since both are
// honored by the default NodeAffinityPolicy.
func TestTopologyGroupSpreadHonorsRequiredNodeAffinity(t *testing.T) {
	nodePools := []*v1.NodePool{
		zonalNodePool("monitoring", []string{"us-west-2a", "us-west-2b"}),
		zonalNodePool("accelerators", []string{"ap-northeast-1a"}),
	}
	pod := zoneSpreadPod(nil)
	pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      v1.NodePoolLabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"monitoring"},
			}},
		}}},
	}}

	domains := spreadDomains(t, pod, nodePools, nil)
	if want := []string{"us-west-2a", "us-west-2b"}; !slices.Equal(domains, want) {
		t.Fatalf("expected only the domains of the affine nodepool %v, got %v", want, domains)
	}
}

// A NodePool leaves some labels to whichever instance type gets launched, so a pod selecting one of
// those keys must not lose the pool's domains: only an outright conflict drops a domain.
func TestTopologyGroupSpreadKeepsDomainsForInstanceTypeOnlyLabels(t *testing.T) {
	nodePools := []*v1.NodePool{zonalNodePool("monitoring", []string{"us-west-2a", "us-west-2b"})}
	pod := zoneSpreadPod(map[string]string{"example.com/flavor": "gpu"})

	instanceTypes := map[string][]*cloudprovider.InstanceType{"monitoring": {{
		Name: "gpu-instance-type",
		Requirements: scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "us-west-2a", "us-west-2b"),
			scheduling.NewRequirement("example.com/flavor", corev1.NodeSelectorOpIn, "gpu"),
		),
	}}}
	domainGroups := buildDomainGroups(nodePools, instanceTypes)
	group := NewTopologyGroup(TopologyTypeSpread, corev1.LabelTopologyZone, pod, sets.New(pod.Namespace),
		pod.Spec.TopologySpreadConstraints[0].LabelSelector, 1, nil, nil, nil, domainGroups[corev1.LabelTopologyZone])

	domains := sets.List(sets.KeySet(group.domains))
	if want := []string{"us-west-2a", "us-west-2b"}; !slices.Equal(domains, want) {
		t.Fatalf("expected the instance types' domains %v, got %v", want, domains)
	}
}

// An ignored NodeAffinityPolicy opts out of the filtering entirely, as it does upstream.
func TestTopologyGroupSpreadIgnoredAffinityPolicyCountsAllDomains(t *testing.T) {
	nodePools := []*v1.NodePool{
		zonalNodePool("monitoring", []string{"us-west-2a"}),
		zonalNodePool("accelerators", []string{"ap-northeast-1a"}),
	}
	pod := zoneSpreadPod(map[string]string{v1.NodePoolLabelKey: "monitoring"})
	ignore := corev1.NodeInclusionPolicyIgnore
	pod.Spec.TopologySpreadConstraints[0].NodeAffinityPolicy = &ignore

	instanceTypes := map[string][]*cloudprovider.InstanceType{}
	for _, np := range nodePools {
		zones := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...).Get(corev1.LabelTopologyZone)
		instanceTypes[np.Name] = zonalInstanceTypes(zones.Values())
	}
	domainGroups := buildDomainGroups(nodePools, instanceTypes)
	group := NewTopologyGroup(TopologyTypeSpread, corev1.LabelTopologyZone, pod, sets.New(pod.Namespace),
		pod.Spec.TopologySpreadConstraints[0].LabelSelector, 1, nil, nil, &ignore, domainGroups[corev1.LabelTopologyZone])

	domains := sets.List(sets.KeySet(group.domains))
	if want := []string{"ap-northeast-1a", "us-west-2a"}; !slices.Equal(domains, want) {
		t.Fatalf("expected every domain %v, got %v", want, domains)
	}
}

// Taint filtering keeps working, including when it is the only thing excluding a domain.
func TestTopologyGroupSpreadHonorsTaints(t *testing.T) {
	taint := corev1.Taint{Key: "accelerator", Value: "true", Effect: corev1.TaintEffectNoSchedule}
	nodePools := []*v1.NodePool{
		zonalNodePool("monitoring", []string{"us-west-2a"}),
		zonalNodePool("accelerators", []string{"us-west-2b"}, taint),
	}
	honor := corev1.NodeInclusionPolicyHonor

	domains := spreadDomains(t, zoneSpreadPod(nil), nodePools, &honor)
	if want := []string{"us-west-2a"}; !slices.Equal(domains, want) {
		t.Fatalf("expected the tainted nodepool's domain to be skipped, got %v", domains)
	}

	tolerating := zoneSpreadPod(nil, corev1.Toleration{Key: "accelerator", Operator: corev1.TolerationOpExists})
	domains = spreadDomains(t, tolerating, nodePools, &honor)
	if want := []string{"us-west-2a", "us-west-2b"}; !slices.Equal(domains, want) {
		t.Fatalf("expected a tolerating pod to keep every domain %v, got %v", want, domains)
	}
}
