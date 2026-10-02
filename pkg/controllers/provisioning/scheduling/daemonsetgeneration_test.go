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
	"context"
	"fmt"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
)

// liveDaemonPod returns a running pod of DaemonSet "node-agent" bound to node, carrying what the
// API server and the DaemonSet controller give each pod individually: its own name and UID, the
// node binding, the node-pinning matchFields affinity term, and a projected service account token
// volume with a per-pod name.
func liveDaemonPod(name, node, tokenVolume string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "kube-system", UID: types.UID(name + "-uid"), ResourceVersion: "7",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "node-agent", UID: "ds-uid", Controller: lo.ToPtr(true)}},
		},
		Spec: corev1.PodSpec{
			NodeName: node,
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64"}}},
					MatchFields:      []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node}}},
				}},
			}}},
			Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{{
				Name:         "agent",
				Ports:        []corev1.ContainerPort{{ContainerPort: 9100, HostPort: 9100, Protocol: corev1.ProtocolTCP}},
				VolumeMounts: []corev1.VolumeMount{{Name: tokenVolume, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount"}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
				},
			}},
			Volumes: []corev1.Volume{{Name: tokenVolume, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}}}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "agent", RestartCount: 3, ContainerID: "containerd://" + name,
				AllocatedResources: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
			}},
		},
	}
}

func mustDaemonSetGeneration(t *testing.T, pods ...*corev1.Pod) string {
	t.Helper()
	generation, ok := daemonSetPodsGeneration(pods)
	if !ok {
		t.Fatalf("generation not computable")
	}
	return generation
}

// TestDaemonSetGenerationIgnoresWhichPodStandsIn: the scheduler is handed the newest running pod of
// each DaemonSet, which changes every time a node joins. Two pods of one DaemonSet differ only in
// what the daemon caches never read, so they must not move the generation and flush the caches.
func TestDaemonSetGenerationIgnoresWhichPodStandsIn(t *testing.T) {
	a := liveDaemonPod("node-agent-x7k2p", "node-a", "kube-api-access-4hq9z")
	b := liveDaemonPod("node-agent-m3c8d", "node-b", "kube-api-access-v2r6t")
	b.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debugger"}}}
	if mustDaemonSetGeneration(t, a) != mustDaemonSetGeneration(t, b) {
		t.Fatalf("pods of one DaemonSet that differ only in per-pod fields produced different generations")
	}

	cache := NewDaemonOverheadCache()
	cache.updateDaemonSetGeneration([]*corev1.Pod{a})
	cache.setDaemonRequests("node-key", corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")})
	cache.updateDaemonSetGeneration([]*corev1.Pod{b})
	if _, ok := cache.daemonRequests("node-key"); !ok {
		t.Fatalf("another pod of the same DaemonSet standing in flushed the daemon cache")
	}
}

// TestDaemonSetGenerationTracksEveryReadInput: each input a daemon cache reads must move the
// generation, including the status resources requests honor during an in-place resize.
func TestDaemonSetGenerationTracksEveryReadInput(t *testing.T) {
	base := mustDaemonSetGeneration(t, liveDaemonPod("node-agent-x7k2p", "node-a", "kube-api-access-4hq9z"))
	for name, mutate := range map[string]func(*corev1.Pod){
		"cpu request": func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("200m")
		},
		"limit": func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
		},
		"sidecar init container": func(p *corev1.Pod) {
			p.Spec.InitContainers = []corev1.Container{{Name: "sidecar", RestartPolicy: lo.ToPtr(corev1.ContainerRestartPolicyAlways),
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m")}}}}
		},
		"pod overhead": func(p *corev1.Pod) {
			p.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m")}
		},
		"pod-level resources": func(p *corev1.Pod) {
			p.Spec.Resources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
		},
		"toleration": func(p *corev1.Pod) {
			p.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
		},
		"node selector": func(p *corev1.Pod) { p.Spec.NodeSelector = map[string]string{"pool": "general"} },
		"affinity expression": func(p *corev1.Pod) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"arm64"}
		},
		"affinity term count": func(p *corev1.Pod) {
			terms := &p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
			*terms = append(*terms, corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelOSStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"linux"}}}})
		},
		"host port": func(p *corev1.Pod) { p.Spec.Containers[0].Ports[0].HostPort = 9200 },
		"resource claim": func(p *corev1.Pod) {
			p.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "gpu"}}
		},
		"status allocated resources": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].AllocatedResources[corev1.ResourceCPU] = resource.MustParse("300m")
		},
		"status actuated resources": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].Resources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("300m")}}
		},
		"resize infeasible": func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodResizePending, Status: corev1.ConditionTrue, Reason: corev1.PodReasonInfeasible}}
		},
		"namespace":     func(p *corev1.Pod) { p.Namespace = "monitoring" },
		"daemonset":     func(p *corev1.Pod) { p.OwnerReferences[0].Name = "other-agent" },
		"no controller": func(p *corev1.Pod) { p.OwnerReferences = nil },
	} {
		pod := liveDaemonPod("node-agent-x7k2p", "node-a", "kube-api-access-4hq9z")
		mutate(pod)
		if mustDaemonSetGeneration(t, pod) == base {
			t.Errorf("%s: generation did not change", name)
		}
	}
}

// BenchmarkDaemonOverheadGroupsAcrossStandInSwaps measures one pass building daemon overhead groups
// while the pods standing in for its DaemonSets change between constructions, as they do whenever a
// node joins: every construction alternates between two running pods of each DaemonSet.
func BenchmarkDaemonOverheadGroupsAcrossStandInSwaps(b *testing.B) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	templates, daemons := daemonOverheadBenchmarkFleet(36, 200, 36, 124)
	for i, nct := range templates {
		nct.cacheFingerprint, nct.cacheFingerprintValid = uint64(i), true //nolint:gosec
	}
	standIns := [2][]*corev1.Pod{}
	for v := range standIns {
		for _, d := range daemons {
			p := d.DeepCopy()
			p.Namespace = "kube-system"
			p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: d.Name, UID: types.UID(d.Name), Controller: lo.ToPtr(true)}}
			p.Name = fmt.Sprintf("%s-%d", d.Name, v)
			p.Spec.NodeName = fmt.Sprintf("node-%d", v)
			standIns[v] = append(standIns[v], p)
		}
	}
	for _, bc := range []struct {
		name     string
		variants int
	}{{name: "swapping", variants: 2}, {name: "steady", variants: 1}} {
		b.Run(bc.name, func(b *testing.B) {
			cache := NewDaemonOverheadCacheWithGroupStore(NewDaemonOverheadGroupStore())
			i := 0
			for b.Loop() {
				pods := standIns[i%bc.variants]
				i++
				cache.updateDaemonSetGeneration(pods)
				buildDaemonOverheadGroups(ctx, cache, templates, pods)
			}
		})
	}
}
