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

// Tests for the DaemonSet generation that keys every daemon-derived cache entry: it has to move when
// anything the daemon caches read from a DaemonSet's pods moves, and hold still when the cluster
// merely swaps which live pod stands in for a DaemonSet, which happens every time a node joins.

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
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// liveDaemonSetPod returns a running pod of DaemonSet "node-agent" the way the DaemonSet controller and
// the API server create it for nodeName: a generated name and UID, a controller reference, the node
// binding in spec.nodeName and in a metadata.name matchFields term, and a projected service account
// token volume whose name is generated per pod.
func liveDaemonSetPod(podName, nodeName, tokenVolume string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system", Name: podName, UID: types.UID(podName + "-uid"), ResourceVersion: "7",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "node-agent", UID: "ds-uid", Controller: lo.ToPtr(true)}},
		},
		Spec: corev1.PodSpec{
			NodeName:     nodeName,
			NodeSelector: map[string]string{corev1.LabelOSStable: "linux"},
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64"}}},
					MatchFields:      []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName}}},
				}},
			}}},
			Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			InitContainers: []corev1.Container{{
				Name:         "init",
				VolumeMounts: []corev1.VolumeMount{{Name: tokenVolume, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount"}},
			}},
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
				Name: "agent", RestartCount: 3, ContainerID: "containerd://" + podName,
				AllocatedResources: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
				},
			}},
		},
	}
}

func mustGeneration(t *testing.T, pods ...*corev1.Pod) string {
	t.Helper()
	generation, ok := daemonSetPodsGeneration(pods)
	if !ok {
		t.Fatal("expected a valid DaemonSet generation")
	}
	return generation
}

func TestDaemonSetGenerationIgnoresWhichPodStandsIn(t *testing.T) {
	onFirstNode := liveDaemonSetPod("node-agent-x7k2p", "node-a", "kube-api-access-88mrr")
	onSecondNode := liveDaemonSetPod("node-agent-q9w4z", "node-b", "kube-api-access-q6b2z")
	onSecondNode.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debugger"}}}
	if mustGeneration(t, onFirstNode) != mustGeneration(t, onSecondNode) {
		t.Fatal("expected two pods of one DaemonSet that differ only in per-pod fields to share a generation")
	}
	// A freshly created stand-in that has not reported its resources yet reads the same requests.
	pending := liveDaemonSetPod("node-agent-m3n8v", "node-c", "kube-api-access-zz9k1")
	pending.Status = corev1.PodStatus{Phase: corev1.PodPending}
	if mustGeneration(t, onFirstNode) != mustGeneration(t, pending) {
		t.Fatal("expected a pending stand-in whose effective requests match the running one to share its generation")
	}
}

func TestDaemonSetGenerationTracksEveryReadInput(t *testing.T) {
	base := liveDaemonSetPod("node-agent-x7k2p", "node-a", "kube-api-access-88mrr")
	for name, mutate := range map[string]func(*corev1.Pod){
		"cpu request": func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("200m")
		},
		"limit": func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
		},
		"sidecar init container": func(p *corev1.Pod) {
			p.Spec.InitContainers = append(p.Spec.InitContainers, corev1.Container{Name: "sidecar", RestartPolicy: lo.ToPtr(corev1.ContainerRestartPolicyAlways),
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m")}}})
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
		"node selector": func(p *corev1.Pod) { p.Spec.NodeSelector[corev1.LabelTopologyZone] = "zone-1" },
		"affinity expression": func(p *corev1.Pod) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"arm64"}
		},
		"affinity term count": func(p *corev1.Pod) {
			terms := &p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
			*terms = append(*terms, corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelOSStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"linux"}}}})
		},
		"host port":      func(p *corev1.Pod) { p.Spec.Containers[0].Ports[0].HostPort = 9200 },
		"resource claim": func(p *corev1.Pod) { p.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "gpu"}} },
		// An in-place resize the kubelet allocated or actuated above the spec raises the effective requests.
		"status allocated resources": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].AllocatedResources[corev1.ResourceCPU] = resource.MustParse("300m")
		},
		"status actuated resources": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("300m")
		},
		"namespace":        func(p *corev1.Pod) { p.Namespace = "monitoring" },
		"owning daemonset": func(p *corev1.Pod) { p.OwnerReferences[0].Name = "other-agent" },
		"no controller":    func(p *corev1.Pod) { p.OwnerReferences = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base.DeepCopy()
			mutate(changed)
			if mustGeneration(t, base) == mustGeneration(t, changed) {
				t.Fatalf("expected a change to the %s to move the DaemonSet generation", name)
			}
		})
	}
}

// An infeasible in-place resize leaves the effective requests at what the kubelet allocated, below a spec
// that was raised. The resize condition alone changes what the caches read, so it must move the generation.
func TestDaemonSetGenerationTracksAnInfeasibleResize(t *testing.T) {
	resized := liveDaemonSetPod("node-agent-x7k2p", "node-a", "kube-api-access-88mrr")
	resized.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("400m")
	infeasible := resized.DeepCopy()
	infeasible.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodResizePending, Status: corev1.ConditionTrue, Reason: corev1.PodReasonInfeasible}}
	resizedRequests, infeasibleRequests := resources.Ceiling(resized).Requests, resources.Ceiling(infeasible).Requests
	if resizedRequests.Cpu().Cmp(*infeasibleRequests.Cpu()) == 0 {
		t.Fatal("test setup: expected the resize condition to change the effective requests")
	}
	if mustGeneration(t, resized) == mustGeneration(t, infeasible) {
		t.Fatal("expected an infeasible resize to move the DaemonSet generation")
	}
}

func TestDaemonSetGenerationDoesNotMutateThePod(t *testing.T) {
	pod := liveDaemonSetPod("node-agent-x7k2p", "node-a", "kube-api-access-88mrr")
	pod.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debugger"}}}
	mustGeneration(t, pod)
	if pod.Spec.NodeName != "node-a" || len(pod.Spec.Volumes) != 1 || len(pod.Spec.EphemeralContainers) != 1 ||
		len(pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields) != 1 ||
		len(pod.Spec.Containers[0].VolumeMounts) != 1 || len(pod.Spec.InitContainers[0].VolumeMounts) != 1 {
		t.Fatal("expected fingerprinting to leave the shared DaemonSet pod untouched")
	}
}

// A node joining makes its fresh DaemonSet pods the newest ones, which is all that changes between two
// constructions. The pass-scoped entries and the cross-pass overhead groups must survive it.
func TestDaemonOverheadCacheSurvivesNewestPodSwap(t *testing.T) {
	store := NewDaemonOverheadGroupStore()
	cache := NewDaemonOverheadCacheWithGroupStore(store)
	nct := &NodeClaimTemplate{NodePoolName: "default", cacheFingerprint: 7, cacheFingerprintValid: true}
	groups := []DaemonOverheadGroup{{DaemonOverhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}}

	cache.updateDaemonSetGeneration([]*corev1.Pod{liveDaemonSetPod("node-agent-x7k2p", "node-a", "kube-api-access-88mrr")})
	cache.setDaemonRequests("node-key", corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")})
	cache.setOverheadGroups(nct, groups)

	cache.updateDaemonSetGeneration([]*corev1.Pod{liveDaemonSetPod("node-agent-q9w4z", "node-b", "kube-api-access-q6b2z")})
	if _, ok := cache.daemonRequests("node-key"); !ok {
		t.Fatal("expected pass-scoped daemon requests to survive a newest-pod swap")
	}
	if _, outcome, ok := cache.overheadGroups(nct); !ok || outcome != cacheOutcomeHit {
		t.Fatalf("expected pass-scoped overhead groups to survive a newest-pod swap, got outcome %q", outcome)
	}

	next := NewDaemonOverheadCacheWithGroupStore(store)
	next.updateDaemonSetGeneration([]*corev1.Pod{liveDaemonSetPod("node-agent-m3n8v", "node-c", "kube-api-access-zz9k1")})
	if _, outcome, ok := next.overheadGroups(nct); !ok || outcome != cacheOutcomeHitCrossPass {
		t.Fatalf("expected the next pass to be served from the group store after a newest-pod swap, got outcome %q", outcome)
	}
}

// BenchmarkDaemonOverheadGroupsAcrossStandInSwaps measures one pass building daemon overhead groups while
// the pods standing in for its DaemonSets change between constructions, as they do whenever a node joins:
// every construction alternates between two running pods of each DaemonSet.
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
