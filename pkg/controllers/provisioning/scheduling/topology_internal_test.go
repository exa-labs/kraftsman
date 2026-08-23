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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newForTopologies must not write through to the pod: pods are shared across scheduler
// constructions, so folding matchLabelKeys into the pod's own selector would grow it on
// every call and change its string form, which topology caches key on.
func TestNewForTopologiesDoesNotMutatePodSelector(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Labels: map[string]string{"app": "web", "pod-template-hash": "abc"}},
		Spec: corev1.PodSpec{
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				TopologyKey:       corev1.LabelHostname,
				WhenUnsatisfiable: corev1.DoNotSchedule,
				MaxSkew:           1,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				MatchLabelKeys:    []string{"pod-template-hash"},
			}},
		},
	}
	topology := &Topology{
		domainGroups: map[string]TopologyDomainGroup{corev1.LabelHostname: NewTopologyDomainGroup()},
	}

	var groups []*TopologyGroup
	for range 3 {
		groups = topology.newForTopologies(pod)
	}

	if got := len(pod.Spec.TopologySpreadConstraints[0].LabelSelector.MatchExpressions); got != 0 {
		t.Fatalf("pod selector gained %d match expressions", got)
	}
	if len(groups) != 1 {
		t.Fatalf("expected one topology group, got %d", len(groups))
	}
	exprs := groups[0].rawSelector.MatchExpressions
	if len(exprs) != 1 || exprs[0].Key != "pod-template-hash" || len(exprs[0].Values) != 1 || exprs[0].Values[0] != "abc" {
		t.Fatalf("expected selector to carry exactly one matchLabelKeys requirement, got %+v", exprs)
	}
}

func TestNewForTopologiesNilSelectorWithMatchLabelKeys(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Labels: map[string]string{"pod-template-hash": "abc"}},
		Spec: corev1.PodSpec{
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				TopologyKey:       corev1.LabelHostname,
				WhenUnsatisfiable: corev1.DoNotSchedule,
				MaxSkew:           1,
				MatchLabelKeys:    []string{"pod-template-hash"},
			}},
		},
	}
	topology := &Topology{
		domainGroups: map[string]TopologyDomainGroup{corev1.LabelHostname: NewTopologyDomainGroup()},
	}
	groups := topology.newForTopologies(pod)
	if len(groups) != 1 || groups[0].rawSelector == nil || len(groups[0].rawSelector.MatchExpressions) != 1 {
		t.Fatalf("expected a selector built from matchLabelKeys, got %+v", groups)
	}

	// With none of the keys present on the pod the selector must stay nil: an empty selector
	// would match every pod in the namespace rather than none.
	pod.Labels = map[string]string{}
	groups = topology.newForTopologies(pod)
	if len(groups) != 1 || groups[0].rawSelector != nil {
		t.Fatalf("expected a nil selector when no matchLabelKeys are present, got %+v", groups[0].rawSelector)
	}
}
