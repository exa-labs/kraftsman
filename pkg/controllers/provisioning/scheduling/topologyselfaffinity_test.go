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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// Only a pod owning a pod affinity that selects itself can bootstrap a domain; every other pod, including one with an
// affinity to other pods or with anti-affinity only, takes the placement path that never prices domains.
func TestTopologyTracksSelfSelectingAffinity(t *testing.T) {
	ctx := karpopts.ToContext(injection.WithControllerName(context.Background(), "provisioner"), test.Options())
	selfLabels := map[string]string{"gang": "self"}
	zoneTerm := func(labels map[string]string) []corev1.PodAffinityTerm {
		return []corev1.PodAffinityTerm{{LabelSelector: &metav1.LabelSelector{MatchLabels: labels}, TopologyKey: corev1.LabelTopologyZone}}
	}
	pod := func(uid string, options test.PodOptions) *corev1.Pod {
		options.UID = types.UID(uid)
		return test.UnschedulablePod(options)
	}
	self := pod("self", test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: selfLabels}, PodRequirements: zoneTerm(selfLabels)})
	other := pod("other", test.PodOptions{PodRequirements: zoneTerm(map[string]string{"app": "elsewhere"})})
	anti := pod("anti", test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: selfLabels}, PodAntiRequirements: zoneTerm(selfLabels)})
	plain := pod("plain", test.PodOptions{})
	pods := []*corev1.Pod{self, other, anti, plain}

	instanceTypes := []*cloudprovider.InstanceType{fake.NewInstanceType("default")}
	nodePool := test.NodePool()
	kubeClient := fakecr.NewFakeClient()
	cluster := state.NewCluster(&clock.RealClock{}, kubeClient, fake.NewCloudProvider())
	topology, err := NewTopology(ctx, kubeClient, cluster, nil, []*v1.NodePool{nodePool}, map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes}, pods)
	if err != nil {
		t.Fatalf("creating topology: %v", err)
	}
	for _, tc := range []struct {
		pod  *corev1.Pod
		want bool
	}{{self, true}, {other, false}, {anti, false}, {plain, false}} {
		if got := topology.HasSelfSelectingAffinity(tc.pod); got != tc.want {
			t.Errorf("HasSelfSelectingAffinity(%s) = %v, want %v", tc.pod.UID, got, tc.want)
		}
	}

	// Relaxing the pod's affinity away re-registers it without one.
	self.Spec.Affinity = nil
	if err := topology.Update(ctx, self); err != nil {
		t.Fatalf("updating topology: %v", err)
	}
	if topology.HasSelfSelectingAffinity(self) {
		t.Errorf("HasSelfSelectingAffinity(self) after dropping its affinity = true, want false")
	}
}
