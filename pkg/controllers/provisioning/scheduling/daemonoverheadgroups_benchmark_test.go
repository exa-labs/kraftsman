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

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

const benchmarkSandboxTaintKey = "example.com/sandbox"

// daemonOverheadBenchmarkFleet models a cluster whose DaemonSets are mostly scoped to a tainted pool: most
// NodePools reject most daemon pods on the taint alone, independent of instance type.
func daemonOverheadBenchmarkFleet(pools, instanceTypes, generalDaemons, scopedDaemons int) ([]*NodeClaimTemplate, []*corev1.Pod) {
	its := fake.InstanceTypes(instanceTypes)
	templates := make([]*NodeClaimTemplate, pools)
	for i := range templates {
		name := fmt.Sprintf("pool-%d", i)
		nct := &NodeClaimTemplate{
			NodePoolName:        name,
			InstanceTypeOptions: its,
			Requirements:        scheduling.NewRequirements(scheduling.NewRequirement(v1.NodePoolLabelKey, corev1.NodeSelectorOpIn, name)),
		}
		// The first two pools are the tainted sandbox pools the scoped daemons target.
		if i < 2 {
			nct.Spec.Taints = []corev1.Taint{{Key: benchmarkSandboxTaintKey, Value: "true", Effect: corev1.TaintEffectNoSchedule}}
		}
		templates[i] = nct
	}
	var daemons []*corev1.Pod
	for i := range generalDaemons {
		daemons = append(daemons, daemonPod(fmt.Sprintf("general-%d", i), "50m"))
	}
	sandboxToleration := corev1.Toleration{Key: benchmarkSandboxTaintKey, Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule}
	for i := range scopedDaemons {
		p := daemonPod(fmt.Sprintf("scoped-%d", i), "100m", in(v1.NodePoolLabelKey, "pool-0", "pool-1"))
		p.Spec.Tolerations = []corev1.Toleration{sandboxToleration}
		daemons = append(daemons, p)
	}
	return templates, daemons
}

func BenchmarkBuildDaemonOverheadGroups(b *testing.B) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	for _, bc := range []struct {
		name                   string
		general, scopedDaemons int
	}{
		// Most daemons rejected on the taint alone: the case the template-level pre-filter targets.
		{name: "taint-scoped", general: 36, scopedDaemons: 124},
		// Every daemon tolerated everywhere: the pre-filter prunes nothing.
		{name: "unscoped", general: 160},
	} {
		templates, daemons := daemonOverheadBenchmarkFleet(36, 200, bc.general, bc.scopedDaemons)
		b.Run(bc.name+"/uncached", func(b *testing.B) {
			for b.Loop() {
				buildDaemonOverheadGroups(ctx, nil, templates, daemons)
			}
		})
		// A later pass served from the group store: only the rebinding to the pass's instance types remains.
		b.Run(bc.name+"/cross-pass-hit", func(b *testing.B) {
			for i, nct := range templates {
				nct.cacheFingerprint, nct.cacheFingerprintValid = uint64(i), true //nolint:gosec
			}
			store := NewDaemonOverheadGroupStore()
			warm := NewDaemonOverheadCacheWithGroupStore(store)
			warm.updateDaemonSetGeneration(daemons)
			buildDaemonOverheadGroups(ctx, warm, templates, daemons)
			for b.Loop() {
				pass := NewDaemonOverheadCacheWithGroupStore(store)
				pass.updateDaemonSetGeneration(daemons)
				buildDaemonOverheadGroups(ctx, pass, templates, daemons)
			}
			for _, nct := range templates {
				nct.cacheFingerprintValid = false
			}
		})
	}
}
