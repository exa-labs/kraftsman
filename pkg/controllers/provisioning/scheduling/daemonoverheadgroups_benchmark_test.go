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
	templates, daemons := daemonOverheadBenchmarkFleet(36, 200, 36, 124)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildDaemonOverheadGroups(ctx, nil, templates, daemons)
	}
}
