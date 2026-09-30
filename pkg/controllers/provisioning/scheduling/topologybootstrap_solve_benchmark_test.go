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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

// BenchmarkSolveUnschedulableBootstrapBacklog solves 20 pods with a required self pod affinity on the zone key that
// fit no instance type, over 700 instance types offered in 20 zones. A pod that fits nowhere must not try every
// bootstrap domain.
func BenchmarkSolveUnschedulableBootstrapBacklog(b *testing.B) {
	ctx := karpopts.ToContext(injection.WithControllerName(context.Background(), "provisioner"), test.Options())
	instanceTypes := make([]*cloudprovider.InstanceType, 0, 700)
	for i := range 700 {
		offerings := make([]cloudprovider.Offering, 0, 40)
		for z := range 20 {
			for _, capacityType := range []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand} {
				offerings = append(offerings, cloudprovider.Offering{
					Available: true,
					Price:     float64(i+z) / 10,
					Requirements: scheduling.NewLabelRequirements(map[string]string{
						v1.CapacityTypeLabelKey:  capacityType,
						corev1.LabelTopologyZone: fmt.Sprintf("zone-%02d", z),
					}),
				})
			}
		}
		instanceTypes = append(instanceTypes, fake.NewInstanceType(fmt.Sprintf("type-%03d", i),
			fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(2 + i%64))}),
			fake.WithOfferings(offerings...)))
	}
	nodePool := test.NodePool()
	var pods []*corev1.Pod
	for i := range 20 {
		labels := map[string]string{"gang": fmt.Sprintf("gang-%d", i)}
		pods = append(pods, test.UnschedulablePod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: labels, UID: types.UID(fmt.Sprintf("gang-%d", i))},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1000")},
			},
			PodRequirements: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
				TopologyKey:   corev1.LabelTopologyZone,
			}},
		}))
	}
	cloudProvider := fake.NewCloudProvider()
	cloudProvider.InstanceTypes = instanceTypes
	kubeClient := fakecr.NewFakeClient()
	cluster := state.NewCluster(&clock.RealClock{}, kubeClient, cloudProvider)
	instanceTypesByPool := map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes}

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		topology, err := NewTopology(ctx, kubeClient, cluster, nil, []*v1.NodePool{nodePool}, instanceTypesByPool, pods)
		if err != nil {
			b.Fatalf("creating topology, %s", err)
		}
		scheduler := NewScheduler(ctx, kubeClient, []*v1.NodePool{nodePool}, cluster, nil, topology, instanceTypesByPool, nil,
			events.NewRecorder(&record.FakeRecorder{}), &clock.RealClock{}, nil, nil)
		b.StartTimer()
		results, err := scheduler.Solve(ctx, pods)
		if err != nil {
			b.Fatalf("solving, %s", err)
		}
		if len(results.PodErrors) != len(pods) {
			b.Fatalf("expected every pod to fail to schedule, got %d errors", len(results.PodErrors))
		}
	}
}
