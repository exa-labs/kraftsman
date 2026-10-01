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

// Benchmarks for bootstrapping a self-selecting pod affinity over a large instance type universe: pricing the candidate
// domains, and solving a backlog of bootstrapping pods that fit no instance type.

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

// bootstrapBenchmarkInstanceTypes returns 700 instance types of 2 to 65 CPUs, each offered in 20 zones as spot and
// on-demand.
func bootstrapBenchmarkInstanceTypes() []*cloudprovider.InstanceType {
	instanceTypes := make([]*cloudprovider.InstanceType, 0, 700)
	for i := range 700 {
		offerings := make([]cloudprovider.Offering, 0, 40)
		for z := range 20 {
			zone := fmt.Sprintf("zone-%02d", z)
			offerings = append(offerings,
				offeringInZone(v1.CapacityTypeSpot, zone, float64(i+z)/10, true),
				offeringInZone(v1.CapacityTypeOnDemand, zone, float64(i+z)/5, true),
			)
		}
		instanceTypes = append(instanceTypes, fake.NewInstanceType(fmt.Sprintf("type-%03d", i),
			fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(2 + i%64))}),
			fake.WithOfferings(offerings...)))
	}
	return instanceTypes
}

// BenchmarkCheapestPriceByDomain prices the zone key for a pod that fits most of the instance types.
func BenchmarkCheapestPriceByDomain(b *testing.B) {
	instanceTypes := bootstrapBenchmarkInstanceTypes()
	groups := []DaemonOverheadGroup{{
		InstanceTypes:  instanceTypes,
		DaemonOverhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		HostPortUsage:  scheduling.NewHostPortUsage(),
	}}
	requests := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}
	pod := gangPod()
	always := func(*cloudprovider.Offering) bool { return true }
	for _, tc := range []struct {
		name         string
		requirements scheduling.Requirements
	}{
		{"three zones on-demand", scheduling.NewRequirements(
			scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "zone-00", "zone-01", "zone-02"),
		)},
		{"unrestricted", scheduling.NewRequirements()},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				cheapestPriceByDomain(instanceTypes, groups, pod, tc.requirements, requests, corev1.LabelTopologyZone, always)
			}
		})
	}
}

// BenchmarkSolveUnschedulableBootstrapBacklog solves 20 pods with a required self pod affinity on the zone key that
// fit no instance type. A pod that fits nowhere must not try every bootstrap domain.
func BenchmarkSolveUnschedulableBootstrapBacklog(b *testing.B) {
	ctx := karpopts.ToContext(injection.WithControllerName(context.Background(), "provisioner"), test.Options())
	instanceTypes := bootstrapBenchmarkInstanceTypes()
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
