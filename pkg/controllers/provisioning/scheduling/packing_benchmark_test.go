//go:build test_performance

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

// Benchmarks for Solve under each packing policy. Every iteration solves the batch with a freshly built scheduler,
// since Solve leaves its in-flight NodeClaims behind and a reused scheduler would place later iterations onto them.
//
//	go test -tags=test_performance -run=XXX -bench=BenchmarkPackingPolicy -benchmem -count=6

package scheduling_test

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// packingLayout names the NodePools a benchmark schedules against.
type packingLayout string

const (
	// layoutBinpack is a single binpack NodePool.
	layoutBinpack packingLayout = "binpack"
	// layoutBinpackBesideMarginal adds a tainted marginal-cost NodePool no pod tolerates, so every pod still lands on
	// the binpack NodePool but the scheduler runs the priced placement path for all of them.
	layoutBinpackBesideMarginal packingLayout = "binpack-beside-marginal"
	// layoutMarginal is a single marginal-cost NodePool.
	layoutMarginal packingLayout = "marginal"
)

func BenchmarkPackingPolicy(b *testing.B) {
	for _, podCount := range []int{500, 2000} {
		pods := makeDiversePods(podCount)
		for _, layout := range []packingLayout{layoutBinpack, layoutBinpackBesideMarginal, layoutMarginal} {
			b.Run(fmt.Sprintf("pods=%d/%s", podCount, layout), func(b *testing.B) {
				benchmarkPackingPolicy(b, pods, layout)
			})
		}
	}
}

func benchmarkPackingPolicy(b *testing.B, pods []*corev1.Pod, layout packingLayout) {
	ctx := options.ToContext(injection.WithControllerName(context.Background(), "provisioner"), test.Options())
	nodeClaims := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s, err := setupPackingScheduler(ctx, pods, layout)
		if err != nil {
			b.Fatalf("creating scheduler, %s", err)
		}
		b.StartTimer()
		results, err := s.Solve(ctx, pods)
		if err != nil {
			b.Fatalf("solving, %s", err)
		}
		if len(results.PodErrors) > 0 {
			b.Fatalf("expected all pods to schedule, got %d pods that didn't", len(results.PodErrors))
		}
		nodeClaims = len(results.NewNodeClaims)
	}
	b.ReportMetric(float64(nodeClaims), "nodeclaims")
}

func setupPackingScheduler(ctx context.Context, pods []*corev1.Pod, layout packingLayout) (*scheduling.Scheduler, error) {
	limits := v1.Limits{
		corev1.ResourceCPU:    resource.MustParse("10000000"),
		corev1.ResourceMemory: resource.MustParse("10000000Gi"),
	}
	general := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Limits: limits}})
	nodePools := []*v1.NodePool{general}
	switch layout {
	case layoutBinpackBesideMarginal:
		accelerated := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{
			Limits: limits,
			Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
				Taints: []corev1.Taint{{Key: "accelerated", Effect: corev1.TaintEffectNoSchedule}},
			}},
		}})
		accelerated.Annotations = map[string]string{v1.NodePoolPackingPolicyAnnotationKey: string(scheduling.PackingPolicyMarginalCost)}
		nodePools = append(nodePools, accelerated)
	case layoutMarginal:
		general.Annotations = map[string]string{v1.NodePoolPackingPolicyAnnotationKey: string(scheduling.PackingPolicyMarginalCost)}
	}

	instanceTypes := fake.InstanceTypes(400)
	byNodePool := map[string][]*cloudprovider.InstanceType{}
	for _, np := range nodePools {
		byNodePool[np.Name] = instanceTypes
	}
	provider := fake.NewCloudProvider()
	provider.InstanceTypes = instanceTypes
	client := fakecr.NewFakeClient()
	realClock := &clock.RealClock{}
	clusterState := state.NewCluster(realClock, client, provider)
	topology, err := scheduling.NewTopology(ctx, client, clusterState, nil, nodePools, byNodePool, pods)
	if err != nil {
		return nil, fmt.Errorf("creating topology, %w", err)
	}
	return scheduling.NewScheduler(ctx, client, nodePools, clusterState, nil, topology, byNodePool, nil,
		events.NewRecorder(&record.FakeRecorder{}), realClock, nil, nil, scheduling.NumConcurrentReconciles(5)), nil
}
