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

// Tests for the evidence the scheduler records when a pod leaving a deleting node finds no existing node.

package scheduling

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// existingStateNode is a ready node of nodePool with cpu cores allocatable and the given taints.
func existingStateNode(name, nodePool, cpu string, taints ...corev1.Taint) *state.StateNode {
	n := state.NewNode()
	n.Node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(name),
			Name:            name,
			ResourceVersion: "1",
			Labels: map[string]string{
				v1.NodePoolLabelKey:      nodePool,
				corev1.LabelHostname:     name,
				corev1.LabelTopologyZone: "test-zone-1",
			},
		},
		Spec: corev1.NodeSpec{Taints: taints},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse("16Gi"),
			corev1.ResourcePods:   resource.MustParse("100"),
		}},
	}
	return n
}

// solveFromDeletingNode solves one two-core pod bound to nodeName, with "draining" marked as deleting, against a
// one-core node and a tainted four-core node, and returns the scheduler and the pod.
func solveFromDeletingNode(t *testing.T, nodeName string) (*Scheduler, *corev1.Pod) {
	t.Helper()
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	client := fakecr.NewFakeClient()
	provider := fake.NewCloudProvider()
	instanceTypes := fake.InstanceTypes(5)
	provider.InstanceTypes = instanceTypes
	nodePool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool"}})
	stateNodes := []*state.StateNode{
		existingStateNode("small", nodePool.Name, "1"),
		existingStateNode("tainted", nodePool.Name, "4", corev1.Taint{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "moving", Namespace: "default", UID: "moving"},
		Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{
			Name:      "main",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	cluster := state.NewCluster(clock.RealClock{}, client, provider)
	byNodePool := map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes}
	topology, err := NewTopology(ctx, client, cluster, stateNodes, []*v1.NodePool{nodePool}, byNodePool, []*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("creating topology: %v", err)
	}
	s := NewScheduler(ctx, client, []*v1.NodePool{nodePool}, cluster, stateNodes, topology, byNodePool, nil,
		events.NewRecorder(&record.FakeRecorder{}), clock.RealClock{}, nil, nil)
	s.deletingNodeNames = sets.New("draining")
	results, err := s.Solve(ctx, []*corev1.Pod{pod})
	if err != nil || len(results.PodErrors) != 0 || len(results.NewNodeClaims) != 1 {
		t.Fatalf("expected the pod on one new NodeClaim, got %d NodeClaims, errors %v, err %v", len(results.NewNodeClaims), results.PodErrors, err)
	}
	return s, pod
}

func TestPodFromDeletingNodeRecordsWhyExistingNodesRejectedIt(t *testing.T) {
	s, pod := solveFromDeletingNode(t, "draining")
	rejections, ok := s.existingNodeRejections[pod.UID]
	if !ok {
		t.Fatal("a pod from a deleting node that needs new capacity must record why the existing nodes rejected it")
	}
	if !strings.HasPrefix(rejections, "1 existing node(s)") || !strings.Contains(rejections, "small: exceeds node resources") {
		t.Fatalf("expected the one-core node rejected for resources, got %q", rejections)
	}
	if strings.Contains(rejections, "tainted") {
		t.Fatalf("a node whose taints the pod does not tolerate is not a candidate and must not be listed, got %q", rejections)
	}
}

func TestPodFromALiveNodeRecordsNothing(t *testing.T) {
	s, pod := solveFromDeletingNode(t, "live")
	if _, ok := s.existingNodeRejections[pod.UID]; ok {
		t.Fatal("only pods from deleting nodes record existing-node rejections")
	}
}
