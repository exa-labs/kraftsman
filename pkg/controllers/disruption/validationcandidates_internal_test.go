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

// Tests and benchmarks for rebuilding a command's candidates during validation: currentCandidates
// must return exactly what GetCandidates followed by mapCandidates returns, at a cost that scales
// with the command rather than the cluster.
package disruption

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// validationFixture is a cluster of initialized, consolidatable nodes, each running podsPerNode
// ReplicaSet pods, behind a fake API client and a synced cluster state.
type validationFixture struct {
	ctx       context.Context
	validator *ConsolidationValidator
	// candidates are the cluster's candidates as a pass would have built them.
	candidates []*Candidate
	nodeClaims []*v1.NodeClaim
	nodes      []*corev1.Node
}

// indexedPodClient answers pod lists by node name from a precomputed index, deep-copying each pod,
// the way the informer cache serves them in a running controller. The fake client would scan
// every pod for each node, which makes building candidates for the whole cluster quadratic and
// overstates what the old path cost.
type indexedPodClient struct {
	client.Client
	podsByNode map[string][]*corev1.Pod
}

func (c *indexedPodClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	podList, ok := list.(*corev1.PodList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	listOpts := (&client.ListOptions{}).ApplyOptions(opts)
	if listOpts.FieldSelector == nil {
		return c.Client.List(ctx, list, opts...)
	}
	nodeName, found := listOpts.FieldSelector.RequiresExactMatch("spec.nodeName")
	if !found {
		return c.Client.List(ctx, list, opts...)
	}
	podList.Items = lo.Map(c.podsByNode[nodeName], func(p *corev1.Pod, _ int) corev1.Pod { return *p.DeepCopy() })
	return nil
}

func newValidationFixture(tb testing.TB, nodeCount, podsPerNode int) *validationFixture {
	tb.Helper()
	ctx := options.ToContext(context.Background(), test.Options())
	cloudProvider := fake.NewCloudProvider()
	nodePool := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
		ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
		ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		Budgets:             []v1.Budget{{Nodes: "100%"}},
	}}})
	instanceTypes, err := cloudProvider.GetInstanceTypes(ctx, nodePool)
	if err != nil || len(instanceTypes) == 0 {
		tb.Fatalf("listing instance types: %v", err)
	}
	it := instanceTypes[0]
	offering := it.Offerings[0]
	nodeClaims, nodes := test.NodeClaimsAndNodes(nodeCount, v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			corev1.LabelInstanceTypeStable: it.Name,
			v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
			corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			v1.NodeRegisteredLabelKey:      "true",
			v1.NodeInitializedLabelKey:     "true",
		}},
		Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:  resource.MustParse("64"),
			corev1.ResourcePods: resource.MustParse("110"),
		}},
	})
	objects := []client.Object{nodePool}
	podsByNode := map[string][]*corev1.Pod{}
	for i := range nodeClaims {
		for _, cond := range []string{v1.ConditionTypeLaunched, v1.ConditionTypeRegistered, v1.ConditionTypeInitialized, v1.ConditionTypeConsolidatable} {
			nodeClaims[i].StatusConditions().SetTrue(cond)
		}
		objects = append(objects, nodeClaims[i], nodes[i])
		for j := range podsPerNode {
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("pod-%d-%d", i, j),
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs-uid", Controller: lo.ToPtr(true),
					}},
				},
				NodeName:             nodes[i].Name,
				Phase:                corev1.PodRunning,
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}},
			})
			objects = append(objects, pod)
			podsByNode[nodes[i].Name] = append(podsByNode[nodes[i].Name], pod)
		}
	}
	kubeClient := &indexedPodClient{
		Client: fakecr.NewClientBuilder().
			WithObjects(objects...).
			WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
			Build(),
		podsByNode: podsByNode,
	}
	clk := clocktesting.NewFakeClock(time.Now())
	cluster := state.NewCluster(clk, kubeClient, cloudProvider)
	for i := range nodeClaims {
		cluster.UpdateNodeClaim(nodeClaims[i])
		if err := cluster.UpdateNode(ctx, nodes[i]); err != nil {
			tb.Fatalf("updating node: %v", err)
		}
	}
	recorder := events.NewRecorder(&record.FakeRecorder{})
	queue := NewQueue(kubeClient, recorder, cluster, clk, nil)
	c := MakeConsolidation(clk, cluster, kubeClient, nil, cloudProvider, recorder, queue)
	validator := NewSingleConsolidationValidator(c)
	candidates, err := GetCandidates(ctx, cluster, kubeClient, recorder, clk, cloudProvider, validator.filter, GracefulDisruptionClass, queue)
	if err != nil {
		tb.Fatalf("building candidates: %v", err)
	}
	if len(candidates) != nodeCount {
		tb.Fatalf("fixture built %d candidates, want %d", len(candidates), nodeCount)
	}
	return &validationFixture{ctx: ctx, validator: validator, candidates: candidates, nodeClaims: nodeClaims, nodes: nodes}
}

// allNodesThenMap is how validation rebuilt a command's candidates before currentCandidates.
func (f *validationFixture) allNodesThenMap(proposed []*Candidate) ([]*Candidate, error) {
	v := f.validator
	all, err := GetCandidates(f.ctx, v.cluster, v.kubeClient, v.recorder, v.clock, v.cloudProvider, v.filter, GracefulDisruptionClass, v.queue)
	if err != nil {
		return nil, err
	}
	return mapCandidates(proposed, all), nil
}

func (f *validationFixture) current(proposed []*Candidate) ([]*Candidate, error) {
	v := f.validator
	return currentCandidates(f.ctx, v.cluster, v.kubeClient, v.recorder, v.clock, v.cloudProvider, v.filter, GracefulDisruptionClass, v.queue, proposed)
}

func candidateNames(candidates []*Candidate) []string {
	names := lo.Map(candidates, func(c *Candidate, _ int) string { return c.Name() })
	slices.Sort(names)
	return names
}

// churn changes two proposed nodes between the pass and its validation: the first stops being
// consolidatable and the second gains the do-not-disrupt annotation.
func (f *validationFixture) churn(t *testing.T, notConsolidatable, doNotDisrupt string) {
	t.Helper()
	for i := range f.nodes {
		switch f.nodes[i].Name {
		case notConsolidatable:
			f.nodeClaims[i].StatusConditions().SetFalse(v1.ConditionTypeConsolidatable, "test", "test")
			f.validator.cluster.UpdateNodeClaim(f.nodeClaims[i])
		case doNotDisrupt:
			f.nodes[i].Annotations = lo.Assign(f.nodes[i].Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
			if err := f.validator.cluster.UpdateNode(f.ctx, f.nodes[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCurrentCandidatesMatchesGetCandidates(t *testing.T) {
	f := newValidationFixture(t, 20, 3)
	proposed := f.candidates[:4]
	// Both churned nodes must drop out exactly as before.
	f.churn(t, proposed[0].Name(), proposed[1].Name())
	if names := lo.Uniq(candidateNames(proposed)); len(names) != 4 {
		t.Fatalf("expected 4 distinct proposed candidates, got %v", names)
	}

	want, err := f.allNodesThenMap(proposed)
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.current(proposed)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(candidateNames(got), candidateNames(want)) {
		t.Fatalf("currentCandidates = %v, GetCandidates+mapCandidates = %v", candidateNames(got), candidateNames(want))
	}
	if len(got) != 2 {
		t.Fatalf("expected the two unchanged candidates to survive, got %v", candidateNames(got))
	}
	for _, c := range got {
		if len(c.reschedulablePods) != 3 {
			t.Fatalf("candidate %s rebuilt with %d reschedulable pods, want 3", c.Name(), len(c.reschedulablePods))
		}
	}
}

// BenchmarkValidationCandidates measures rebuilding one single-node command's candidate on a
// 1,000-node cluster, the way validation does twice per command.
func BenchmarkValidationCandidates(b *testing.B) {
	f := newValidationFixture(b, 1000, 10)
	proposed := f.candidates[:1]
	b.Run("all-nodes", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := f.allNodesThenMap(proposed); err != nil || len(got) != 1 {
				b.Fatalf("got %d candidates, err %v", len(got), err)
			}
		}
	})
	b.Run("command-nodes", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := f.current(proposed); err != nil || len(got) != 1 {
				b.Fatalf("got %d candidates, err %v", len(got), err)
			}
		}
	})
}
