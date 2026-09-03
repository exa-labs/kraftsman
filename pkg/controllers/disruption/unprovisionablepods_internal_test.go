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

package disruption

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func namespacedTestPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
}

func podNames(pods []*corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}

func newVerdictCluster(clk clock.Clock) *state.Cluster {
	return state.NewCluster(clk, nil, fake.NewCloudProvider())
}

func TestPartitionUnprovisionablePodsExcludesFreshVerdictsOnly(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	cluster := newVerdictCluster(clk)
	ttl := 2 * time.Minute

	stale := namespacedTestPod("stale")
	cluster.MarkPodsUnprovisionable([]*corev1.Pod{stale})
	clk.Step(ttl)

	fresh := namespacedTestPod("fresh")
	cluster.MarkPodsUnprovisionable([]*corev1.Pod{fresh})
	clk.Step(ttl / 2)

	never := namespacedTestPod("never-decided")
	placed := namespacedTestPod("placed")
	cluster.MarkPodsUnprovisionable([]*corev1.Pod{placed})
	cluster.MarkPodSchedulingDecisions(context.Background(), nil, nil, map[string][]*corev1.Pod{"existing-node": {placed}})
	// An error the provisioner does not classify as pass-invariant is no verdict at all.
	errored := namespacedTestPod("errored")
	cluster.MarkPodSchedulingDecisions(context.Background(), map[*corev1.Pod]error{errored: errors.New("node limits have been exhausted for nodepool")}, nil, nil)

	backlog := []*corev1.Pod{never, fresh, stale, placed, errored}
	simulated, excluded := partitionUnprovisionablePods(cluster, clk, ttl, backlog)

	// A verdict exactly ttl old has expired; only the one younger than ttl excludes its pod.
	if got := podNames(simulated); !slices.Equal(got, []string{"never-decided", "stale", "placed", "errored"}) {
		t.Fatalf("unexpected simulated pods %v", got)
	}
	if got := podNames(excluded); !slices.Equal(got, []string{"fresh"}) {
		t.Fatalf("unexpected excluded pods %v", got)
	}
	if got := podNames(backlog); !slices.Equal(got, []string{"never-decided", "fresh", "stale", "placed", "errored"}) {
		t.Fatalf("input backlog was modified: %v", got)
	}
}

func TestPartitionUnprovisionablePodsDisabledByZeroTTL(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	cluster := newVerdictCluster(clk)

	pod := namespacedTestPod("fresh")
	cluster.MarkPodsUnprovisionable([]*corev1.Pod{pod})

	backlog := []*corev1.Pod{pod}
	simulated, excluded := partitionUnprovisionablePods(cluster, clk, 0, backlog)
	if len(excluded) != 0 || len(simulated) != 1 {
		t.Fatalf("expected a zero ttl to exclude nothing, got simulated=%v excluded=%v", podNames(simulated), podNames(excluded))
	}
	// The caller appends candidate pods to the returned slice, so it must not share the memoized backlog's backing array.
	if &simulated[0] == &backlog[0] {
		t.Fatalf("returned slice aliases the input backlog")
	}
}

func TestUnprovisionablePodsLogRateLimits(t *testing.T) {
	var l unprovisionablePodsLog
	now := time.Now()
	if !l.shouldLog(now) {
		t.Fatalf("the first report must be logged")
	}
	if l.shouldLog(now.Add(unprovisionablePodsLogInterval - time.Second)) {
		t.Fatalf("a report inside the interval must be suppressed")
	}
	if !l.shouldLog(now.Add(unprovisionablePodsLogInterval)) {
		t.Fatalf("a report at the interval must be logged")
	}
	if l.shouldLog(now.Add(unprovisionablePodsLogInterval + unprovisionablePodsLogInterval/2)) {
		t.Fatalf("the interval restarts from the last accepted report")
	}
}
