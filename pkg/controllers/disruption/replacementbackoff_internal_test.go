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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func backoffCandidate(providerID string, podUIDs ...string) *Candidate {
	nc := &v1.NodeClaim{Status: v1.NodeClaimStatus{ProviderID: providerID}}
	c := &Candidate{
		StateNode: &state.StateNode{NodeClaim: nc},
		NodePool:  &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool", UID: "pool-uid", Generation: 1}},
	}
	for _, uid := range podUIDs {
		c.reschedulablePods = append(c.reschedulablePods, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid)}})
	}
	return c
}

func TestReplacementBackoffDuration(t *testing.T) {
	base := time.Minute
	for failures, want := range map[int]time.Duration{1: time.Minute, 2: 2 * time.Minute, 3: 4 * time.Minute, 5: 16 * time.Minute, 6: 16 * time.Minute, 100: 16 * time.Minute} {
		if got := replacementBackoffDuration(base, failures); got != want {
			t.Errorf("replacementBackoffDuration(%s, %d) = %s, want %s", base, failures, got, want)
		}
	}
}

func TestReplacementBackoffHolds(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	on := options.ToContext(context.Background(), test.Options(test.OptionsFields{ConsolidationReplacementFailureBackoff: &[]time.Duration{time.Minute}[0]}))
	off := options.ToContext(context.Background(), test.Options())
	b := NewReplacementBackoff(clk)
	c := backoffCandidate("a", "pod-1", "pod-2")

	b.RecordFailure(off, []*Candidate{c})
	if b.Holds(on, c) {
		t.Fatal("a failure recorded while the back-off is off must not hold the candidate")
	}
	b.RecordFailure(on, []*Candidate{c})
	if !b.Holds(on, c) {
		t.Fatal("expected the candidate to be held after a failed replacement")
	}
	if b.Holds(off, c) {
		t.Fatal("turning the back-off off must release every hold")
	}
	// The same pods in another order are the same replacement problem.
	if !b.Holds(on, backoffCandidate("a", "pod-2", "pod-1")) {
		t.Fatal("pod order must not change the fingerprint")
	}
	if b.Holds(on, backoffCandidate("b", "pod-1", "pod-2")) {
		t.Fatal("another node must not be held")
	}
	// A pod leaving the node is a different replacement problem: the hold is released for good.
	if b.Holds(on, backoffCandidate("a", "pod-1")) {
		t.Fatal("a candidate whose pods changed must be released")
	}
	if b.Holds(on, c) {
		t.Fatal("a released hold must not come back")
	}

	b.RecordFailure(on, []*Candidate{c})
	clk.Step(time.Minute)
	if b.Holds(on, c) {
		t.Fatal("expected the hold to end after the base back-off")
	}
	b.RecordFailure(on, []*Candidate{c})
	b.Forget([]*Candidate{c})
	if b.Holds(on, c) {
		t.Fatal("expected a forgotten candidate to be released")
	}
}

func TestReplacementBackoffPrunesQuietEntries(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	on := options.ToContext(context.Background(), test.Options(test.OptionsFields{ConsolidationReplacementFailureBackoff: &[]time.Duration{time.Minute}[0]}))
	b := NewReplacementBackoff(clk)
	b.RecordFailure(on, []*Candidate{backoffCandidate("gone")})
	clk.Step(2*maxReplacementBackoffMultiple*time.Minute + time.Second)
	// A lookup of any other candidate sweeps entries quiet for longer than twice the longest hold,
	// so a candidate that left the fleet does not need a later failure to be dropped.
	b.Holds(on, backoffCandidate("a"))
	if _, ok := b.entries["gone"]; ok {
		t.Fatal("expected an entry quiet for longer than twice the longest hold to be pruned")
	}
}

func TestReplacementBackoffReleasesResizedPods(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	on := options.ToContext(context.Background(), test.Options(test.OptionsFields{ConsolidationReplacementFailureBackoff: &[]time.Duration{time.Minute}[0]}))
	b := NewReplacementBackoff(clk)
	c := backoffCandidate("a", "pod-1")
	c.reschedulablePods[0].Spec.Containers = []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}}}
	b.RecordFailure(on, []*Candidate{c})
	if !b.Holds(on, c) {
		t.Fatal("expected the candidate to be held")
	}
	// The same pod resized in place needs a different replacement.
	resized := backoffCandidate("a", "pod-1")
	resized.reschedulablePods[0].Spec.Containers = []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}}}}
	if b.Holds(on, resized) {
		t.Fatal("a candidate whose pod was resized must be released")
	}
}
