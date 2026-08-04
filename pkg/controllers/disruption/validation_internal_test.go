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
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	clocktesting "k8s.io/utils/clock/testing"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func simulatedNodeClaim(nodePool string, instanceTypes []string, requirements ...*scheduling.Requirement) *pscheduling.NodeClaim {
	nc := &pscheduling.NodeClaim{}
	nc.NodePoolName = nodePool
	nc.Requirements = scheduling.NewRequirements(requirements...)
	for _, name := range instanceTypes {
		nc.InstanceTypeOptions = append(nc.InstanceTypeOptions, &cloudprovider.InstanceType{Name: name})
	}
	return nc
}

func replacementFor(nc *pscheduling.NodeClaim) *Replacement {
	return &Replacement{NodeClaim: nc}
}

func TestReplacementsMatchSimulationInstanceTypeSubset(t *testing.T) {
	replacement := replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"}))
	if !replacementsMatchSimulation([]*Replacement{replacement}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.large", "m5.xlarge"}),
	}) {
		t.Fatal("expected subset instance types in the same nodepool to match")
	}
	if replacementsMatchSimulation([]*Replacement{replacement}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.xlarge"}),
	}) {
		t.Fatal("expected non-subset instance types to not match")
	}
}

func TestReplacementsMatchSimulationRejectsDifferentNodePool(t *testing.T) {
	replacement := replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"}))
	if replacementsMatchSimulation([]*Replacement{replacement}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-b", []string{"m5.large"}),
	}) {
		t.Fatal("expected same instance type names in a different nodepool to not match")
	}
}

func TestReplacementsMatchSimulationRejectsConflictingRequirements(t *testing.T) {
	zone := func(zones ...string) *scheduling.Requirement {
		return scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zones...)
	}
	capacityType := func(ct string) *scheduling.Requirement {
		return scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, ct)
	}
	replacement := replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"}, zone("zone-1"), capacityType(v1.CapacityTypeSpot)))

	if !replacementsMatchSimulation([]*Replacement{replacement}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.large"}, zone("zone-1"), capacityType(v1.CapacityTypeSpot)),
	}) {
		t.Fatal("expected identical requirements to match")
	}
	if replacementsMatchSimulation([]*Replacement{replacement}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.large"}, zone("zone-2"), capacityType(v1.CapacityTypeSpot)),
	}) {
		t.Fatal("expected same instance type names with a conflicting zone requirement to not match")
	}
	// Partial overlap is not containment: a replacement allowed in {zone-1, zone-2} could launch in zone-1
	// even though the fresh simulation only allows {zone-2, zone-3}.
	partialOverlap := replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"}, zone("zone-1", "zone-2"), capacityType(v1.CapacityTypeSpot)))
	if replacementsMatchSimulation([]*Replacement{partialOverlap}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.large"}, zone("zone-2", "zone-3"), capacityType(v1.CapacityTypeSpot)),
	}) {
		t.Fatal("expected a replacement with partially-overlapping zones to not match")
	}
	// Containment in the other direction is fine: the replacement's zones are a subset of the fresh claim's.
	if !replacementsMatchSimulation([]*Replacement{partialOverlap}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.large"}, zone("zone-1", "zone-2", "zone-3"), capacityType(v1.CapacityTypeSpot)),
	}) {
		t.Fatal("expected a replacement whose zones are contained in the fresh claim's zones to match")
	}
	if replacementsMatchSimulation([]*Replacement{replacement}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.large"}, zone("zone-1"), capacityType(v1.CapacityTypeOnDemand)),
	}) {
		t.Fatal("expected same instance type names with a conflicting capacity type requirement to not match")
	}
	// DoesNotExist is a distinct selector state: an old replacement requiring a label to be absent cannot
	// satisfy a fresh claim that now requires the label to be present.
	absent := replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"},
		scheduling.NewRequirement("team", corev1.NodeSelectorOpDoesNotExist)))
	if replacementsMatchSimulation([]*Replacement{absent}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.large"},
			scheduling.NewRequirement("team", corev1.NodeSelectorOpIn, "blue")),
	}) {
		t.Fatal("expected a DoesNotExist replacement requirement to not satisfy a fresh In requirement")
	}
}

func TestReplacementsMatchSimulationIgnoresReservationIDs(t *testing.T) {
	// each simulation run reserves whichever reserved offerings are available at that moment, so the
	// reservation ID requirement can legitimately differ between the command and the fresh simulation
	replacement := replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"},
		scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeReserved),
		scheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpIn, "res-1")))
	if !replacementsMatchSimulation([]*Replacement{replacement}, []*pscheduling.NodeClaim{
		simulatedNodeClaim("pool-a", []string{"m5.large"},
			scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeReserved),
			scheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpIn, "res-2")),
	}) {
		t.Fatal("expected differing reservation IDs between simulation runs to still match")
	}
}

func TestReplacementsMatchSimulationRejectsDifferentNodePoolUID(t *testing.T) {
	replacement := simulatedNodeClaim("pool-a", []string{"m5.large"})
	replacement.NodePoolUUID = "uid-old"
	recreated := simulatedNodeClaim("pool-a", []string{"m5.large"})
	recreated.NodePoolUUID = "uid-new"
	if replacementsMatchSimulation([]*Replacement{replacementFor(replacement)}, []*pscheduling.NodeClaim{recreated}) {
		t.Fatal("expected a same-name NodePool with a different UID (deleted and recreated) to not match")
	}
}

func TestReplacementsMatchSimulationRejectsDifferentNodePoolHash(t *testing.T) {
	replacement := simulatedNodeClaim("pool-a", []string{"m5.large"})
	replacement.Annotations = map[string]string{v1.NodePoolHashAnnotationKey: "hash-old", v1.NodePoolHashVersionAnnotationKey: v1.NodePoolHashVersion}
	edited := simulatedNodeClaim("pool-a", []string{"m5.large"})
	edited.Annotations = map[string]string{v1.NodePoolHashAnnotationKey: "hash-new", v1.NodePoolHashVersionAnnotationKey: v1.NodePoolHashVersion}
	if replacementsMatchSimulation([]*Replacement{replacementFor(replacement)}, []*pscheduling.NodeClaim{edited}) {
		t.Fatal("expected a same-UID NodePool with an edited template (different hash annotation) to not match")
	}
	same := simulatedNodeClaim("pool-a", []string{"m5.large"})
	same.Annotations = map[string]string{v1.NodePoolHashAnnotationKey: "hash-old", v1.NodePoolHashVersionAnnotationKey: v1.NodePoolHashVersion}
	if !replacementsMatchSimulation([]*Replacement{replacementFor(replacement)}, []*pscheduling.NodeClaim{same}) {
		t.Fatal("expected identical hash annotations to match")
	}
}

func TestReplacementsMatchSimulationRejectsDifferentTaints(t *testing.T) {
	tainted := simulatedNodeClaim("pool-a", []string{"m5.large"})
	tainted.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule}}
	if replacementsMatchSimulation([]*Replacement{replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"}))}, []*pscheduling.NodeClaim{tainted}) {
		t.Fatal("expected a replacement without the fresh claim's taints to not match")
	}
	taintedReplacement := simulatedNodeClaim("pool-a", []string{"m5.large"})
	taintedReplacement.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule}}
	if !replacementsMatchSimulation([]*Replacement{replacementFor(taintedReplacement)}, []*pscheduling.NodeClaim{tainted}) {
		t.Fatal("expected identical taints to match")
	}
}

func TestReplacementsMatchSimulationOneToOneMatching(t *testing.T) {
	// One simulated claim can satisfy both replacements, but there is no distinct claim for the second
	// replacement, so the matching must fail regardless of iteration order.
	broad := simulatedNodeClaim("pool-a", []string{"m5.large", "m5.xlarge"})
	if replacementsMatchSimulation(
		[]*Replacement{
			replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"})),
			replacementFor(simulatedNodeClaim("pool-a", []string{"m5.xlarge"})),
		},
		[]*pscheduling.NodeClaim{broad, simulatedNodeClaim("pool-b", []string{"m5.large"})},
	) {
		t.Fatal("expected matching to fail when two replacements compete for one compatible simulated claim")
	}
	// With two compatible claims the augmenting-path matching must reassign and succeed.
	if !replacementsMatchSimulation(
		[]*Replacement{
			replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large", "m5.xlarge"})),
			replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"})),
		},
		[]*pscheduling.NodeClaim{broad, simulatedNodeClaim("pool-a", []string{"m5.large"})},
	) {
		t.Fatal("expected augmenting-path matching to find a valid one-to-one assignment")
	}
}

func TestReplacementsMatchSimulationLargeAdversarialInput(t *testing.T) {
	// A failing match over many near-identical claims must complete quickly (polynomial, not factorial).
	const n = 50
	var replacements []*Replacement
	var claims []*pscheduling.NodeClaim
	for i := 0; i < n; i++ {
		replacements = append(replacements, replacementFor(simulatedNodeClaim("pool-a", []string{"m5.large"})))
		claims = append(claims, simulatedNodeClaim("pool-a", []string{"m5.large"}))
	}
	// Make one replacement unsatisfiable so the overall match fails after exploring alternatives.
	replacements = append(replacements, replacementFor(simulatedNodeClaim("pool-a", []string{fmt.Sprintf("missing-%d", n)})))
	claims = append(claims, simulatedNodeClaim("pool-a", []string{"m5.large"}))
	if replacementsMatchSimulation(replacements, claims) {
		t.Fatal("expected matching to fail when one replacement has no compatible simulated claim")
	}
}

// kuhnMatchingSize is the previous Kuhn's augmenting-path implementation, kept here as a benchmark
// baseline against hopcroftKarpMatchingSize.
func kuhnMatchingSize(adjacency [][]int, rightSize int) int {
	matchedTo := make([]int, rightSize)
	for j := range matchedTo {
		matchedTo[j] = -1
	}
	var augment func(i int, visited []bool) bool
	augment = func(i int, visited []bool) bool {
		for _, j := range adjacency[i] {
			if visited[j] {
				continue
			}
			visited[j] = true
			if matchedTo[j] == -1 || augment(matchedTo[j], visited) {
				matchedTo[j] = i
				return true
			}
		}
		return false
	}
	size := 0
	for i := range adjacency {
		if augment(i, make([]bool, rightSize)) {
			size++
		}
	}
	return size
}

// worstCaseAdjacency builds an n x n graph where every left vertex is compatible with every right
// vertex except a shifted diagonal, forcing augmenting-path rework without making matching trivial.
func worstCaseAdjacency(n int) [][]int {
	adjacency := make([][]int, n)
	for i := range adjacency {
		for j := 0; j < n; j++ {
			if j != (i+1)%n {
				adjacency[i] = append(adjacency[i], j)
			}
		}
	}
	return adjacency
}

func TestHopcroftKarpMatchesKuhn(t *testing.T) {
	for n := 1; n <= 12; n++ {
		adjacency := worstCaseAdjacency(n)
		if got, want := hopcroftKarpMatchingSize(adjacency, n), kuhnMatchingSize(adjacency, n); got != want {
			t.Fatalf("n=%d: hopcroft-karp found %d, kuhn found %d", n, got, want)
		}
	}
}

func BenchmarkMatchingHopcroftKarp(b *testing.B) {
	for _, n := range []int{3, 5, 10, 50} {
		adjacency := worstCaseAdjacency(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				hopcroftKarpMatchingSize(adjacency, n)
			}
		})
	}
}

func BenchmarkMatchingKuhn(b *testing.B) {
	for _, n := range []int{3, 5, 10, 50} {
		adjacency := worstCaseAdjacency(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				kuhnMatchingSize(adjacency, n)
			}
		})
	}
}

func TestIsValidRecordsWaitStageOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(withConsolidationType(context.Background(), "cancel-test"))
	fakeClock := clocktesting.NewFakeClock(time.Now())
	v := &ConsolidationValidator{validation: validation{clock: fakeClock}}

	errCh := make(chan error, 1)
	go func() {
		errCh <- v.isValid(ctx, Command{}, time.Minute)
	}()
	// wait until isValid is blocked on the validation wait, then cancel partway through
	for !fakeClock.HasWaiters() {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-errCh; err == nil {
		t.Fatal("expected isValid to fail when the context is canceled")
	}

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "karpenter_voluntary_disruption_pass_stage_seconds_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == ConsolidationTypeLabel && label.GetValue() == "cancel-test" {
					if metric.GetCounter().GetValue() <= 0 {
						t.Fatal("expected the canceled validation wait to record elapsed time")
					}
					return
				}
			}
		}
	}
	t.Fatal("expected a validation_wait stage series for the canceled wait")
}
