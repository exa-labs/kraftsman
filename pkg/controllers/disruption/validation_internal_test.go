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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestValidationFailureDetail(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"churn", newChurnValidationErrorWithDetail(validationDetailCandidateChanged, fmt.Errorf("x")), validationDetailCandidateChanged},
		{"budget", newBudgetValidationErrorWithDetail(validationDetailCandidateNominated, fmt.Errorf("x")), validationDetailCandidateNominated},
		{"scheduling", newSchedulingValidationErrorWithDetail(validationDetailReplacementMismatch, fmt.Errorf("x")), validationDetailReplacementMismatch},
		{"wrapped", fmt.Errorf("validating: %w", newSchedulingValidationErrorWithDetail(validationDetailUninitializedNode, fmt.Errorf("x"))), validationDetailUninitializedNode},
		{"no detail", NewSchedulingValidationError(fmt.Errorf("x")), "unknown"},
		{"not a validation error", fmt.Errorf("x"), "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := getValidationFailureDetail(tc.err); got != tc.want {
				t.Fatalf("getValidationFailureDetail() = %q, want %q", got, tc.want)
			}
		})
	}
	// The detail refines the reason; it never changes it.
	if got := getValidationFailureReason(newSchedulingValidationErrorWithDetail(validationDetailReplacementCountChanged, fmt.Errorf("x"))); got != "scheduling" {
		t.Fatalf("getValidationFailureReason() = %q, want scheduling", got)
	}
}

func TestUnscheduledPodsDetail(t *testing.T) {
	bound := func(name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.PodSpec{NodeName: "candidate"}}
	}
	pending := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pending"},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
		}}},
	}
	uninitialized := NewUninitializedNodeError(&pscheduling.ExistingNode{})
	for _, tc := range []struct {
		name   string
		errors map[*corev1.Pod]error
		want   string
	}{
		{"uninitialized node", map[*corev1.Pod]error{bound("a"): uninitialized, bound("b"): fmt.Errorf("no fit")}, validationDetailUninitializedNode},
		{"fits nowhere", map[*corev1.Pod]error{bound("a"): fmt.Errorf("no fit")}, validationDetailPodsUnschedulable},
		// A pending pod's error never fails validation, so it does not decide the detail either.
		{"pending pod ignored", map[*corev1.Pod]error{pending: uninitialized, bound("a"): fmt.Errorf("no fit")}, validationDetailPodsUnschedulable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := unscheduledPodsDetail(pscheduling.Results{PodErrors: tc.errors}); got != tc.want {
				t.Fatalf("unscheduledPodsDetail() = %q, want %q", got, tc.want)
			}
		})
	}
}
