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
	"math"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// offering is an available offering of one capacity type in one zone.
func offering(capacityType, zone string, price float64) *cloudprovider.Offering {
	return &cloudprovider.Offering{
		Available: true,
		Price:     price,
		Requirements: scheduling.NewRequirements(
			scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityType),
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
		),
	}
}

// claim is a replacement NodeClaim restricted to the given requirements with the given instance type options.
func claim(reqs scheduling.Requirements, options ...*cloudprovider.InstanceType) *pscheduling.NodeClaim {
	return &pscheduling.NodeClaim{NodeClaimTemplate: pscheduling.NodeClaimTemplate{Requirements: reqs, InstanceTypeOptions: options}}
}

// counterValue reads the registered counter series carrying every given label, failing the test when none does.
func counterValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matched := 0
			for _, pair := range metric.GetLabel() {
				if want, ok := labels[pair.GetName()]; ok && pair.GetValue() == want {
					matched++
				}
			}
			if matched == len(labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("no %s series with labels %v", name, labels)
	return 0
}

func approx(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// launchedNodeClaim is the NodeClaim object a replacement launched as.
func launchedNodeClaim(name, instanceType, zone, capacityType string) *v1.NodeClaim {
	return &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
		corev1.LabelInstanceTypeStable: instanceType,
		corev1.LabelTopologyZone:       zone,
		v1.CapacityTypeLabelKey:        capacityType,
	}}}
}

// spotReplacementCommand replaces one $1/h spot candidate with a spot claim allowed two types in two zones. Its
// cheapest launchable option is a.large in zone-a at $0.20/h.
func spotReplacementCommand(replacementName string) Command {
	cheap := &cloudprovider.InstanceType{Name: "a.large", Offerings: cloudprovider.Offerings{
		offering(v1.CapacityTypeSpot, "zone-a", 0.20),
		offering(v1.CapacityTypeSpot, "zone-b", 0.60),
	}}
	pricier := &cloudprovider.InstanceType{Name: "b.large", Offerings: cloudprovider.Offerings{
		offering(v1.CapacityTypeSpot, "zone-a", 0.40),
		offering(v1.CapacityTypeSpot, "zone-b", 0.70),
	}}
	nc := claim(scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot)), cheap, pricier)
	replacements := replacementsFromNodeClaims(nc)
	replacements[0].Name = replacementName
	return Command{
		Method:       valueStubMethod{},
		Candidates:   []*Candidate{pricedCandidate("savings-pool", v1.CapacityTypeSpot, 1.00, time.Now().Add(-time.Hour))},
		Replacements: replacements,
		Results:      pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{nc}},
	}
}

func TestExecutedSavingsPricesTheOfferingTheReplacementLaunched(t *testing.T) {
	cmd := spotReplacementCommand("replacement-1")
	kubeClient := fake.NewClientBuilder().WithObjects(launchedNodeClaim("replacement-1", "b.large", "zone-b", v1.CapacityTypeSpot)).Build()
	approx(t, "executedSavings", executedSavings(context.Background(), kubeClient, cmd), 0.30)
}

func TestExecutedSavingsFallsBackToTheEstimateWhenTheLaunchIsUnknown(t *testing.T) {
	for name, kubeClient := range map[string]*fake.ClientBuilder{
		"replacement NodeClaim not found": fake.NewClientBuilder(),
		"launched type was never priced":  fake.NewClientBuilder().WithObjects(launchedNodeClaim("replacement-1", "c.large", "zone-a", v1.CapacityTypeSpot)),
		"launched offering not offered":   fake.NewClientBuilder().WithObjects(launchedNodeClaim("replacement-1", "b.large", "zone-c", v1.CapacityTypeSpot)),
		"not launched yet":                fake.NewClientBuilder().WithObjects(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "replacement-1"}}),
	} {
		t.Run(name, func(t *testing.T) {
			approx(t, "executedSavings", executedSavings(context.Background(), kubeClient.Build(), spotReplacementCommand("replacement-1")), 0.80)
		})
	}
}

func TestRealizedSavingsAndSavingsFractionUseTheLaunchedOffering(t *testing.T) {
	ConsolidationRealizedSavingsDollarsPerHourTotal.Reset()
	ConsolidationExecutedSavingsFraction.Reset()
	cmd := spotReplacementCommand("replacement-1")
	kubeClient := fake.NewClientBuilder().WithObjects(launchedNodeClaim("replacement-1", "b.large", "zone-b", v1.CapacityTypeSpot)).Build()

	ObserveRealizedSavings(context.Background(), kubeClient, cmd)
	ObserveExecutedCommandValue(context.Background(), kubeClient, testclock.NewFakeClock(time.Now()), cmd)

	labels := map[string]string{"nodepool": "savings-pool", "decision": "replace", "capacity_type_transition": "spot->spot"}
	approx(t, "realized savings", counterValue(t, "karpenter_voluntary_disruption_consolidation_realized_savings_dollars_per_hour_total", labels), 0.30)
	fraction := histogramSample(t, "karpenter_voluntary_disruption_consolidation_executed_savings_fraction", labels)
	if fraction == nil || fraction.GetSampleCount() != 1 {
		t.Fatalf("savings fraction observed %v times, want 1", fraction.GetSampleCount())
	}
	approx(t, "savings fraction", fraction.GetSampleSum(), 0.30)
}
