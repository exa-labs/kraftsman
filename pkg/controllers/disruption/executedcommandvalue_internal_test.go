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

	prometheus "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type valueStubMethod struct{}

func (valueStubMethod) ShouldDisrupt(context.Context, *Candidate) bool { return true }
func (valueStubMethod) ComputeCommands(context.Context, map[string]int, ...*Candidate) ([]Command, error) {
	return nil, nil
}
func (valueStubMethod) Reason() v1.DisruptionReason { return v1.DisruptionReasonUnderutilized }
func (valueStubMethod) Class() string               { return "stub" }
func (valueStubMethod) ConsolidationType() string   { return "single" }

// pricedCandidate is a candidate of a known hourly price, capacity type, and age, which is all the
// executed-command value observer reads.
func pricedCandidate(nodePool, capacityType string, price float64, created time.Time) *Candidate {
	return &Candidate{
		NodePool:     &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: nodePool}},
		capacityType: capacityType,
		Price:        price,
		StateNode:    &state.StateNode{NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(created)}}},
	}
}

// spotReplacement is a single spot replacement whose cheapest available offering costs price, which
// is what EstimatedSavings charges against the candidates.
func spotReplacement(price float64) *pscheduling.NodeClaim {
	return &pscheduling.NodeClaim{
		NodeClaimTemplate: pscheduling.NodeClaimTemplate{
			Requirements: scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot)),
			InstanceTypeOptions: []*cloudprovider.InstanceType{{
				Name:      "spot-type",
				Offerings: cloudprovider.Offerings{{Price: price, Available: true, Requirements: scheduling.NewRequirements()}},
			}},
		},
	}
}

func histogramSample(t *testing.T, name string, labels map[string]string) *prometheus.Histogram {
	t.Helper()
	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
	metrics:
		for _, metric := range family.GetMetric() {
			for key, want := range labels {
				found := false
				for _, pair := range metric.GetLabel() {
					if pair.GetName() == key {
						found = pair.GetValue() == want
						break
					}
				}
				if !found {
					continue metrics
				}
			}
			return metric.GetHistogram()
		}
	}
	return nil
}

func TestObserveExecutedCommandValueRecordsSavingsFractionAndAge(t *testing.T) {
	ConsolidationExecutedSavingsFraction.Reset()
	ConsolidationDisruptedNodeAgeSeconds.Reset()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := testclock.NewFakeClock(now)
	cmd := Command{
		Method: valueStubMethod{},
		Candidates: []*Candidate{
			pricedCandidate("value-pool", v1.CapacityTypeSpot, 1.0, now.Add(-2*time.Hour)),
			pricedCandidate("value-pool", v1.CapacityTypeSpot, 1.0, now.Add(-30*time.Minute)),
		},
		Replacements: replacementsFromNodeClaims(spotReplacement(1.5)),
		Results:      pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{spotReplacement(1.5)}},
	}
	ObserveExecutedCommandValue(context.Background(), fake.NewClientBuilder().Build(), clk, cmd)

	labels := map[string]string{"nodepool": "value-pool", "decision": "replace", "capacity_type_transition": "spot->spot"}
	fraction := histogramSample(t, "karpenter_voluntary_disruption_consolidation_executed_savings_fraction", labels)
	if fraction == nil || fraction.GetSampleCount() != 1 {
		t.Fatalf("savings fraction observed %v times for one NodePool, want 1", fraction.GetSampleCount())
	}
	// Two nodes at $1/h replaced by one at $1.5/h saves $0.5/h of $2/h.
	if got := fraction.GetSampleSum(); got < 0.2499 || got > 0.2501 {
		t.Fatalf("savings fraction = %v, want 0.25", got)
	}
	age := histogramSample(t, "karpenter_voluntary_disruption_consolidation_disrupted_node_age_seconds", labels)
	if age == nil || age.GetSampleCount() != 2 {
		t.Fatalf("node age observed %v times for two candidates, want 2", age.GetSampleCount())
	}
	if want := (2*time.Hour + 30*time.Minute).Seconds(); age.GetSampleSum() != want {
		t.Fatalf("node age sum = %v, want %v", age.GetSampleSum(), want)
	}
}

func TestObserveExecutedCommandValueSkipsUnpricedAndUndatedCandidates(t *testing.T) {
	ConsolidationExecutedSavingsFraction.Reset()
	ConsolidationDisruptedNodeAgeSeconds.Reset()
	clk := testclock.NewFakeClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	candidate := pricedCandidate("unpriced-pool", v1.CapacityTypeOnDemand, 0, time.Time{})
	ObserveExecutedCommandValue(context.Background(), fake.NewClientBuilder().Build(), clk, Command{
		Method:     valueStubMethod{},
		Candidates: []*Candidate{candidate},
	})
	labels := map[string]string{"nodepool": "unpriced-pool"}
	if histogramSample(t, "karpenter_voluntary_disruption_consolidation_executed_savings_fraction", labels) != nil {
		t.Fatal("a zero-priced source must not produce a savings fraction")
	}
	if histogramSample(t, "karpenter_voluntary_disruption_consolidation_disrupted_node_age_seconds", labels) != nil {
		t.Fatal("a NodeClaim without a creation timestamp must not produce an age")
	}
}

func TestReplacementOriginNamesReasonAndSourceCapacityTypes(t *testing.T) {
	now := time.Now()
	cmd := Command{
		Method: valueStubMethod{},
		Candidates: []*Candidate{
			pricedCandidate("p", v1.CapacityTypeSpot, 1, now),
			pricedCandidate("p", v1.CapacityTypeOnDemand, 1, now),
			pricedCandidate("p", v1.CapacityTypeSpot, 1, now),
		},
	}
	if got, want := replacementOrigin(cmd), "underutilized:on-demand,spot"; got != want {
		t.Fatalf("replacementOrigin = %q, want %q", got, want)
	}
}
