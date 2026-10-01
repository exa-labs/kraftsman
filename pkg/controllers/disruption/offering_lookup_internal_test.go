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
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// filterOutSameInstanceType caps replacements at the price of each candidate's own offering, and a candidate it finds
// no offering for caps them at zero. A candidate keeps the value of an offering value label it launched with, or has
// none when it launched before the key was registered; either way its offering must still be found, so a cheaper
// replacement survives the filter.
func TestFilterOutSameInstanceTypeFindsOfferingAcrossValueLabelChanges(t *testing.T) {
	const valueLabel = "example.com/price"
	cloudprovider.OfferingValueLabels.Insert(valueLabel)
	defer cloudprovider.OfferingValueLabels.Delete(valueLabel)

	const zone = "test-zone-1"
	current := fake.NewInstanceType("current", fake.WithOfferings(cloudprovider.Offering{
		Available: true,
		Price:     1.0,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			corev1.LabelTopologyZone: zone,
			v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
			valueLabel:               "2080",
		}),
	}))
	cheaper := priceFilterInstanceType("cheaper", 0.2)

	for name, candidateLabels := range map[string]map[string]string{
		"launched at another value":  {corev1.LabelTopologyZone: zone, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, valueLabel: "2070"},
		"launched without the label": {corev1.LabelTopologyZone: zone, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := &Candidate{
				StateNode:    &state.StateNode{NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: candidateLabels}}},
				instanceType: current,
			}
			replacement, err := filterOutSameInstanceType(&Replacement{NodeClaim: priceFilterNodeClaim(1, cheaper, current)}, []*Candidate{candidate})
			if err != nil {
				t.Fatalf("expected the cheaper instance type to survive, got %v", err)
			}
			got := lo.Map(replacement.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
			if len(got) != 1 || got[0] != cheaper.Name {
				t.Fatalf("expected only %q to survive, got %v", cheaper.Name, got)
			}
		})
	}
}
