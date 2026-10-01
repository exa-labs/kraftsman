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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// A node keeps the value of an offering value label it launched with; a later change of the offering's value must
// not make the node's instance type look gone.
func TestInstanceTypeNotFoundIgnoresOfferingValueLabels(t *testing.T) {
	const priceLabel = "example.com/price"
	cloudprovider.OfferingValueLabels.Insert(priceLabel)
	defer cloudprovider.OfferingValueLabels.Delete(priceLabel)

	instanceType := &cloudprovider.InstanceType{
		Name: "gpu.large",
		Offerings: cloudprovider.Offerings{{
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "zone-a"),
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot),
				scheduling.NewRequirement(priceLabel, corev1.NodeSelectorOpIn, "2080"),
			),
			Price:     16.6,
			Available: true,
		}},
	}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		corev1.LabelInstanceTypeStable: "gpu.large",
		corev1.LabelTopologyZone:       "zone-a",
		v1.CapacityTypeLabelKey:        v1.CapacityTypeSpot,
		priceLabel:                     "2070",
	}}}

	if reason := instanceTypeNotFound([]*cloudprovider.InstanceType{instanceType}, nodeClaim); reason != "" {
		t.Fatalf("expected no drift, got %q", reason)
	}
	// A node launched before the provider stamped the label still belongs to the offering: the value label is not
	// part of the offering's identity.
	delete(nodeClaim.Labels, priceLabel)
	if reason := instanceTypeNotFound([]*cloudprovider.InstanceType{instanceType}, nodeClaim); reason != "" {
		t.Fatalf("expected no drift for a node without the value label, got %q", reason)
	}
	nodeClaim.Labels[corev1.LabelTopologyZone] = "zone-b"
	if reason := instanceTypeNotFound([]*cloudprovider.InstanceType{instanceType}, nodeClaim); reason != InstanceTypeNotFound {
		t.Fatalf("expected %q for an offering that is really gone, got %q", InstanceTypeNotFound, reason)
	}
}
