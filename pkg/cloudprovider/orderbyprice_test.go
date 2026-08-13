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

package cloudprovider_test

import (
	"fmt"
	"math/rand"
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func offering(price float64, available bool, capacityType string, zone string) *cloudprovider.Offering {
	return &cloudprovider.Offering{
		Available: available,
		Price:     price,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  capacityType,
			corev1.LabelTopologyZone: zone,
		}),
	}
}

func instanceType(name string, offerings ...*cloudprovider.Offering) *cloudprovider.InstanceType {
	return &cloudprovider.InstanceType{Name: name, Offerings: offerings}
}

func names(its cloudprovider.InstanceTypes) []string {
	out := make([]string, len(its))
	for i, it := range its {
		out[i] = it.Name
	}
	return out
}

func TestOrderByPrice(t *testing.T) {
	reqs := scheduling.NewRequirements(
		scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, "spot"),
	)
	its := cloudprovider.InstanceTypes{
		// cheapest offering is incompatible (on-demand); effective price is 5.0
		instanceType("b", offering(1.0, true, "on-demand", "z1"), offering(5.0, true, "spot", "z1")),
		// cheapest offering is unavailable; effective price is 3.0
		instanceType("a", offering(0.5, false, "spot", "z1"), offering(3.0, true, "spot", "z2")),
		// no eligible offerings at all; sorts last
		instanceType("d", offering(0.1, true, "on-demand", "z1")),
		// cheapest compatible+available across multiple offerings
		instanceType("c", offering(2.0, true, "spot", "z1"), offering(4.0, true, "spot", "z2")),
	}
	got := names(its.OrderByPrice(reqs))
	want := []string{"c", "a", "b", "d"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected order: got %v, want %v", got, want)
		}
	}
}

func BenchmarkOrderByPrice(b *testing.B) {
	reqs := scheduling.NewRequirements(
		scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, "spot", "on-demand"),
	)
	rng := rand.New(rand.NewSource(1)) //nolint:gosec
	its := make(cloudprovider.InstanceTypes, 600)
	for i := range its {
		offerings := make(cloudprovider.Offerings, 6)
		for j := range offerings {
			offerings[j] = offering(rng.Float64()*10, j%5 != 0, []string{"spot", "on-demand"}[j%2], fmt.Sprintf("z%d", j%3))
		}
		its[i] = instanceType(fmt.Sprintf("it-%d", i), offerings...)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rng.Shuffle(len(its), func(x, y int) { its[x], its[y] = its[y], its[x] })
		its.OrderByPrice(reqs)
	}
}
