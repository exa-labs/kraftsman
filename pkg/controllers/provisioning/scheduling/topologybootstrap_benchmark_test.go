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

package scheduling

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// BenchmarkCheapestPriceByDomain prices a zone key over 700 instance types offered in 20 zones as spot and on-demand.
func BenchmarkCheapestPriceByDomain(b *testing.B) {
	instanceTypes := make([]*cloudprovider.InstanceType, 0, 700)
	for i := range 700 {
		offerings := make([]cloudprovider.Offering, 0, 40)
		for z := range 20 {
			zone := fmt.Sprintf("zone-%02d", z)
			offerings = append(offerings,
				offeringInZone(v1.CapacityTypeSpot, zone, float64(i+z)/10, true),
				offeringInZone(v1.CapacityTypeOnDemand, zone, float64(i+z)/5, true),
			)
		}
		instanceTypes = append(instanceTypes, fake.NewInstanceType(fmt.Sprintf("type-%03d", i),
			fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(2 + i%64))}),
			fake.WithOfferings(offerings...)))
	}
	groups := []DaemonOverheadGroup{{
		InstanceTypes:  instanceTypes,
		DaemonOverhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		HostPortUsage:  scheduling.NewHostPortUsage(),
	}}
	requests := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}
	pod := gangPod()
	always := func(*cloudprovider.Offering) bool { return true }
	for _, tc := range []struct {
		name         string
		requirements scheduling.Requirements
	}{
		{"three zones on-demand", scheduling.NewRequirements(
			scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "zone-00", "zone-01", "zone-02"),
		)},
		{"unrestricted", scheduling.NewRequirements()},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				cheapestPriceByDomain(instanceTypes, groups, pod, tc.requirements, requests, corev1.LabelTopologyZone, always)
			}
		})
	}
}
