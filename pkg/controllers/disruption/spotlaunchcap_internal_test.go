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
	"slices"
	"testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func spotLaunchContext(minInstanceTypes, launchInstanceTypes int) context.Context {
	return options.ToContext(context.Background(), &options.Options{
		SpotToSpotMinInstanceTypes:    minInstanceTypes,
		SpotToSpotLaunchInstanceTypes: launchInstanceTypes,
		FeatureGates:                  options.FeatureGates{SpotToSpotConsolidation: true},
	})
}

// fourSpotTypes are four spot types cheaper than candidatePrice, plus one that is not, in price order.
func fourSpotTypes() []*cloudprovider.InstanceType {
	return []*cloudprovider.InstanceType{
		odToSpotInstanceType("s1", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 1.0)),
		odToSpotInstanceType("s2", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 2.0)),
		odToSpotInstanceType("s3", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 3.0)),
		odToSpotInstanceType("s4", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", 4.0)),
		odToSpotInstanceType("pricey", odToSpotOffering(v1.CapacityTypeSpot, "zone-a", candidatePrice+1)),
	}
}

func TestSpotToSpotLaunchCarriesTheConfiguredNumberOfCheapestTypes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		min, launch     int
		wantLaunchTypes []string
	}{
		{name: "the launch follows the minimum by default", min: 1, launch: 0, wantLaunchTypes: []string{"s1"}},
		{name: "a larger launch cap keeps the next cheapest types", min: 1, launch: 3, wantLaunchTypes: []string{"s1", "s2", "s3"}},
		{name: "the launch never carries a type that is not cheaper", min: 1, launch: 10, wantLaunchTypes: []string{"s1", "s2", "s3", "s4"}},
		{name: "a launch cap below the minimum keeps the minimum", min: 2, launch: 1, wantLaunchTypes: []string{"s1", "s2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nc := odToSpotNodeClaim(fourSpotTypes()...)
			nc.NodePoolName = "pool"
			c := &consolidation{cloudProvider: fake.NewCloudProvider(), recorder: test.NewEventRecorder()}
			cmd, reason, err := c.computeSpotToSpotConsolidation(spotLaunchContext(tc.min, tc.launch), advisorCandidates(), pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{nc}}, priceBudget{candidatePrice: candidatePrice}, consolidationSimulationOptions{silent: true})
			if err != nil || reason != "" || len(cmd.Replacements) != 1 {
				t.Fatalf("computeSpotToSpotConsolidation() = (%d replacements, %q, %v), want one replacement", len(cmd.Replacements), reason, err)
			}
			if got := instanceTypeNames(nc.InstanceTypeOptions); !slices.Equal(got, tc.wantLaunchTypes) {
				t.Fatalf("launch carries %v, want %v", got, tc.wantLaunchTypes)
			}
		})
	}
}

func TestODToSpotRetryLaunchCarriesTheConfiguredNumberOfCheapestTypes(t *testing.T) {
	for _, tc := range []struct {
		launch          int
		wantLaunchTypes []string
	}{
		{launch: 0, wantLaunchTypes: []string{"s1"}},
		{launch: 2, wantLaunchTypes: []string{"s1", "s2"}},
	} {
		nc := odToSpotNodeClaim(fourSpotTypes()...)
		snapshot := [][]*cloudprovider.InstanceType{append([]*cloudprovider.InstanceType(nil), nc.InstanceTypeOptions...)}
		opts := options.FromContext(spotLaunchContext(1, tc.launch))
		c := &consolidation{recorder: test.NewEventRecorder()}
		if !c.retrySpotOnlyReplacements(SingleNodeConsolidationType, consolidationSimulationOptions{silent: true}, advisorCandidates(), []*pscheduling.NodeClaim{nc}, snapshot, priceBudget{candidatePrice: candidatePrice}, opts.SpotReplacementLaunchCap(), nil) {
			t.Fatalf("launch cap %d: retrySpotOnlyReplacements() rejected a replacement with four cheaper spot types", tc.launch)
		}
		if got := instanceTypeNames(nc.InstanceTypeOptions); !slices.Equal(got, tc.wantLaunchTypes) {
			t.Fatalf("launch cap %d: launch carries %v, want %v", tc.launch, got, tc.wantLaunchTypes)
		}
	}
}
