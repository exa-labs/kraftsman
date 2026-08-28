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
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestResolveCustomLabelsFromRequirements(t *testing.T) {
	nct := &NodeClaimTemplate{
		Requirements: scheduling.NewRequirements(
			scheduling.NewRequirement("efa", corev1.NodeSelectorOpExists),
			scheduling.NewRequirement("team", corev1.NodeSelectorOpIn, "training"),
		),
	}

	labels := nct.resolveCustomLabelsFromRequirements()

	if value, ok := labels["efa"]; ok {
		t.Fatalf("expected no label for unnarrowed Exists requirement, got efa=%q", value)
	}
	if labels["team"] != "training" {
		t.Fatalf("expected team=training, got %v", labels)
	}
}

func TestResolveCustomLabelsFromRequirementsNarrowedExists(t *testing.T) {
	requirements := scheduling.NewRequirements(scheduling.NewRequirement("efa", corev1.NodeSelectorOpExists))
	// A pod requirement `efa In [true]` intersects the pool's `efa Exists` down to a concrete value.
	requirements.Add(scheduling.NewRequirement("efa", corev1.NodeSelectorOpIn, "true"))
	nct := &NodeClaimTemplate{Requirements: requirements}

	labels := nct.resolveCustomLabelsFromRequirements()

	if labels["efa"] != "true" {
		t.Fatalf("expected efa=true from narrowed requirement, got %v", labels)
	}
}
