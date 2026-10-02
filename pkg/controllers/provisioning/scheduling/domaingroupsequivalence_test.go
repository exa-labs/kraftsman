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

// Equivalence tests for buildDomainGroups: building each NodePool's template requirements once and
// intersecting them key by key with every instance type must produce exactly the domain groups that
// rebuilding the template requirements per instance type and adding the instance type's to them does.

package scheduling

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

// referenceDomainGroups rebuilds the template requirements for every instance type and adds the instance
// type's requirements to them.
func referenceDomainGroups(nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) map[string]TopologyDomainGroup {
	nodePoolIndex := lo.SliceToMap(nodePools, func(np *v1.NodePool) (string, *v1.NodePool) { return np.Name, np })
	domainGroups := map[string]TopologyDomainGroup{}
	for npName, its := range instanceTypes {
		np := nodePoolIndex[npName]
		nodePoolRequirements := nodePoolDomainRequirements(np)
		for _, it := range its {
			requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
			requirements.Add(scheduling.NewLabelRequirements(np.Spec.Template.Labels).Values()...)
			requirements.Add(it.Requirements.Values()...)
			for topologyKey, requirement := range requirements {
				if _, ok := domainGroups[topologyKey]; !ok {
					domainGroups[topologyKey] = NewTopologyDomainGroup()
				}
				for _, domain := range requirement.Values() {
					domainGroups[topologyKey].Insert(domain, npName, np.Spec.Template.Spec.Taints, nodePoolRequirements)
				}
			}
		}
		requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
		requirements.Add(scheduling.NewLabelRequirements(np.Spec.Template.Labels).Values()...)
		for key, requirement := range requirements {
			if requirement.Operator() == corev1.NodeSelectorOpIn {
				if _, ok := domainGroups[key]; !ok {
					domainGroups[key] = NewTopologyDomainGroup()
				}
				for _, value := range requirement.Values() {
					domainGroups[key].Insert(value, npName, np.Spec.Template.Spec.Taints, nodePoolRequirements)
				}
			}
		}
	}
	return domainGroups
}

func expectSameDomainGroups(t *testing.T, context string, got, want map[string]TopologyDomainGroup) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		for key := range lo.Assign(got, want) {
			if !reflect.DeepEqual(got[key], want[key]) {
				t.Fatalf("%s: domain groups for %q differ\ngot:  %v\nwant: %v", context, key, got[key], want[key])
			}
		}
		t.Fatalf("%s: domain groups differ", context)
	}
}

func TestBuildDomainGroupsMatchesReferenceOnFleet(t *testing.T) {
	fleet := newConstructionFleet(16, 400)
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	if len(fleet.templates(ctx)) == 0 {
		t.Fatal("test setup: expected the fleet NodePools to have launchable instance types")
	}
	expectSameDomainGroups(t, "fleet", buildDomainGroups(fleet.nodePools, fleet.instanceTypes), referenceDomainGroups(fleet.nodePools, fleet.instanceTypes))
}

// randomDomainWorld generates NodePools and instance types that exercise every operator on both sides, numeric
// bounds, keys set by only one side, NodePools without instance types and instance types without requirements.
type randomDomainWorld struct{ *rand.Rand }

var (
	randomDomainKeys   = []string{corev1.LabelTopologyZone, v1.CapacityTypeLabelKey, corev1.LabelArchStable, "example.com/a", "example.com/b", "example.com/size"}
	randomDomainValues = []string{"1", "2", "3", "4", "x", "y"}
	randomDomainOps    = []corev1.NodeSelectorOperator{corev1.NodeSelectorOpIn, corev1.NodeSelectorOpNotIn, corev1.NodeSelectorOpExists, corev1.NodeSelectorOpDoesNotExist, corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt}
)

func (w randomDomainWorld) requirement() (corev1.NodeSelectorOperator, []string) {
	op := randomDomainOps[w.Intn(len(randomDomainOps))]
	switch op {
	case corev1.NodeSelectorOpIn, corev1.NodeSelectorOpNotIn:
		return op, lo.Filter(randomDomainValues, func(string, int) bool { return w.Intn(2) == 0 })
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		return op, []string{fmt.Sprint(w.Intn(5))}
	default:
		return op, nil
	}
}

func (w randomDomainWorld) nodePool(name string) *v1.NodePool {
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}})
	np.Spec.Template.Spec.Requirements = nil
	np.Spec.Template.Labels = map[string]string{}
	for _, key := range randomDomainKeys {
		switch w.Intn(4) {
		case 0:
			op, values := w.requirement()
			np.Spec.Template.Spec.Requirements = append(np.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{Key: key, Operator: op, Values: values})
		case 1:
			np.Spec.Template.Labels[key] = randomDomainValues[w.Intn(len(randomDomainValues))]
		}
	}
	if w.Intn(3) == 0 {
		np.Spec.Template.Spec.Taints = []corev1.Taint{{Key: "example.com/taint", Effect: corev1.TaintEffectNoSchedule}}
	}
	return np
}

func (w randomDomainWorld) instanceTypes(prefix string) []*cloudprovider.InstanceType {
	var its []*cloudprovider.InstanceType
	for i := range w.Intn(6) {
		requirements := scheduling.NewRequirements()
		for _, key := range randomDomainKeys {
			if w.Intn(2) == 0 {
				op, values := w.requirement()
				requirements.Add(scheduling.NewRequirement(key, op, values...))
			}
		}
		its = append(its, &cloudprovider.InstanceType{Name: fmt.Sprintf("%s-%d", prefix, i), Requirements: requirements})
	}
	return its
}

func TestBuildDomainGroupsMatchesReferenceOnRandomPools(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	w := randomDomainWorld{rand.New(rand.NewSource(seed))} //nolint:gosec
	for iteration := range 300 {
		var nodePools []*v1.NodePool
		instanceTypes := map[string][]*cloudprovider.InstanceType{}
		for p := range 1 + w.Intn(4) {
			np := w.nodePool(fmt.Sprintf("pool-%d", p))
			nodePools = append(nodePools, np)
			instanceTypes[np.Name] = w.instanceTypes(np.Name)
		}
		expectSameDomainGroups(t, fmt.Sprintf("seed %d iteration %d", seed, iteration), buildDomainGroups(nodePools, instanceTypes), referenceDomainGroups(nodePools, instanceTypes))
	}
}
