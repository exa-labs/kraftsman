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
	"context"
	"fmt"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// daemonPod builds a daemon pod requesting cpu with the given node selector requirements.
func daemonPod(name, cpu string, reqs ...corev1.NodeSelectorRequirement) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: name},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
				},
			}},
		},
	}
	if len(reqs) != 0 {
		p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: reqs}},
			},
		}}
	}
	return p
}

func in(key string, values ...string) corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpIn, Values: values}
}

func notIn(key string, values ...string) corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpNotIn, Values: values}
}

func expectOverhead(t *testing.T, got corev1.ResourceList, cpu string, pods int64) {
	t.Helper()
	if got.Cpu().Cmp(resource.MustParse(cpu)) != 0 {
		t.Fatalf("expected cpu %s, got %s", cpu, got.Cpu())
	}
	if got.Pods().Value() != pods {
		t.Fatalf("expected %d pods, got %d", pods, got.Pods().Value())
	}
}

func TestComputeDaemonOverheadSumsCoSchedulableDaemons(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, "r1", "r2"))
	got, truncated := computeDaemonOverhead(candidate, []*corev1.Pod{
		daemonPod("a", "100m"),
		daemonPod("b", "200m"),
		daemonPod("c", "300m", in(corev1.LabelOSStable, "linux")),
		daemonPod("d", "400m", in(corev1.LabelOSStable, "linux")),
	})
	if truncated {
		t.Fatal("unexpected truncation")
	}
	expectOverhead(t, got, "1", 4)
}

func TestComputeDaemonOverheadDoesNotSumMutuallyExclusiveDaemons(t *testing.T) {
	regions := []string{"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9", "r10", "r11", "r12"}
	candidate := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, regions...))
	var pods []*corev1.Pod
	for _, region := range regions {
		pods = append(pods, daemonPod("cache-"+region, "200m", in(corev1.LabelTopologyRegion, region)))
	}
	pods = append(pods, daemonPod("shared", "50m"))
	got, truncated := computeDaemonOverhead(candidate, pods)
	if truncated {
		t.Fatal("unexpected truncation")
	}
	// one regional daemon plus the region-agnostic one
	expectOverhead(t, got, "250m", 2)
}

func TestComputeDaemonOverheadNarrowCandidateSeesOnlyItsRegion(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, "r2"))
	got, _ := computeDaemonOverhead(candidate, []*corev1.Pod{
		daemonPod("cache-r2", "700m", in(corev1.LabelTopologyRegion, "r2")),
		daemonPod("shared", "50m"),
	})
	expectOverhead(t, got, "750m", 2)
}

// daemonPodWithTerms builds a daemon pod requesting cpu whose required node affinity ORs one term per element of terms.
func daemonPodWithTerms(name, cpu string, terms ...[]corev1.NodeSelectorRequirement) *corev1.Pod {
	p := daemonPod(name, cpu)
	p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: lo.Map(terms, func(reqs []corev1.NodeSelectorRequirement, _ int) corev1.NodeSelectorTerm {
				return corev1.NodeSelectorTerm{MatchExpressions: reqs}
			}),
		},
	}}
	return p
}

func TestComputeDaemonOverheadHonorsEveryRequiredAffinityAlternative(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "a", "b"))
	got, truncated := computeDaemonOverhead(candidate, []*corev1.Pod{
		// zone a OR zone b: schedules in both realizations, so it co-schedules with the zone b daemon
		daemonPodWithTerms("either", "300m", []corev1.NodeSelectorRequirement{in(corev1.LabelTopologyZone, "a")}, []corev1.NodeSelectorRequirement{in(corev1.LabelTopologyZone, "b")}),
		daemonPod("b-only", "500m", in(corev1.LabelTopologyZone, "b")),
	})
	if truncated {
		t.Fatal("unexpected truncation")
	}
	expectOverhead(t, got, "800m", 2)
}

func TestBuildDaemonOverheadGroupsKeepsAffinityAlternativesAcrossInstanceTypes(t *testing.T) {
	zoned := func(name, zone string) *cloudprovider.InstanceType {
		return &cloudprovider.InstanceType{
			Name:         name,
			Requirements: scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone)),
		}
	}
	// The zone b instance type comes first: it is compatible only through the daemon's second term, and the
	// relaxation that establishes this must not drop the first term the zone a instance type needs.
	nct := &NodeClaimTemplate{
		NodePoolName:        "pool",
		Requirements:        scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "a", "b")),
		InstanceTypeOptions: []*cloudprovider.InstanceType{zoned("in-b", "b"), zoned("in-a", "a")},
	}
	daemon := daemonPodWithTerms("either", "400m", []corev1.NodeSelectorRequirement{in(corev1.LabelTopologyZone, "a")}, []corev1.NodeSelectorRequirement{in(corev1.LabelTopologyZone, "b")})
	groups := buildDaemonOverheadGroupsForTemplate(context.Background(), nct, []*corev1.Pod{daemon})
	if len(groups) != 1 {
		t.Fatalf("expected one overhead group covering both instance types, got %d", len(groups))
	}
	if len(groups[0].InstanceTypes) != 2 {
		t.Fatalf("expected both instance types in the group, got %d", len(groups[0].InstanceTypes))
	}
	expectOverhead(t, groups[0].DaemonOverhead, "400m", 1)
	if got := len(daemon.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms); got != 2 {
		t.Fatalf("daemon pod affinity was mutated: %d terms remain", got)
	}
}

func TestComputeDaemonOverheadIgnoresAlternativesTheCandidateCannotRealize(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "a", "b"))
	got, _ := computeDaemonOverhead(candidate, []*corev1.Pod{
		// zone a OR zone c: no node satisfying candidate is in zone c, so only the zone a alternative counts
		daemonPodWithTerms("a-or-c", "300m", []corev1.NodeSelectorRequirement{in(corev1.LabelTopologyZone, "a")}, []corev1.NodeSelectorRequirement{in(corev1.LabelTopologyZone, "c")}),
		daemonPod("b-only", "500m", in(corev1.LabelTopologyZone, "b")),
	})
	expectOverhead(t, got, "500m", 1)
}

func TestComputeDaemonOverheadCombinesNodeSelectorWithEachAlternative(t *testing.T) {
	candidate := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "a", "b"),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, "amd64", "arm64"),
	)
	either := daemonPodWithTerms("either-arm", "300m", []corev1.NodeSelectorRequirement{in(corev1.LabelTopologyZone, "a")}, []corev1.NodeSelectorRequirement{in(corev1.LabelTopologyZone, "b")})
	either.Spec.NodeSelector = map[string]string{corev1.LabelArchStable: "arm64"}
	got, _ := computeDaemonOverhead(candidate, []*corev1.Pod{
		either,
		daemonPod("b-amd", "500m", in(corev1.LabelTopologyZone, "b"), in(corev1.LabelArchStable, "amd64")),
	})
	// the nodeSelector applies to every alternative, so the two never share an (arch, zone) realization
	expectOverhead(t, got, "500m", 1)
}

func TestComputeDaemonOverheadTakesElementWiseMaxAcrossRealizations(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "z1", "z2"))
	z1 := daemonPod("z1", "1", in(corev1.LabelTopologyZone, "z1"))
	z1.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1Gi")
	z2 := daemonPod("z2", "100m", in(corev1.LabelTopologyZone, "z2"))
	z2.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("4Gi")
	got, _ := computeDaemonOverhead(candidate, []*corev1.Pod{z1, z2})
	expectOverhead(t, got, "1", 1)
	if got.Memory().Cmp(resource.MustParse("4Gi")) != 0 {
		t.Fatalf("expected memory 4Gi, got %s", got.Memory())
	}
}

func TestComputeDaemonOverheadNotInCoversValuesNoDaemonNames(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "z1", "z2", "z3"))
	got, _ := computeDaemonOverhead(candidate, []*corev1.Pod{
		daemonPod("only-z1", "1", in(corev1.LabelTopologyZone, "z1")),
		daemonPod("not-z1", "2", notIn(corev1.LabelTopologyZone, "z1")),
		daemonPod("not-z2", "3", notIn(corev1.LabelTopologyZone, "z2")),
	})
	// z1: only-z1 + not-z2 = 4; z2: not-z1 = 2; z3: not-z1 + not-z2 = 5
	expectOverhead(t, got, "5", 2)
}

func TestComputeDaemonOverheadExistsCandidateAdmitsUnnamedValue(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement("example.com/tier", corev1.NodeSelectorOpExists))
	got, _ := computeDaemonOverhead(candidate, []*corev1.Pod{
		daemonPod("gold", "1", in("example.com/tier", "gold")),
		daemonPod("not-gold", "2", notIn("example.com/tier", "gold")),
		daemonPod("not-silver", "3", notIn("example.com/tier", "silver")),
	})
	// gold: 1 + 3; silver: 2; anything else: 2 + 3
	expectOverhead(t, got, "5", 2)
}

func TestComputeDaemonOverheadAbsentLabelOnlyForCustomKeys(t *testing.T) {
	custom := "example.com/pool"
	// The candidate says nothing about the custom key, so the node may lack it; NotIn matches an absent label.
	got, _ := computeDaemonOverhead(scheduling.NewRequirements(), []*corev1.Pod{
		daemonPod("a", "1", notIn(custom, "x")),
		daemonPod("b", "2", notIn(custom, "y")),
	})
	expectOverhead(t, got, "3", 2)

	// A well-known label is always present, so `In` and `NotIn` on the same value are exclusive.
	got, _ = computeDaemonOverhead(scheduling.NewRequirements(), []*corev1.Pod{
		daemonPod("a", "1", in(corev1.LabelTopologyRegion, "r1")),
		daemonPod("b", "2", notIn(corev1.LabelTopologyRegion, "r1")),
	})
	expectOverhead(t, got, "2", 1)
}

func TestComputeDaemonOverheadMultipleKeysAreIndependent(t *testing.T) {
	candidate := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, "r1", "r2"),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, "amd64", "arm64"),
	)
	got, _ := computeDaemonOverhead(candidate, []*corev1.Pod{
		daemonPod("r1", "1", in(corev1.LabelTopologyRegion, "r1")),
		daemonPod("r2", "2", in(corev1.LabelTopologyRegion, "r2")),
		daemonPod("amd", "10", in(corev1.LabelArchStable, "amd64")),
		daemonPod("arm", "20", in(corev1.LabelArchStable, "arm64")),
	})
	// (r2, arm64) = 22
	expectOverhead(t, got, "22", 2)
}

func TestComputeDaemonOverheadNumericBoundsFallBackToSum(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement("example.com/gen", corev1.NodeSelectorOpExists))
	got, truncated := computeDaemonOverhead(candidate, []*corev1.Pod{
		daemonPod("old", "1", corev1.NodeSelectorRequirement{Key: "example.com/gen", Operator: corev1.NodeSelectorOpLt, Values: []string{"5"}}),
		daemonPod("new", "2", corev1.NodeSelectorRequirement{Key: "example.com/gen", Operator: corev1.NodeSelectorOpGt, Values: []string{"4"}}),
	})
	if truncated {
		t.Fatal("unexpected truncation")
	}
	expectOverhead(t, got, "3", 2)
}

func TestComputeDaemonOverheadTruncatesLargeRealizationSpaces(t *testing.T) {
	candidate := scheduling.NewRequirements()
	var pods []*corev1.Pod
	// four custom keys with 20 named values each -> (20 + other + absent)^4 realizations, far beyond the limit
	for k := 0; k < 4; k++ {
		key := fmt.Sprintf("example.com/k%d", k)
		for v := 0; v < 20; v++ {
			pods = append(pods, daemonPod(fmt.Sprintf("d-%d-%d", k, v), "1", in(key, fmt.Sprintf("v%d", v))))
		}
	}
	got, truncated := computeDaemonOverhead(candidate, pods)
	if !truncated {
		t.Fatal("expected truncation")
	}
	expectOverhead(t, got, "80", 80)
}

func TestComputeDaemonOverheadNoDaemons(t *testing.T) {
	got, truncated := computeDaemonOverhead(scheduling.NewRequirements(), nil)
	if got != nil || truncated {
		t.Fatalf("expected nil overhead, got %v (truncated=%v)", got, truncated)
	}
}
