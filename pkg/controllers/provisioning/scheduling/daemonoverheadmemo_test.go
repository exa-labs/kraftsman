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

// Equivalence tests for the memoized daemon overhead walk: computeDaemonOverheadWithMemo must return
// exactly what summing every realization's accepted daemon pods from scratch returns, including the
// formatting of each quantity, since the overhead is part of the instance type grouping key.

package scheduling

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// referenceDaemonAlternatives builds a daemon pod's node selection alternatives from scratch for every candidate.
func referenceDaemonAlternatives(candidate scheduling.Requirements, p *corev1.Pod) []scheduling.Requirements {
	labels := scheduling.NewLabelRequirements(p.Spec.NodeSelector)
	var terms []corev1.NodeSelectorTerm
	if affinity := p.Spec.Affinity; affinity != nil && affinity.NodeAffinity != nil && affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		terms = affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	}
	if len(terms) == 0 {
		return []scheduling.Requirements{labels}
	}
	alternatives := lo.FilterMap(terms, func(term corev1.NodeSelectorTerm, _ int) (scheduling.Requirements, bool) {
		alternative := scheduling.NewRequirements(labels.Values()...)
		alternative.Add(scheduling.NewNodeSelectorRequirements(term.MatchExpressions...).Values()...)
		return alternative, candidate.IsCompatible(alternative, scheduling.AllowUndefinedWellKnownLabels)
	})
	if len(alternatives) == 0 {
		return []scheduling.Requirements{scheduling.NewStrictPodRequirements(p)}
	}
	return alternatives
}

// referenceDaemonOverhead is the realization walk without memoized requests or skipped realizations: every
// realization recomputes the effective requests of the pods it accepts and folds their sum into the maximum.
func referenceDaemonOverhead(candidate scheduling.Requirements, daemonPods []*corev1.Pod) (corev1.ResourceList, bool) {
	if len(daemonPods) == 0 {
		return nil, false
	}
	alternatives := lo.Map(daemonPods, func(p *corev1.Pod, _ int) []scheduling.Requirements {
		return referenceDaemonAlternatives(candidate, p)
	})
	daemonRequirements := lo.Flatten(alternatives)
	var keys []string
	var domains [][]labelValue
	realizations := 1
	for _, key := range partitioningLabelKeys(daemonRequirements) {
		domain := realizationDomain(key, candidate, daemonRequirements)
		if len(domain) == 0 {
			continue
		}
		realizations *= len(domain)
		if realizations > daemonOverheadRealizationLimit {
			return resources.RequestsForPods(daemonPods...), true
		}
		keys = append(keys, key)
		domains = append(domains, domain)
	}
	if len(keys) == 0 {
		return resources.RequestsForPods(daemonPods...), false
	}
	realization := make([]labelValue, len(keys))
	overhead := corev1.ResourceList{}
	var walk func(depth int)
	walk = func(depth int) {
		if depth == len(keys) {
			accepted := lo.Filter(daemonPods, func(_ *corev1.Pod, i int) bool {
				return lo.SomeBy(alternatives[i], func(alternative scheduling.Requirements) bool {
					return acceptsRealization(alternative, keys, realization)
				})
			})
			if len(accepted) != 0 {
				overhead = resources.MaxResources(overhead, resources.RequestsForPods(accepted...))
			}
			return
		}
		for _, value := range domains[depth] {
			realization[depth] = value
			walk(depth + 1)
		}
	}
	walk(0)
	return overhead, false
}

// expectSameOverhead fails unless got and want hold the same resources with identical quantities and formatting.
func expectSameOverhead(t *testing.T, context string, got, want corev1.ResourceList, gotTruncated, wantTruncated bool) {
	t.Helper()
	if gotTruncated != wantTruncated {
		t.Fatalf("%s: truncated %t, reference %t", context, gotTruncated, wantTruncated)
	}
	if len(got) != len(want) || (got == nil) != (want == nil) {
		t.Fatalf("%s: got %v, reference %v", context, got, want)
	}
	for name, quantity := range want {
		g, ok := got[name]
		if !ok || g.Cmp(quantity) != 0 || g.String() != quantity.String() {
			t.Fatalf("%s: %s is %s, reference %s", context, name, g.String(), quantity.String())
		}
	}
}

func TestComputeDaemonOverheadMatchesReferenceWalkOnRandomTemplates(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	g := propertyGen{rand.New(rand.NewSource(seed))} //nolint:gosec
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	for range 200 {
		its := g.instanceTypes()
		daemons := lo.Times(g.Intn(10), g.daemon)
		for i := range 3 {
			nct := g.template(fmt.Sprintf("pool-%d", i), its)
			candidates := daemonPodTemplateCandidates(ctx, nct, daemons)
			requests := newDaemonPodMemo()
			for _, it := range nct.InstanceTypeOptions {
				compatible := lo.FilterMap(candidates, func(c daemonPodCandidate, _ int) (*corev1.Pod, bool) { return c.pod, c.fits(it) })
				candidate := candidateRequirements(nct, it)
				got, gotTruncated := computeDaemonOverheadWithMemo(candidate, compatible, requests)
				want, wantTruncated := referenceDaemonOverhead(candidate, compatible)
				expectSameOverhead(t, fmt.Sprintf("seed %d, template %v, instance type %s", seed, nct.Requirements, it.Name), got, want, gotTruncated, wantTruncated)
			}
		}
	}
}

func TestComputeDaemonOverheadMatchesReferenceWalkOnFleet(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	fleet := newConstructionFleet(16, 240)
	for _, nct := range fleet.templates(ctx) {
		candidates := daemonPodTemplateCandidates(ctx, nct, fleet.daemonSetPods)
		requests := newDaemonPodMemo()
		for _, it := range nct.InstanceTypeOptions {
			compatible := lo.FilterMap(candidates, func(c daemonPodCandidate, _ int) (*corev1.Pod, bool) { return c.pod, c.fits(it) })
			candidate := candidateRequirements(nct, it)
			got, gotTruncated := computeDaemonOverheadWithMemo(candidate, compatible, requests)
			want, wantTruncated := referenceDaemonOverhead(candidate, compatible)
			expectSameOverhead(t, fmt.Sprintf("template %s, instance type %s", nct.NodePoolName, it.Name), got, want, gotTruncated, wantTruncated)
		}
	}
}

// Quantities that compare equal but are written differently must come out of the walk exactly as the reference
// walk writes them: the first realization to reach the maximum wins, whichever realizations are skipped.
func TestComputeDaemonOverheadKeepsFirstMaximumFormatting(t *testing.T) {
	candidate := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, "r1", "r2", "r3"))
	milli := daemonPod("milli", "1000m", in(corev1.LabelTopologyRegion, "r1"))
	whole := daemonPod("whole", "1", in(corev1.LabelTopologyRegion, "r2"))
	again := daemonPod("again", "1000m", in(corev1.LabelTopologyRegion, "r1"))
	pods := []*corev1.Pod{milli, whole, again}
	got, gotTruncated := computeDaemonOverheadWithMemo(candidate, pods, newDaemonPodMemo())
	want, wantTruncated := referenceDaemonOverhead(candidate, pods)
	expectSameOverhead(t, "mixed formatting", got, want, gotTruncated, wantTruncated)
}

func TestDaemonPodRequestsSumDoesNotAliasTheMemo(t *testing.T) {
	requests := newDaemonPodMemo()
	pod := daemonPod("a", "100m")
	first := requests.sum([]*corev1.Pod{pod})
	first[corev1.ResourceCPU] = resource.MustParse("5")
	cpu := first[corev1.ResourceCPU]
	cpu.Add(resource.MustParse("1"))
	second := requests.sum([]*corev1.Pod{pod})
	if second.Cpu().MilliValue() != 100 {
		t.Fatalf("expected a mutated sum to leave the memoized requests alone, got %s", second.Cpu())
	}
	expectSameOverhead(t, "sum", requests.sum([]*corev1.Pod{pod, pod}), resources.RequestsForPods(pod, pod), false, false)
}
