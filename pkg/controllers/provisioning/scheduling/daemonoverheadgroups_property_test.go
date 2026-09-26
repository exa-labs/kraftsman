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
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// referenceDaemonPodCompatible is the direct per-instance-type statement of daemon compatibility: the pod tolerates
// the template's taints (PreferNoSchedule always tolerated) and some required node affinity relaxation step is
// compatible with the template and intersects the instance type.
func referenceDaemonPodCompatible(ctx context.Context, nct *NodeClaimTemplate, it *cloudprovider.InstanceType, p *corev1.Pod) bool {
	if podutils.HasDRARequirements(p) && operatoroptions.FromContext(ctx).IgnoreDRARequests {
		return false
	}
	p = p.DeepCopy()
	preferences := &Preferences{}
	_ = preferences.toleratePreferNoScheduleTaints(p)
	if err := scheduling.Taints(nct.Spec.Taints).ToleratesPod(p); err != nil {
		return false
	}
	for {
		podRequirements := scheduling.NewStrictPodRequirements(p)
		if nct.Requirements.IsCompatible(podRequirements, scheduling.AllowUndefinedWellKnownLabels) &&
			it.Requirements.Intersects(podRequirements) == nil {
			return true
		}
		if preferences.removeRequiredNodeAffinityTerm(p) == nil {
			return false
		}
	}
}

var (
	propertyLabelValues = map[string][]string{
		corev1.LabelTopologyZone:       {"test-zone-1", "test-zone-2", "test-zone-3", "test-zone-4"},
		corev1.LabelArchStable:         {"amd64", "arm64"},
		corev1.LabelOSStable:           {"linux", "windows"},
		corev1.LabelInstanceTypeStable: {"it-0", "it-1", "it-2", "it-3", "other"},
		v1.CapacityTypeLabelKey:        {"spot", "on-demand", "reserved"},
		v1.NodePoolLabelKey:            {"pool-0", "pool-1", "pool-2"},
		v1.NodeRegisteredLabelKey:      {"true", "false"},
		fake.LabelInstanceSize:         {"small", "large"},
		"custom/a":                     {"a1", "a2", "a3"},
		"custom/b":                     {"b1", "b2"},
		"custom/undefined":             {"u1"},
	}
	propertyLabelKeys = lo.Keys(propertyLabelValues)
	propertyTaints    = []corev1.Taint{
		{Key: "dedicated", Value: "a", Effect: corev1.TaintEffectNoSchedule},
		{Key: "dedicated", Value: "b", Effect: corev1.TaintEffectNoSchedule},
		{Key: "soft", Value: "x", Effect: corev1.TaintEffectPreferNoSchedule},
		{Key: "evict", Value: "y", Effect: corev1.TaintEffectNoExecute},
	}
)

type propertyGen struct{ *rand.Rand }

func (g propertyGen) pick(xs []string) string { return xs[g.Intn(len(xs))] }

func (g propertyGen) subset(xs []string) []string {
	out := lo.Filter(xs, func(string, int) bool { return g.Intn(2) == 0 })
	return lo.Ternary(len(out) == 0, []string{g.pick(xs)}, out)
}

func (g propertyGen) key() string {
	keys := append([]string{}, propertyLabelKeys...)
	sort.Strings(keys)
	return g.pick(keys)
}

func (g propertyGen) expression() corev1.NodeSelectorRequirement {
	key := g.key()
	op := []corev1.NodeSelectorOperator{corev1.NodeSelectorOpIn, corev1.NodeSelectorOpNotIn, corev1.NodeSelectorOpExists, corev1.NodeSelectorOpDoesNotExist}[g.Intn(4)]
	r := corev1.NodeSelectorRequirement{Key: key, Operator: op}
	if op == corev1.NodeSelectorOpIn || op == corev1.NodeSelectorOpNotIn {
		r.Values = g.subset(propertyLabelValues[key])
	}
	return r
}

func (g propertyGen) daemon(i int) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: fmt.Sprintf("ds-%d", i)},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprintf("%dm", 10*(g.Intn(20)+1)))}},
		}}},
	}
	for _, t := range propertyTaints {
		switch g.Intn(4) {
		case 0:
			p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: t.Key, Operator: corev1.TolerationOpEqual, Value: t.Value, Effect: t.Effect})
		case 1:
			p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: t.Key, Operator: corev1.TolerationOpExists})
		}
	}
	if g.Intn(8) == 0 {
		p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Operator: corev1.TolerationOpExists})
	}
	for range g.Intn(3) {
		key := g.key()
		p.Spec.NodeSelector = lo.Assign(p.Spec.NodeSelector, map[string]string{key: g.pick(propertyLabelValues[key])})
	}
	if terms := g.Intn(4); terms > 0 {
		p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: lo.Times(terms, func(int) corev1.NodeSelectorTerm {
				return corev1.NodeSelectorTerm{MatchExpressions: lo.Times(g.Intn(3), func(int) corev1.NodeSelectorRequirement { return g.expression() })}
			}),
		}}}
	}
	if g.Intn(5) == 0 {
		p.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "claim"}}
	}
	if g.Intn(4) == 0 {
		p.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: int32(8000 + g.Intn(3)), Protocol: corev1.ProtocolTCP}} //nolint:gosec
	}
	return p
}

func (g propertyGen) instanceTypes() []*cloudprovider.InstanceType {
	return lo.Times(g.Intn(4)+1, func(i int) *cloudprovider.InstanceType {
		var offerings []cloudprovider.Offering
		for _, zone := range g.subset(propertyLabelValues[corev1.LabelTopologyZone][:3]) {
			for _, capacityType := range g.subset(propertyLabelValues[v1.CapacityTypeLabelKey][:2]) {
				offerings = append(offerings, cloudprovider.Offering{
					Available:    g.Intn(5) != 0,
					Price:        1,
					Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: capacityType, corev1.LabelTopologyZone: zone}),
				})
			}
		}
		var reqs []*scheduling.Requirement
		for _, key := range []string{"custom/a", "custom/b"} {
			if g.Intn(2) == 0 {
				reqs = append(reqs, scheduling.NewRequirement(key, corev1.NodeSelectorOpIn, g.subset(propertyLabelValues[key])...))
			}
		}
		return fake.NewInstanceType(fmt.Sprintf("it-%d", i),
			fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(g.Intn(8) + 1))}),
			fake.WithArchitecture(g.pick(propertyLabelValues[corev1.LabelArchStable])),
			fake.WithOfferings(offerings...),
			fake.WithRequirements(reqs...),
		)
	})
}

func (g propertyGen) template(name string, its []*cloudprovider.InstanceType) *NodeClaimTemplate {
	nct := &NodeClaimTemplate{
		NodePoolName:        name,
		InstanceTypeOptions: its,
		Requirements: scheduling.NewRequirements(
			scheduling.NewRequirement(v1.NodePoolLabelKey, corev1.NodeSelectorOpIn, name),
			scheduling.NewRequirement(v1.NodeRegisteredLabelKey, corev1.NodeSelectorOpIn, "true"),
		),
	}
	for _, key := range []string{corev1.LabelTopologyZone, corev1.LabelArchStable, v1.CapacityTypeLabelKey, "custom/a", "custom/b"} {
		switch g.Intn(4) {
		case 0:
			nct.Requirements.Add(scheduling.NewRequirement(key, corev1.NodeSelectorOpIn, g.subset(propertyLabelValues[key])...))
		case 1:
			nct.Requirements.Add(scheduling.NewRequirement(key, corev1.NodeSelectorOpNotIn, g.subset(propertyLabelValues[key])...))
		case 2:
			nct.Requirements.Add(scheduling.NewRequirement(key, corev1.NodeSelectorOpExists))
		}
	}
	nct.Spec.Taints = lo.Filter(propertyTaints, func(corev1.Taint, int) bool { return g.Intn(3) == 0 })
	return nct
}

// canonicalGroups renders groups independently of group order and of *InstanceType identity.
func canonicalGroups(groups []DaemonOverheadGroup) string {
	lines := lo.Map(groups, func(g DaemonOverheadGroup, _ int) string {
		return fmt.Sprintf("%s|%s|%v", strings.Join(lo.Map(g.InstanceTypes, func(it *cloudprovider.InstanceType, _ int) string { return it.Name }), ","), resources.String(g.DaemonOverhead), g.HostPortUsage)
	})
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// referenceDaemonOverheadGroups groups instance types by the reference check with the same grouping rule as
// buildDaemonOverheadGroupsForTemplate.
func referenceDaemonOverheadGroups(ctx context.Context, nct *NodeClaimTemplate, daemons []*corev1.Pod) []DaemonOverheadGroup {
	groups := map[string]*DaemonOverheadGroup{}
	var order []string
	for _, it := range nct.InstanceTypeOptions {
		compatible := lo.Filter(daemons, func(p *corev1.Pod, _ int) bool { return referenceDaemonPodCompatible(ctx, nct, it, p) })
		overhead, _ := computeDaemonOverhead(candidateRequirements(nct, it), compatible)
		key := podSetKey(compatible) + "|" + resources.String(overhead)
		if g, ok := groups[key]; ok {
			g.InstanceTypes = append(g.InstanceTypes, it)
			continue
		}
		hostPortUsage := scheduling.NewHostPortUsage()
		for _, p := range compatible {
			hostPortUsage.Add(p, scheduling.GetHostPorts(p))
		}
		groups[key] = &DaemonOverheadGroup{InstanceTypes: []*cloudprovider.InstanceType{it}, DaemonOverhead: overhead, HostPortUsage: hostPortUsage}
		order = append(order, key)
	}
	return lo.Map(order, func(key string, _ int) DaemonOverheadGroup { return *groups[key] })
}

func TestDaemonOverheadGroupsMatchPerInstanceTypeCompatibility(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	g := propertyGen{rand.New(rand.NewSource(seed))} //nolint:gosec
	for range 300 {
		ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{IgnoreDRARequests: g.Intn(2) == 0})
		its := g.instanceTypes()
		daemons := lo.Times(g.Intn(8), g.daemon)
		before := lo.Map(daemons, func(p *corev1.Pod, _ int) *corev1.Pod { return p.DeepCopy() })
		for i := range 3 {
			nct := g.template(fmt.Sprintf("pool-%d", i), its)
			got, want := canonicalGroups(buildDaemonOverheadGroupsForTemplate(ctx, nct, daemons)), canonicalGroups(referenceDaemonOverheadGroups(ctx, nct, daemons))
			if got != want {
				t.Fatalf("seed %d: groups for template %v taints %v diverged\ngot:\n%s\nwant:\n%s", seed, nct.Requirements, nct.Spec.Taints, got, want)
			}
		}
		if !equality.Semantic.DeepEqual(daemons, before) {
			t.Fatalf("seed %d: building groups mutated the daemon pods", seed)
		}
	}
}
