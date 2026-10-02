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

// Daemon overhead for a NodeClaim candidate.
//
// A NodeClaim template is broad: it permits many values for labels such as topology.kubernetes.io/region or
// topology.kubernetes.io/zone. The node it becomes carries exactly one value per label, so daemon pods whose node
// selectors disagree on a label (`region In [us-east-1]` vs `region In [us-west-2]`) never share a node. Summing
// every daemon pod that is individually compatible with the template therefore over-reserves, which pushes
// NodeClaims onto larger, more expensive instance types than the workload needs.
//
// computeDaemonOverhead instead enumerates the realizations of the disagreeing labels that the candidate permits,
// sums the daemon pods accepting each realization, and reserves the element-wise maximum across realizations:
//
//	overhead(candidate) = max over realizations r of  sum over daemon pods d accepting r of  requests(d)
//
// Daemon pods that genuinely co-schedule are still summed, and the result is never lower than what any concrete
// node satisfying the candidate has to host. A daemon pod with several required node affinity terms (which
// Kubernetes ORs) accepts a realization when any of its terms does.

package scheduling

import (
	"sort"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// daemonOverheadRealizationLimit bounds the number of label realizations enumerated for a single candidate. Beyond
// it the plain sum over every compatible daemon pod is reserved, which is always sufficient.
const daemonOverheadRealizationLimit = 4096

// labelValue is one realization of a single label on a concrete node: either a concrete value or the label being
// absent from the node.
type labelValue struct {
	value  string
	absent bool
}

// candidateRequirements returns the requirements a node launched from the template as the given instance type
// satisfies: the intersection of the template's requirements with the instance type's.
func candidateRequirements(nct *NodeClaimTemplate, it *cloudprovider.InstanceType) scheduling.Requirements {
	candidate := scheduling.NewRequirements(nct.Requirements.Values()...)
	candidate.Add(it.Requirements.Values()...)
	return candidate
}

// daemonAlternatives returns the node selection alternatives under which a daemon pod schedules onto a node
// satisfying candidate. Kubernetes ORs the RequiredDuringScheduling node selector terms, so each term compatible with
// candidate, combined with the pod's nodeSelector, is one alternative. Falls back to the pod's strict requirements
// (its first term) when no term is compatible, which is how the caller established compatibility.
func daemonAlternatives(candidate scheduling.Requirements, p *corev1.Pod) []scheduling.Requirements {
	return newDaemonNodeSelection(p).alternatives(candidate)
}

// daemonNodeSelection holds the candidate-independent half of daemonAlternatives for one daemon pod: each required
// node affinity term combined with the pod's nodeSelector (or the nodeSelector alone when the pod has no terms), and
// the pod's strict requirements. Its requirement sets are shared by every candidate and are only ever read.
type daemonNodeSelection struct {
	// terms holds one requirement set per required node affinity term, or the nodeSelector alone when hasTerms is false.
	terms    []scheduling.Requirements
	hasTerms bool
	strict   scheduling.Requirements
}

func newDaemonNodeSelection(p *corev1.Pod) daemonNodeSelection {
	labels := scheduling.NewLabelRequirements(p.Spec.NodeSelector)
	var terms []corev1.NodeSelectorTerm
	if affinity := p.Spec.Affinity; affinity != nil && affinity.NodeAffinity != nil && affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		terms = affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	}
	if len(terms) == 0 {
		return daemonNodeSelection{terms: []scheduling.Requirements{labels}}
	}
	return daemonNodeSelection{
		terms: lo.Map(terms, func(term corev1.NodeSelectorTerm, _ int) scheduling.Requirements {
			alternative := scheduling.NewRequirements(labels.Values()...)
			alternative.Add(scheduling.NewNodeSelectorRequirements(term.MatchExpressions...).Values()...)
			return alternative
		}),
		hasTerms: true,
		strict:   scheduling.NewStrictPodRequirements(p),
	}
}

// alternatives returns the pod's node selection alternatives for candidate; see daemonAlternatives.
func (s daemonNodeSelection) alternatives(candidate scheduling.Requirements) []scheduling.Requirements {
	if !s.hasTerms {
		return s.terms
	}
	alternatives := lo.Filter(s.terms, func(alternative scheduling.Requirements, _ int) bool {
		return candidate.IsCompatible(alternative, scheduling.AllowUndefinedWellKnownLabels)
	})
	if len(alternatives) == 0 {
		return []scheduling.Requirements{s.strict}
	}
	return alternatives
}

// daemonPodMemo memoizes the per-pod inputs of computeDaemonOverhead for one overhead group build: each daemon pod's
// effective requests (resources.Ceiling) and its node selection. Every instance type of a template walks the same
// daemon pods, summing their requests once per label realization, and recomputing these per instance type and per
// realization dominated the build. Neither depends on the instance type or the realization, so the memo returns
// exactly what recomputing them would.
type daemonPodMemo struct {
	requests  map[*corev1.Pod]corev1.ResourceList
	selection map[*corev1.Pod]daemonNodeSelection
}

func newDaemonPodMemo() *daemonPodMemo {
	return &daemonPodMemo{requests: map[*corev1.Pod]corev1.ResourceList{}, selection: map[*corev1.Pod]daemonNodeSelection{}}
}

// requestsOf returns the effective requests of p. The returned list is shared and must not be mutated.
func (m *daemonPodMemo) requestsOf(p *corev1.Pod) corev1.ResourceList {
	if requests, ok := m.requests[p]; ok {
		return requests
	}
	requests := resources.Ceiling(p).Requests
	m.requests[p] = requests
	return requests
}

// sum returns resources.RequestsForPods(pods...), merging the memoized per-pod requests in the same order.
func (m *daemonPodMemo) sum(pods []*corev1.Pod) corev1.ResourceList {
	lists := make([]corev1.ResourceList, len(pods))
	for i, p := range pods {
		lists[i] = m.requestsOf(p)
	}
	merged := resources.Merge(lists...)
	merged[corev1.ResourcePods] = *resource.NewQuantity(int64(len(pods)), resource.DecimalExponent)
	return merged
}

// alternatives returns daemonAlternatives(candidate, p) from the memoized node selection of p.
func (m *daemonPodMemo) alternatives(candidate scheduling.Requirements, p *corev1.Pod) []scheduling.Requirements {
	selection, ok := m.selection[p]
	if !ok {
		selection = newDaemonNodeSelection(p)
		m.selection[p] = selection
	}
	return selection.alternatives(candidate)
}

// computeDaemonOverhead returns the resources a node satisfying candidate must reserve for daemonPods, every one of
// which is individually compatible with candidate. See the file header for the model. truncated reports that the
// realization space exceeded daemonOverheadRealizationLimit and the plain sum was reserved instead.
func computeDaemonOverhead(candidate scheduling.Requirements, daemonPods []*corev1.Pod) (overhead corev1.ResourceList, truncated bool) {
	return computeDaemonOverheadWithMemo(candidate, daemonPods, newDaemonPodMemo())
}

// computeDaemonOverheadWithMemo is computeDaemonOverhead reading per-pod inputs through a memo shared by the caller.
// Realizations that accept a set of daemon pods an earlier realization already accepted are skipped: the element-wise
// maximum already covers that set's sum, so folding it in again cannot change the result.
func computeDaemonOverheadWithMemo(candidate scheduling.Requirements, daemonPods []*corev1.Pod, memo *daemonPodMemo) (overhead corev1.ResourceList, truncated bool) {
	if len(daemonPods) == 0 {
		return nil, false
	}
	alternatives := lo.Map(daemonPods, func(p *corev1.Pod, _ int) []scheduling.Requirements {
		return memo.alternatives(candidate, p)
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
			return memo.sum(daemonPods), true
		}
		keys = append(keys, key)
		domains = append(domains, domain)
	}
	if len(keys) == 0 {
		return memo.sum(daemonPods), false
	}
	realization := make([]labelValue, len(keys))
	overhead = corev1.ResourceList{}
	folded := sets.New[string]()
	acceptedKey := make([]byte, len(daemonPods))
	var walk func(depth int)
	walk = func(depth int) {
		if depth == len(keys) {
			accepted := lo.Filter(daemonPods, func(_ *corev1.Pod, i int) bool {
				ok := lo.SomeBy(alternatives[i], func(alternative scheduling.Requirements) bool {
					return acceptsRealization(alternative, keys, realization)
				})
				acceptedKey[i] = lo.Ternary[byte](ok, 1, 0)
				return ok
			})
			if len(accepted) == 0 || folded.Has(string(acceptedKey)) {
				return
			}
			folded.Insert(string(acceptedKey))
			overhead = resources.MaxResources(overhead, memo.sum(accepted))
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

// partitioningLabelKeys returns, sorted, the label keys on which at least two of the daemon alternatives place
// differing requirements. Only these keys can make daemon pods mutually exclusive; a key every constraining
// alternative agrees on admits the same pods in every realization.
func partitioningLabelKeys(daemonRequirements []scheduling.Requirements) []string {
	byKey := map[string][]*scheduling.Requirement{}
	for _, reqs := range daemonRequirements {
		for key, req := range reqs {
			byKey[key] = append(byKey[key], req)
		}
	}
	var keys []string
	for key, reqs := range byKey {
		differs := lo.SomeBy(reqs, func(req *scheduling.Requirement) bool {
			return !req.SubsetOf(reqs[0]) || !reqs[0].SubsetOf(req)
		})
		if differs {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// realizationDomain returns the distinct realizations of key worth enumerating for candidate: every value a daemon
// pod names that candidate permits, one representative for any other value candidate permits (indistinguishable to
// the daemon pods), and the label being absent when candidate does not require it. It returns nil when a numeric
// (Gt/Lt) requirement is involved, since concrete values cannot then be collapsed into a representative; the caller
// treats the key as non-partitioning, which over-reserves rather than under-reserves.
func realizationDomain(key string, candidate scheduling.Requirements, daemonRequirements []scheduling.Requirements) []labelValue {
	candidateReq := candidate.Get(key)
	if hasNumericBounds(candidateReq) {
		return nil
	}
	named := sets.New[string]()
	for _, reqs := range daemonRequirements {
		req, ok := reqs[key]
		if !ok {
			continue
		}
		if hasNumericBounds(req) {
			return nil
		}
		for _, value := range req.Values() {
			if candidateReq.Has(value) {
				named.Insert(value)
			}
		}
	}
	domain := lo.Map(sets.List(named), func(value string, _ int) labelValue { return labelValue{value: value} })
	switch candidateReq.Operator() {
	case corev1.NodeSelectorOpIn:
		values := candidateReq.Values()
		sort.Strings(values)
		if other, ok := lo.Find(values, func(value string) bool { return !named.Has(value) }); ok {
			domain = append(domain, labelValue{value: other})
		}
	case corev1.NodeSelectorOpNotIn, corev1.NodeSelectorOpExists:
		// Label values cannot contain NUL, so this never collides with a value a daemon pod names.
		domain = append(domain, labelValue{value: "\x00other"})
	}
	if candidateAllowsAbsent(key, candidate) {
		domain = append(domain, labelValue{absent: true})
	}
	return domain
}

// candidateAllowsAbsent reports whether a node satisfying candidate may lack the label entirely. Well-known labels
// are always set by the cloud provider, matching the AllowUndefinedWellKnownLabels compatibility semantics.
func candidateAllowsAbsent(key string, candidate scheduling.Requirements) bool {
	if !candidate.Has(key) {
		return !v1.WellKnownLabels.Has(key)
	}
	op := candidate.Get(key).Operator()
	return op == corev1.NodeSelectorOpNotIn || op == corev1.NodeSelectorOpDoesNotExist
}

// acceptsRealization reports whether a daemon pod with the given requirements schedules onto a node carrying the
// realization (indexed like keys) of the enumerated labels. Labels the pod does not constrain are irrelevant.
func acceptsRealization(reqs scheduling.Requirements, keys []string, realization []labelValue) bool {
	for i, key := range keys {
		req, ok := reqs[key]
		if !ok {
			continue
		}
		if realization[i].absent {
			// NotIn and DoesNotExist are the only node selector operators satisfied by a missing label.
			if op := req.Operator(); op != corev1.NodeSelectorOpNotIn && op != corev1.NodeSelectorOpDoesNotExist {
				return false
			}
			continue
		}
		if !req.Has(realization[i].value) {
			return false
		}
	}
	return true
}

// hasNumericBounds reports whether the requirement carries a Gt/Lt bound.
func hasNumericBounds(req *scheduling.Requirement) bool {
	op := req.NodeSelectorRequirement().Operator
	return op == v1.NodeSelectorOpGte || op == v1.NodeSelectorOpLte
}
