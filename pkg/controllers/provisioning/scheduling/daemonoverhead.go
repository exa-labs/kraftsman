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
// node satisfying the candidate has to host.

package scheduling

import (
	"sort"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
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

// computeDaemonOverhead returns the resources a node satisfying candidate must reserve for daemonPods, every one of
// which is individually compatible with candidate. See the file header for the model. truncated reports that the
// realization space exceeded daemonOverheadRealizationLimit and the plain sum was reserved instead.
func computeDaemonOverhead(candidate scheduling.Requirements, daemonPods []*corev1.Pod) (overhead corev1.ResourceList, truncated bool) {
	if len(daemonPods) == 0 {
		return nil, false
	}
	daemonRequirements := lo.Map(daemonPods, func(p *corev1.Pod, _ int) scheduling.Requirements {
		return scheduling.NewStrictPodRequirements(p)
	})
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
	overhead = corev1.ResourceList{}
	var walk func(depth int)
	walk = func(depth int) {
		if depth == len(keys) {
			accepted := lo.Filter(daemonPods, func(_ *corev1.Pod, i int) bool {
				return acceptsRealization(daemonRequirements[i], keys, realization)
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

// partitioningLabelKeys returns, sorted, the label keys on which at least two of the daemon pods place differing
// requirements. Only these keys can make daemon pods mutually exclusive; a key every constraining daemon pod agrees
// on admits the same pods in every realization.
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
