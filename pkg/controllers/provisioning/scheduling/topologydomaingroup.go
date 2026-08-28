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
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// TopologyDomainSource is a NodePool that can supply a domain, along with the taints and
// requirements a node launched from that NodePool would carry. Taints answer whether a pod with
// NodeTaintsPolicy honor counts the domain; requirements answer the same for NodeAffinityPolicy.
type TopologyDomainSource struct {
	Taints       []v1.Taint
	Requirements scheduling.Requirements
}

// TopologyDomainGroup tracks the domains for a single topology, keyed by domain and then by the
// name of each NodePool that can supply it.
type TopologyDomainGroup map[string]map[string]TopologyDomainSource

func NewTopologyDomainGroup() TopologyDomainGroup {
	return TopologyDomainGroup{}
}

// Insert records that nodePool can supply domain, on nodes carrying the given taints and
// requirements.
func (t TopologyDomainGroup) Insert(domain string, nodePool string, taints []v1.Taint, requirements scheduling.Requirements) {
	sources, ok := t[domain]
	if !ok {
		sources = map[string]TopologyDomainSource{}
		t[domain] = sources
	}
	sources[nodePool] = TopologyDomainSource{Taints: taints, Requirements: requirements}
}

// ForEachDomain calls f on each domain tracked by the topology group that the pod could actually
// land in, given at least one NodePool supplying that domain. If the taint policy is honor, the pod
// must tolerate that NodePool's taints; if the affinity policy is honor, the pod's node selector and
// required node affinity must be compatible with the NodePool.
//
// Honoring affinity here is what keeps a spread's global minimum meaningful in a cluster whose
// NodePools do not all offer the same domains. A pod pinned to one NodePool would otherwise count
// every domain reachable only through the other pools (for example the zones of a pool spanning
// another region), each with a pod count of zero, pinning the global minimum at zero and leaving no
// domain within maxSkew of it, so a DoNotSchedule spread could never be satisfied.
func (t TopologyDomainGroup) ForEachDomain(pod *v1.Pod, nodeFilter TopologyNodeFilter, f func(domain string)) {
	for domain, sources := range t {
		for _, source := range sources {
			if nodeFilter.TaintPolicy != v1.NodeInclusionPolicyIgnore {
				// Perf Note: We could consider hashing the pod's tolerations and using that to look up a set of
				// tolerated domains.
				if err := scheduling.Taints(source.Taints).ToleratesPod(pod); err != nil {
					continue
				}
			}
			if nodeFilter.AffinityPolicy == v1.NodeInclusionPolicyHonor &&
				!nodeFilter.matchesRequirements(source.Requirements, scheduling.AllowUndefinedWellKnownLabels) {
				continue
			}
			f(domain)
			break
		}
	}
}
