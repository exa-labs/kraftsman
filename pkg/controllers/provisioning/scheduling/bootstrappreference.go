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

// This file chooses the domain a self-selecting required pod affinity bootstraps in when no matching pod has scheduled
// yet. BootstrapPreference carries that choice through topology evaluation: it orders the candidate domains cheapest
// first (orderBootstrapCandidates) by the cheapest offering a NodeClaim could launch in each (cheapestPriceByDomain),
// and remembers the domains to skip when NodeClaim.placeWithBootstrapFallback finds that a chosen domain leaves nothing
// to launch.

package scheduling

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// DomainPriceFunc returns, for a topology key, the cheapest price at which a new node could launch in each domain of
// that key. Domains it omits have no known price.
type DomainPriceFunc func(topologyKey string) map[string]float64

// BootstrapPreference steers the domain a self-selecting pod affinity bootstraps in on a node that is not yet pinned to
// one domain. Topology evaluation accepts a nil *BootstrapPreference, which keeps no prices, excludes nothing and
// records nothing.
//
// It carries state across the attempts of a single NodeClaim.CanAdd: the caller evaluates the topology, reads Chosen,
// and when the choice leaves nothing to launch it calls ExcludeChosen and evaluates again.
type BootstrapPreference struct {
	prices   DomainPriceFunc
	resolved map[string]map[string]float64
	excluded map[string]sets.Set[string]
	chosen   map[string]string
}

// NewBootstrapPreference returns a preference that ranks domains by prices, which is resolved at most once per key.
func NewBootstrapPreference(prices DomainPriceFunc) *BootstrapPreference {
	return &BootstrapPreference{
		prices:   prices,
		resolved: map[string]map[string]float64{},
		excluded: map[string]sets.Set[string]{},
		chosen:   map[string]string{},
	}
}

// Chosen reports whether the last evaluation bootstrapped a domain it had a choice about.
func (p *BootstrapPreference) Chosen() bool {
	return len(p.chosen) != 0
}

// ExcludeChosen removes the domains the last evaluation chose from further consideration and forgets the choice.
func (p *BootstrapPreference) ExcludeChosen() {
	for key, domain := range p.chosen {
		if p.excluded[key] == nil {
			p.excluded[key] = sets.New[string]()
		}
		p.excluded[key].Insert(domain)
	}
	clear(p.chosen)
}

// excludes reports whether domain of topologyKey was excluded by an earlier attempt.
func (p *BootstrapPreference) excludes(topologyKey, domain string) bool {
	return p != nil && p.excluded[topologyKey].Has(domain)
}

// record notes that the evaluation bootstrapped topologyKey in domain.
func (p *BootstrapPreference) record(topologyKey, domain string) {
	if p != nil {
		p.chosen[topologyKey] = domain
	}
}

// pricesFor returns the domain prices for topologyKey, resolving them on first use.
func (p *BootstrapPreference) pricesFor(topologyKey string) map[string]float64 {
	if p == nil {
		return nil
	}
	prices, ok := p.resolved[topologyKey]
	if !ok {
		prices = p.prices(topologyKey)
		p.resolved[topologyKey] = prices
	}
	return prices
}

// orderBootstrapCandidates sorts domains in place, cheapest first, with priced domains before unpriced ones. Equal
// prices, and unpriced domains, are ordered by a hash of tieSeed and the domain name: the order is stable across passes
// and processes, but groups with different seeds spread across equally priced domains instead of all taking the same one.
func orderBootstrapCandidates(domains []string, prices map[string]float64, tieSeed uint64) {
	ties := make(map[string]uint64, len(domains))
	for _, domain := range domains {
		ties[domain] = tieBreak(tieSeed, domain)
	}
	slices.SortFunc(domains, func(a, b string) int {
		priceA, pricedA := prices[a]
		priceB, pricedB := prices[b]
		switch {
		case pricedA && !pricedB:
			return -1
		case !pricedA && pricedB:
			return 1
		case pricedA && priceA != priceB:
			return cmp.Compare(priceA, priceB)
		}
		return cmp.Or(cmp.Compare(ties[a], ties[b]), cmp.Compare(a, b))
	})
}

// tieBreak is a stable 64-bit FNV-1a hash of seed and domain.
func tieBreak(seed uint64, domain string) uint64 {
	h := fnv.New64a()
	// Writing to a hash.Hash never returns an error.
	_ = binary.Write(h, binary.LittleEndian, seed)
	_, _ = h.Write([]byte(domain))
	return h.Sum64()
}

// cheapestPriceByDomain returns, for each value of topologyKey that offerings carry, the lowest price among the offerings
// in that domain that pod could launch on. It applies the same per-instance-type checks as
// filterInstanceTypesByRequirements (requirements, daemon host ports, requests plus daemon overhead) and then keeps the
// available offerings compatible with requirements that pass usable. Offering prices are the ones the scheduler sees, so
// they include any price overlays. Domains without such an offering are absent, as is every domain of a key offerings
// do not carry.
func cheapestPriceByDomain(
	instanceTypes []*cloudprovider.InstanceType,
	daemonOverheadGroups []DaemonOverheadGroup,
	pod *corev1.Pod,
	requirements scheduling.Requirements,
	requests corev1.ResourceList,
	topologyKey string,
	usable func(*cloudprovider.Offering) bool,
) map[string]float64 {
	prices := map[string]float64{}
	domains := newDomainMatcher(requirements, topologyKey)
	hostPorts := scheduling.GetHostPorts(pod)
	eligible := sets.New(instanceTypes...)
	for _, group := range daemonOverheadGroups {
		if group.HostPortUsage.Conflicts(pod, hostPorts) != nil {
			continue
		}
		total := resources.Merge(requests, group.DaemonOverhead)
		for _, it := range group.InstanceTypes {
			if eligible.Has(it) && compatible(it, requirements) {
				recordCheapestOfferings(prices, it, requirements, total, domains, usable)
			}
		}
	}
	return prices
}

// recordCheapestOfferings lowers prices[domain] to the price of each of the instance type's offerings in that domain
// that fits requests, is available, is compatible with requirements and passes usable.
func recordCheapestOfferings(
	prices map[string]float64,
	it *cloudprovider.InstanceType,
	requirements scheduling.Requirements,
	requests corev1.ResourceList,
	domains domainMatcher,
	usable func(*cloudprovider.Offering) bool,
) {
	for _, group := range it.AllocatableOfferingsList() {
		if !resources.Fits(requests, group.Allocatable) {
			continue
		}
		for _, o := range group.Offerings {
			domain, ok := domains.of(o)
			if !ok {
				continue
			}
			// Only offerings that would lower the domain's price need the full compatibility check.
			if price, priced := prices[domain]; priced && price <= o.Price {
				continue
			}
			if requirements.IsCompatible(o.Requirements, scheduling.AllowUndefinedWellKnownLabels) && usable(o) {
				prices[domain] = o.Price
			}
		}
	}
}

// domainMatcher finds the domain of topologyKey an offering is in, among the domains requirements allow.
type domainMatcher struct {
	topologyKey string
	// allowed lists the domains requirements allow, when they restrict topologyKey to a finite set; nil otherwise.
	allowed []string
}

// newDomainMatcher returns the matcher for topologyKey among the domains requirements allow.
func newDomainMatcher(requirements scheduling.Requirements, topologyKey string) domainMatcher {
	m := domainMatcher{topologyKey: topologyKey}
	if allowed, ok := requirements[topologyKey]; ok && allowed.Operator() == corev1.NodeSelectorOpIn {
		m.allowed = allowed.Values()
	}
	return m
}

// of returns the single value an available offering requires for the topology key, if it requires exactly one and the
// requirements allow it. It is a cheap filter ahead of the full compatibility check: with a finite set of allowed
// domains it probes the offering for each of them instead of materializing the offering's value.
func (m domainMatcher) of(o *cloudprovider.Offering) (string, bool) {
	if !o.Available {
		return "", false
	}
	domain, ok := o.Requirements[m.topologyKey]
	if !ok || domain.Operator() != corev1.NodeSelectorOpIn || domain.Len() != 1 {
		return "", false
	}
	if m.allowed == nil {
		return domain.Any(), true
	}
	for _, allowed := range m.allowed {
		if domain.Has(allowed) {
			return allowed, true
		}
	}
	return "", false
}
