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

// This file decides which domain a self-selecting required pod affinity bootstraps in when no matching pod has
// scheduled yet. A new NodeClaim ranks the candidate domains by the cheapest offering it could launch there, and
// retries the next candidate when the chosen one leaves it nothing to launch.

package scheduling

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"slices"

	"k8s.io/apimachinery/pkg/util/sets"
)

// DomainPriceFunc returns, for a topology key, the cheapest price at which a new node could launch in each domain of
// that key. Domains it omits have no known price.
type DomainPriceFunc func(topologyKey string) map[string]float64

// BootstrapPreference steers the domain a self-selecting pod affinity bootstraps in on a node that is not yet pinned to
// one domain. A nil *BootstrapPreference keeps no prices, excludes nothing and records nothing.
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
	return p != nil && len(p.chosen) != 0
}

// ExcludeChosen removes the domains the last evaluation chose from further consideration and resets the choice.
func (p *BootstrapPreference) ExcludeChosen() {
	for key, domain := range p.chosen {
		if p.excluded[key] == nil {
			p.excluded[key] = sets.New[string]()
		}
		p.excluded[key].Insert(domain)
	}
	clear(p.chosen)
}

// ResetChosen forgets the choices of the last evaluation.
func (p *BootstrapPreference) ResetChosen() {
	if p != nil {
		clear(p.chosen)
	}
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
	if p == nil || p.prices == nil {
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
	_ = binary.Write(h, binary.LittleEndian, seed)
	_, _ = h.Write([]byte(domain))
	return h.Sum64()
}
