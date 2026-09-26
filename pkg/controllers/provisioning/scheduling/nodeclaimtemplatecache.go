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
	"hash/maphash"
	"maps"
	"math"
	"slices"
	"strconv"
	"sync"

	"github.com/samber/lo"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type nodeClaimTemplateCacheContextKey struct{}

// NodeClaimTemplateCache memoizes the per-NodePool NodeClaimTemplate construction and instance
// type pre-filtering done at the top of NewScheduler for one scheduling pass. Both are pure
// functions of the NodePool spec and its instance type list, which are candidate-invariant, so
// consolidation rebuilding a scheduler per candidate re-filters an identical instance type set
// every time. Fingerprinting combines NodePool identity (UID, generation) with the provider's
// instance type revision plus the minValues policy and new-capacity price limit that parameterize
// the filter; a NodePool without a UID or a revision bypasses the cache. The scheduler treats its templates as
// read-only (NewNodeClaim copies the struct and builds fresh Requirements before mutating), but
// hits still hand out a shallow struct copy with a cloned Requirements map so a cached template
// can never be aliased across schedulers. The cache must not outlive a pass.
type NodeClaimTemplateCache struct {
	mu      sync.Mutex
	seed    maphash.Seed
	entries map[string]nodeClaimTemplateCacheEntry
}

type nodeClaimTemplateCacheEntry struct {
	fingerprint uint64
	template    *NodeClaimTemplate // nil records a NodePool whose requirements filtered out all instance types
}

// nodeClaimTemplateFingerprintSeed is shared by every NodeClaimTemplateCache in the process so a template
// fingerprint identifies the same inputs across passes, which DaemonOverheadGroupStore relies on.
var nodeClaimTemplateFingerprintSeed = maphash.MakeSeed()

func NewNodeClaimTemplateCache() *NodeClaimTemplateCache {
	return &NodeClaimTemplateCache{seed: nodeClaimTemplateFingerprintSeed, entries: map[string]nodeClaimTemplateCacheEntry{}}
}

func WithNodeClaimTemplateCache(ctx context.Context, cache *NodeClaimTemplateCache) context.Context {
	return context.WithValue(ctx, nodeClaimTemplateCacheContextKey{}, cache)
}

func NodeClaimTemplateCacheFromContext(ctx context.Context) *NodeClaimTemplateCache {
	cache, _ := ctx.Value(nodeClaimTemplateCacheContextKey{}).(*NodeClaimTemplateCache)
	return cache
}

// nodeClaimTemplateWithCache returns the pre-filtered NodeClaimTemplate for a NodePool, reusing
// the pass-scoped cached result when the NodePool's identity and instance type revision are
// unchanged. build runs the full construction (including recorder events and logging for pools
// whose requirements filter out every instance type); cached negative results skip those
// emissions, which otherwise repeat identically for every candidate in a pass.
// A non-zero price limit filters the instance types the template is built from, so entries are keyed by limit as
// well as NodePool: consolidation interleaves unlimited simulations with price-limited split retries, and a single
// key per NodePool would make the two evict each other on every candidate.
func nodeClaimTemplateWithCache(ctx context.Context, np *v1.NodePool, instanceTypes []*cloudprovider.InstanceType, minValuesPolicy karpopts.MinValuesPolicy, priceLimit float64, build func() *NodeClaimTemplate) (*NodeClaimTemplate, bool) {
	cache := NodeClaimTemplateCacheFromContext(ctx)
	if cache == nil {
		nct := build()
		return nct, nct != nil
	}
	fingerprint, ok := cache.fingerprintInputs(instanceTypeRevisionsFromContext(ctx), np, instanceTypes, minValuesPolicy, priceLimit)
	if !ok {
		NodeClaimTemplateCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeBypass})
		nct := build()
		return nct, nct != nil
	}
	key := nodeClaimTemplateCacheKey(np.Name, priceLimit)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := cache.entries[key]; ok && entry.fingerprint == fingerprint {
		NodeClaimTemplateCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		if entry.template == nil {
			return nil, false
		}
		return copyNodeClaimTemplate(entry.template), true
	}
	NodeClaimTemplateCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	nct := build()
	var cached *NodeClaimTemplate
	if nct != nil {
		nct.cacheFingerprint = fingerprint
		nct.cacheFingerprintValid = true
		cached = copyNodeClaimTemplate(nct)
	}
	cache.entries[key] = nodeClaimTemplateCacheEntry{fingerprint: fingerprint, template: cached}
	return nct, nct != nil
}

func nodeClaimTemplateCacheKey(nodePoolName string, priceLimit float64) string {
	if priceLimit <= 0 {
		return nodePoolName
	}
	return nodePoolName + "|" + strconv.FormatFloat(priceLimit, 'g', -1, 64)
}

// instanceTypesBelowPrice returns the instance types that can launch below limit under requirements, preserving
// order. An instance type that can't launch below limit can never be part of a replacement that beats a candidate
// priced at limit, so dropping it up front is what forces the scheduler to pack the candidate's pods onto several
// smaller nodes instead of one node of the candidate's own type. Only offerings the requirements admit count: a
// zone-pinned or on-demand-only NodePool must not keep a candidate-sized type because of a cheap offering it can
// never launch, which would let the type absorb every pod again and suppress the split. Instance types with no
// offerings at all are retained since there is no price to judge them by.
func instanceTypesBelowPrice(instanceTypes []*cloudprovider.InstanceType, requirements scheduling.Requirements, limit float64) []*cloudprovider.InstanceType {
	if limit <= 0 {
		return instanceTypes
	}
	filtered := make([]*cloudprovider.InstanceType, 0, len(instanceTypes))
	for _, it := range instanceTypes {
		if len(it.Offerings) == 0 || lo.SomeBy(it.Offerings.Available().Compatible(requirements), func(o *cloudprovider.Offering) bool {
			return o.Price < limit
		}) {
			filtered = append(filtered, it)
		}
	}
	return filtered
}

// copyNodeClaimTemplate returns a shallow struct copy with fresh mutable containers: the
// Requirements map, the InstanceTypeOptions slice (sorted in place by OrderByPrice during
// NodeClaim finalization), and the ObjectMeta Labels/Annotations maps (the scheduler writes the
// min-values-relaxed annotation into a NodeClaim's shared map in place). Each *Requirement is
// struct-copied because the best-effort minValues relaxation path writes MinValues on the
// requirement in place (Requirements.Add stores incoming pointers verbatim when the key is
// absent, so the template's requirement objects flow into per-NodeClaim maps); the inner value
// sets and *InstanceType values are never mutated in place and are shared.
func copyNodeClaimTemplate(nct *NodeClaimTemplate) *NodeClaimTemplate {
	out := *nct
	out.InstanceTypeOptions = slices.Clone(nct.InstanceTypeOptions)
	out.Labels = maps.Clone(nct.Labels)
	out.Annotations = maps.Clone(nct.Annotations)
	out.Requirements = scheduling.NewRequirements()
	for _, r := range nct.Requirements {
		cr := *r
		out.Requirements[cr.Key] = &cr
	}
	return &out
}

// fingerprintInputs hashes the inputs that determine a NodePool's pre-filtered template: the
// NodePool identity (UID, generation) covering its spec, the provider instance type revision
// (which only guarantees identical content for the same UID+generation) with the list length as
// a cross-check, and the minValues policy that changes filter behavior.
func (c *NodeClaimTemplateCache) fingerprintInputs(revisions map[string]uint64, np *v1.NodePool, instanceTypes []*cloudprovider.InstanceType, minValuesPolicy karpopts.MinValuesPolicy, priceLimit float64) (uint64, bool) {
	revision, ok := revisions[np.Name]
	if !ok || np.UID == "" {
		return 0, false
	}
	var h maphash.Hash
	h.SetSeed(c.seed)
	h.WriteString(string(np.UID))
	h.WriteByte(0)
	writeUint64(&h, uint64(np.Generation)) //nolint:gosec
	writeUint64(&h, uint64(len(instanceTypes)))
	writeUint64(&h, revision)
	h.WriteString(string(minValuesPolicy))
	writeUint64(&h, math.Float64bits(priceLimit))
	return h.Sum64(), true
}
