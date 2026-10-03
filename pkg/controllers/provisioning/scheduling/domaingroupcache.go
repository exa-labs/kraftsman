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
	"encoding/binary"
	"hash/maphash"
	"sync"
	"time"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type domainGroupCacheContextKey struct{}

type instanceTypeRevisionsContextKey struct{}

// WithInstanceTypeRevisions attaches per-NodePool instance type revisions (from an
// InstanceTypesRevisionProvider) to the context. A NodePool present in the map is guaranteed by
// the provider to have identical instance type content whenever (UID, generation, revision) all
// match, letting the domain group fingerprint skip hashing that pool's requirement content.
func WithInstanceTypeRevisions(ctx context.Context, revisions map[string]uint64) context.Context {
	if len(revisions) == 0 {
		return ctx
	}
	return context.WithValue(ctx, instanceTypeRevisionsContextKey{}, revisions)
}

func instanceTypeRevisionsFromContext(ctx context.Context) map[string]uint64 {
	revisions, _ := ctx.Value(instanceTypeRevisionsContextKey{}).(map[string]uint64)
	return revisions
}

// DomainGroupCache memoizes buildDomainGroups results for one scheduling pass. Domain groups are a
// pure function of the NodePools and their instance types, both of which are candidate-invariant,
// so consolidation rebuilding a scheduler per candidate recomputes an identical result every time.
// The cached map is shared, never mutated after construction (Topology and TopologyGroup only read
// it via ForEachDomain), and the cache must not outlive a pass.
type DomainGroupCache struct {
	mu           sync.Mutex
	seed         maphash.Seed
	valid        bool
	fingerprint  uint64
	domainGroups map[string]TopologyDomainGroup
}

func NewDomainGroupCache() *DomainGroupCache {
	return &DomainGroupCache{seed: maphash.MakeSeed()}
}

func WithDomainGroupCache(ctx context.Context, cache *DomainGroupCache) context.Context {
	return context.WithValue(ctx, domainGroupCacheContextKey{}, cache)
}

func DomainGroupCacheFromContext(ctx context.Context) *DomainGroupCache {
	cache, _ := ctx.Value(domainGroupCacheContextKey{}).(*DomainGroupCache)
	return cache
}

// domainGroupsWithCache returns domain groups for the given inputs, reusing the pass-scoped cached
// result when the inputs are content-identical to the previous construction. Without a cache on the
// context, or when the inputs cannot be fingerprinted, behavior is identical to buildDomainGroups.
func domainGroupsWithCache(ctx context.Context, nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) map[string]TopologyDomainGroup {
	start := time.Now()
	defer func() {
		ConstructionPhaseDurationSeconds.Observe(time.Since(start).Seconds(), constructionPhaseLabels(ctx, phaseDomainGroups))
	}()
	cache := DomainGroupCacheFromContext(ctx)
	if cache == nil {
		return buildDomainGroups(nodePools, instanceTypes)
	}
	fingerprint, ok := cache.fingerprintInputs(instanceTypeRevisionsFromContext(ctx), nodePools, instanceTypes)
	if !ok {
		DomainGroupCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeBypass})
		return buildDomainGroups(nodePools, instanceTypes)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.valid && cache.fingerprint == fingerprint {
		DomainGroupCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return cache.domainGroups
	}
	DomainGroupCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	domainGroups := buildDomainGroups(nodePools, instanceTypes)
	cache.fingerprint = fingerprint
	cache.domainGroups = domainGroups
	cache.valid = true
	return domainGroups
}

// fingerprintInputs hashes everything buildDomainGroups consumes: the NodePool specs (requirements,
// labels, and taints are all under spec, so UID+Generation covers them) and the per-NodePool
// instance type requirements (which carry offering-derived domains such as zones and capacity
// types). Instance types are fetched from a generation-keyed provider cache, but content can change
// for the same generation across cache refills, so identity alone is not a safe key. When the
// provider reported a content revision for a NodePool, hashing that revision replaces hashing the
// pool's per-instance-type requirement content, which is what makes cache hits cheap at large
// instance type counts. Requirement values are hashed order-insensitively because set iteration
// order is not deterministic.
func (c *DomainGroupCache) fingerprintInputs(revisions map[string]uint64, nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) (uint64, bool) {
	var h maphash.Hash
	h.SetSeed(c.seed)
	revisionPools, contentPools := 0, 0
	for _, np := range nodePools {
		if np.UID == "" {
			return 0, false
		}
		h.WriteString(string(np.UID))
		h.WriteByte(0)
		writeUint64(&h, uint64(np.Generation)) //nolint:gosec
		its := instanceTypes[np.Name]
		writeUint64(&h, uint64(len(its)))
		if revision, ok := revisions[np.Name]; ok {
			revisionPools++
			h.WriteByte(1)
			writeUint64(&h, revision)
			continue
		}
		contentPools++
		h.WriteByte(0)
		for _, it := range its {
			h.WriteString(it.Name)
			h.WriteByte(0)
			writeUint64(&h, c.hashRequirements(it.Requirements))
		}
	}
	writeUint64(&h, uint64(len(instanceTypes)))
	DomainGroupFingerprintTotal.Inc(map[string]string{modeLabel: fingerprintMode(revisionPools, contentPools)})
	return h.Sum64(), true
}

func fingerprintMode(revisionPools, contentPools int) string {
	switch {
	case contentPools == 0 && revisionPools > 0:
		return fingerprintModeRevision
	case revisionPools == 0:
		return fingerprintModeContent
	default:
		return fingerprintModeMixed
	}
}

// hashRequirements produces an order-insensitive hash of a Requirements map by XOR-combining each
// requirement's content hash, since the map iterates in nondeterministic order.
func (c *DomainGroupCache) hashRequirements(requirements scheduling.Requirements) uint64 {
	var combined uint64
	for _, requirement := range requirements {
		combined ^= requirement.ContentHash64(c.seed)
	}
	return combined
}

func writeUint64(h *maphash.Hash, v uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	h.Write(buf[:])
}
