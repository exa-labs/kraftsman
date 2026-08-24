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

package disruption

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/status"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
)

// NegativeResultCache remembers, across consolidation passes, candidates whose simulation ended
// in a no-op so an identical candidate can be skipped instead of re-simulated. Only negative
// answers are stored: a hit skips a simulation and can never produce a command, so the worst a
// stale entry can do is delay one node's consolidation until the entry expires or one of its
// fingerprinted inputs moves.
//
// The fingerprint covers the candidate-local inputs that can flip a no into a yes: the Node and
// NodeClaim resourceVersions (capacity, labels, taints), the NodePool's generation (template,
// requirements, budgets - spec only, so the counter patches of ordinary node churn don't
// invalidate entries), the set of reschedulable pods at their own resourceVersions (a pod that
// gains a toleration or resizes its requests changes what the simulation would do), and the
// NodePool's instance type revision. That revision hashes instance type names and requirements
// only: a Spot price move or an offering becoming (un)available does not change it, so cheaper
// prices are picked up on TTL expiry, not fingerprint change. The fingerprint also cannot see the
// rest of the fleet's pods - capacity another node frees can turn "pods did not schedule" into a
// delete - which the same TTL bounds, and the cache is dropped entirely whenever the pass admits
// a command.
type NegativeResultCache struct {
	mu      sync.Mutex
	clk     clock.Clock
	entries map[string]negativeEntry
}

type negativeEntry struct {
	fingerprint string
	expiresAt   time.Time
}

func NewNegativeResultCache(clk clock.Clock) *NegativeResultCache {
	return &NegativeResultCache{
		clk:     clk,
		entries: map[string]negativeEntry{},
	}
}

// ShouldSkip reports whether the candidate's previous no-op verdict is still current, and records
// the lookup outcome so the recurrence rate is measurable whether or not skipping changes behavior.
func (c *NegativeResultCache) ShouldSkip(consolidationType, providerID, fingerprint string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[providerID]
	if !ok {
		ObserveNegativeResultCacheLookup(consolidationType, NegativeCacheLookupAbsent)
		return false
	}
	if c.clk.Now().After(entry.expiresAt) {
		delete(c.entries, providerID)
		ObserveNegativeResultCacheLookup(consolidationType, NegativeCacheLookupExpired)
		return false
	}
	if entry.fingerprint != fingerprint {
		delete(c.entries, providerID)
		ObserveNegativeResultCacheLookup(consolidationType, NegativeCacheLookupChanged)
		return false
	}
	ObserveNegativeResultCacheLookup(consolidationType, NegativeCacheLookupHit)
	return true
}

// StoreNegative records that the candidate's simulation ended in a no-op.
func (c *NegativeResultCache) StoreNegative(providerID, fingerprint string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Re-storing an unchanged verdict keeps the original expiry. In observation mode (skip
	// disabled) every pass re-simulates and re-stores; refreshing the TTL here would keep the
	// entry alive forever, so the hit rate the counters report would overstate what enabling
	// the skip delivers and the expired outcome would be unreachable. Entries age identically
	// in both modes this way.
	if existing, ok := c.entries[providerID]; ok && existing.fingerprint == fingerprint && c.clk.Now().Before(existing.expiresAt) {
		return
	}
	c.entries[providerID] = negativeEntry{fingerprint: fingerprint, expiresAt: c.clk.Now().Add(ttl)}
}

// Clear drops every entry. Called when a pass admits a command: an executed command changes the
// free capacity every stored verdict was computed against, and the fingerprint cannot see that.
func (c *NegativeResultCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = map[string]negativeEntry{}
}

// DropExpired removes every expired entry. Lookups already evict the entries they touch, but a
// node that leaves the fleet is never looked up again, so without a sweep the map would grow with
// cumulative node churn. Called once per pass, it bounds the map to nodes seen within one TTL.
func (c *NegativeResultCache) DropExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clk.Now()
	for providerID, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, providerID)
		}
	}
}

// noOpDurability distinguishes a no-op that is a verdict about the candidate ("no cheaper option
// existed") from one that is a consequence of pass-scoped exhaustion (the split fallback's attempt
// budget ran out before the candidate was simulated, the candidate started deleting mid-pass, a
// transient simulation error). Only the former may be stored across passes: the latter would make
// a fresh pass, with a fresh budget, skip a candidate it never actually evaluated.
type noOpDurability struct {
	inconclusive atomic.Bool
}

type noOpDurabilityKey struct{}

func withNoOpDurability(ctx context.Context) (context.Context, *noOpDurability) {
	d := &noOpDurability{}
	return context.WithValue(ctx, noOpDurabilityKey{}, d), d
}

// markNoOpInconclusive flags the candidate evaluation on the context, if any, as having ended in
// a no-op that is not a durable verdict. A no-op when no evaluation is tracked is a no-op.
func markNoOpInconclusive(ctx context.Context) {
	if d, ok := ctx.Value(noOpDurabilityKey{}).(*noOpDurability); ok {
		d.inconclusive.Store(true)
	}
}

func (d *noOpDurability) Conclusive() bool {
	return !d.inconclusive.Load()
}

// negativeCacheFingerprints resolves fingerprints for a pass's candidates. Instance type
// revisions are looked up once per NodePool per pass, and the fleet component once per pass.
type negativeCacheFingerprints struct {
	kubeClient       client.Client
	cloudProvider    cloudprovider.CloudProvider
	revisionProvider cloudprovider.InstanceTypeRevisionProvider
	revisions        map[string]uint64
	fleet            *string
}

func newNegativeCacheFingerprints(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) *negativeCacheFingerprints {
	revisionProvider, _ := cloudProvider.(cloudprovider.InstanceTypeRevisionProvider)
	return &negativeCacheFingerprints{
		kubeClient:       kubeClient,
		cloudProvider:    cloudProvider,
		revisionProvider: revisionProvider,
		revisions:        map[string]uint64{},
	}
}

// fingerprint returns the candidate's fingerprint, or "" when one of its inputs cannot be
// versioned, in which case the candidate is never skipped.
func (f *negativeCacheFingerprints) fingerprint(ctx context.Context, candidate *Candidate) string {
	if candidate.Node == nil || candidate.NodeClaim == nil || candidate.NodePool == nil {
		return ""
	}
	revision, ok := f.instanceTypeRevision(ctx, candidate.NodePool)
	if !ok {
		return ""
	}
	// The simulation a verdict came from searches every ready NodePool for a replacement, not
	// just the candidate's, so the fingerprint carries a fleet component covering all of them: a
	// new pool, an edited pool, a readiness flip, or an instance type refresh anywhere can flip a
	// no into a yes.
	fleet, ok := f.fleetComponent(ctx)
	if !ok {
		return ""
	}
	// Pods carry their resourceVersion, not just their identity: a spec update (new tolerations,
	// changed requests via in-place resize) changes what the simulation would do without changing
	// the pod set.
	podUIDs := make([]string, 0, len(candidate.reschedulablePods))
	for _, pod := range candidate.reschedulablePods {
		if pod.UID == "" || pod.ResourceVersion == "" {
			return ""
		}
		podUIDs = append(podUIDs, string(pod.UID)+":"+pod.ResourceVersion)
	}
	sort.Strings(podUIDs)
	// The NodePool is fingerprinted by UID and generation, not resourceVersion: its status counters
	// are patched on every node join/leave, so resourceVersion churns continuously on a busy fleet
	// while only spec changes can alter what the simulation would do with this candidate. The UID
	// covers a delete/recreate under the same name, which resets generation and, per the
	// InstanceTypesRevisionProvider contract, may reuse a revision for different content.
	return fmt.Sprintf("%s|%s|%s:%d|%d|%s|%s",
		candidate.Node.ResourceVersion,
		candidate.NodeClaim.ResourceVersion,
		candidate.NodePool.UID,
		candidate.NodePool.Generation,
		revision,
		strings.Join(podUIDs, ","),
		fleet,
	)
}

// fleetComponent is the fingerprint's view of every NodePool a replacement could be templated
// from: each ready managed pool's name, UID, generation, and instance type revision, resolved once
// per pass. Only ready pools are included, so a pool's Ready condition flipping changes the
// component by changing the set. It fails closed — no fleet component, no fingerprint — when the
// pools cannot be listed or any pool's offerings cannot be versioned.
func (f *negativeCacheFingerprints) fleetComponent(ctx context.Context) (string, bool) {
	if f.fleet != nil {
		return *f.fleet, *f.fleet != ""
	}
	failed := ""
	nodePools, err := nodepoolutils.ListManaged(ctx, f.kubeClient, f.cloudProvider)
	if err != nil {
		f.fleet = &failed
		return "", false
	}
	parts := make([]string, 0, len(nodePools))
	for _, nodePool := range nodePools {
		if !nodePool.StatusConditions().IsTrue(status.ConditionReady) {
			continue
		}
		revision, ok := f.instanceTypeRevision(ctx, nodePool)
		if !ok {
			f.fleet = &failed
			return "", false
		}
		parts = append(parts, fmt.Sprintf("%s:%s:%d:%d", nodePool.Name, nodePool.UID, nodePool.Generation, revision))
	}
	sort.Strings(parts)
	fleet := "{" + strings.Join(parts, ";") + "}"
	f.fleet = &fleet
	return fleet, true
}

// instanceTypeRevision returns the revision of the candidate NodePool's instance type list,
// without materializing the list. The revision covers instance type identity and requirements: a
// provider that cannot version them at all makes every fingerprint unresolvable, while price and
// offering-availability changes are invisible to the revision by design and are bounded by the
// entry TTL instead.
func (f *negativeCacheFingerprints) instanceTypeRevision(ctx context.Context, nodePool *v1.NodePool) (uint64, bool) {
	if f.revisionProvider == nil {
		return 0, false
	}
	if revision, ok := f.revisions[nodePool.Name]; ok {
		return revision, revision != 0
	}
	revision, err := f.revisionProvider.InstanceTypeRevision(ctx, nodePool)
	if err != nil {
		revision = 0
	}
	f.revisions[nodePool.Name] = revision
	return revision, revision != 0
}
