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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"hash/maphash"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/dump"
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
// The fingerprint covers the candidate-local inputs that can flip a no into a yes: the content of
// the Node and NodeClaim a simulation reads (labels, annotations, spec, capacity, allocatable,
// condition statuses - not resourceVersion, which kubelet heartbeats and pod-event timestamps move
// every few minutes without changing any of those), the NodePool's generation (template,
// requirements, budgets - spec only, so the counter patches of ordinary node churn don't
// invalidate entries), the set of reschedulable pods at their own resourceVersions (a pod that
// gains a toleration or resizes its requests changes what the simulation would do), and the
// content of the NodePool's instance types: names, capacity, overhead, and the requirements their
// offerings do not carry. Offerings are left out, so a Spot price move or an offering becoming
// (un)available does not change the fingerprint, and cheaper prices are picked up on TTL expiry.
// The provider's instance type revision cannot stand in for that content: its contract requires
// it to change whenever anything returned can differ, offerings included, so on a fleet where
// offering availability moves every few minutes it changes between nearly every pair of passes. The fingerprint also cannot see the
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

// negativeCacheFingerprints resolves fingerprints for a pass's candidates. Instance type content
// is resolved once per NodePool per pass, and the fleet component once per pass.
type negativeCacheFingerprints struct {
	kubeClient       client.Client
	cloudProvider    cloudprovider.CloudProvider
	revisionProvider cloudprovider.InstanceTypeRevisionProvider
	contents         map[string]instanceTypeContent
	fleet            *string
}

// instanceTypeContent is a NodePool's resolved instance type content hash; ok is false when it
// could not be resolved, which makes every fingerprint depending on it unresolvable.
type instanceTypeContent struct {
	hash uint64
	ok   bool
}

func newNegativeCacheFingerprints(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) *negativeCacheFingerprints {
	revisionProvider, _ := cloudProvider.(cloudprovider.InstanceTypeRevisionProvider)
	return &negativeCacheFingerprints{
		kubeClient:       kubeClient,
		cloudProvider:    cloudProvider,
		revisionProvider: revisionProvider,
		contents:         map[string]instanceTypeContent{},
	}
}

// fingerprint returns the candidate's fingerprint, or "" when one of its inputs cannot be
// versioned, in which case the candidate is never skipped.
func (f *negativeCacheFingerprints) fingerprint(ctx context.Context, candidate *Candidate) string {
	if candidate.Node == nil || candidate.NodeClaim == nil || candidate.NodePool == nil {
		return ""
	}
	instanceTypes, ok := f.instanceTypeContent(ctx, candidate.NodePool)
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
	node, ok := nodeContentHash(candidate.Node)
	if !ok {
		return ""
	}
	nodeClaim, ok := nodeClaimContentHash(candidate.NodeClaim)
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
	// covers a delete/recreate under the same name, which resets generation. The
	// spot-to-spot stability annotations steer the decision from metadata, which generation does
	// not track, so their raw values are carried explicitly.
	return fmt.Sprintf("%s|%s|%s:%d|%d|%s|%s|%s|%s",
		node,
		nodeClaim,
		candidate.NodePool.UID,
		candidate.NodePool.Generation,
		instanceTypes,
		strings.Join(podUIDs, ","),
		candidate.NodePool.Annotations[v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey],
		candidate.NodePool.Annotations[v1.NodePoolSpotToSpotMinSavingsAnnotationKey],
		fleet,
	)
}

// conditionState is the part of a status condition a simulation can depend on. Transition and
// heartbeat timestamps are left out: they move without the condition changing.
type conditionState struct {
	Type   string
	Status string
}

// nodeContentHash hashes what a consolidation simulation reads from a Node: labels, annotations,
// spec (taints, unschedulable, provider ID), capacity, allocatable, and each condition's status.
// The Node's resourceVersion is deliberately not used: the kubelet rewrites condition heartbeat
// timestamps on every status report, so on a large fleet it moves for a large share of nodes
// between two passes while nothing the simulation sees has changed.
func nodeContentHash(node *corev1.Node) (string, bool) {
	conditions := make([]conditionState, 0, len(node.Status.Conditions))
	for _, condition := range node.Status.Conditions {
		conditions = append(conditions, conditionState{Type: string(condition.Type), Status: string(condition.Status)})
	}
	return contentHash(struct {
		Labels      map[string]string
		Annotations map[string]string
		Spec        corev1.NodeSpec
		Capacity    corev1.ResourceList
		Allocatable corev1.ResourceList
		Conditions  []conditionState
	}{node.Labels, node.Annotations, node.Spec, node.Status.Capacity, node.Status.Allocatable, sortedConditions(conditions)})
}

// nodeClaimContentHash hashes what a consolidation simulation reads from a NodeClaim: labels,
// annotations, spec (requirements, resources, taints), the launched node's identity, capacity,
// allocatable, and each condition's status. The NodeClaim's resourceVersion is deliberately not
// used: status.lastPodEventTime is patched on pod events, which the reschedulable pod set already
// covers, and condition transition timestamps move without the condition changing.
func nodeClaimContentHash(nodeClaim *v1.NodeClaim) (string, bool) {
	conditions := make([]conditionState, 0, len(nodeClaim.Status.Conditions))
	for _, condition := range nodeClaim.Status.Conditions {
		conditions = append(conditions, conditionState{Type: condition.Type, Status: string(condition.Status)})
	}
	return contentHash(struct {
		Labels      map[string]string
		Annotations map[string]string
		Spec        v1.NodeClaimSpec
		NodeName    string
		ProviderID  string
		ImageID     string
		Capacity    corev1.ResourceList
		Allocatable corev1.ResourceList
		Conditions  []conditionState
	}{nodeClaim.Labels, nodeClaim.Annotations, nodeClaim.Spec, nodeClaim.Status.NodeName, nodeClaim.Status.ProviderID,
		nodeClaim.Status.ImageID, nodeClaim.Status.Capacity, nodeClaim.Status.Allocatable, sortedConditions(conditions)})
}

func sortedConditions(conditions []conditionState) []conditionState {
	sort.Slice(conditions, func(i, j int) bool {
		return conditions[i].Type < conditions[j].Type
	})
	return conditions
}

// contentHash returns a hash of the value's JSON encoding, which orders map keys and so is
// canonical for the structs above. It fails closed - no hash, no fingerprint - if the value
// cannot be encoded.
func contentHash(value any) (string, bool) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	hash := fnv.New64a()
	_, _ = hash.Write(encoded)
	return fmt.Sprintf("%016x", hash.Sum64()), true
}

// fleetComponent is the fingerprint's view of every NodePool a replacement could be templated
// from: each ready managed pool's name, UID, generation, and instance type content, resolved once
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
		instanceTypes, ok := f.instanceTypeContent(ctx, nodePool)
		if !ok {
			f.fleet = &failed
			return "", false
		}
		parts = append(parts, fmt.Sprintf("%s:%s:%d:%d", nodePool.Name, nodePool.UID, nodePool.Generation, instanceTypes))
	}
	sort.Strings(parts)
	fleet := "{" + strings.Join(parts, ";") + "}"
	f.fleet = &fleet
	return fleet, true
}

// instanceTypeContent returns the hash of the NodePool's instance types as a verdict depends on
// them (see instanceTypesContentHash), resolved once per pass. It fails closed when the provider
// reports no stable instance type revision, as a provider that cannot version its instance types
// gives no assurance that two lists fetched a pass apart describe the same fleet, and when the
// list cannot be fetched.
func (f *negativeCacheFingerprints) instanceTypeContent(ctx context.Context, nodePool *v1.NodePool) (uint64, bool) {
	if content, ok := f.contents[nodePool.Name]; ok {
		return content.hash, content.ok
	}
	content := instanceTypeContent{}
	if f.revisionProvider != nil {
		if revision, err := f.revisionProvider.InstanceTypeRevision(ctx, nodePool); err == nil && revision != 0 {
			if instanceTypes, err := f.cloudProvider.GetInstanceTypes(ctx, nodePool); err == nil {
				hash, ok := instanceTypesContentHash(instanceTypes)
				content = instanceTypeContent{hash: hash, ok: ok}
			}
		}
	}
	f.contents[nodePool.Name] = content
	return content.hash, content.ok
}

// instanceTypeContentSeed seeds the content hashes. Verdicts live in memory only, so a hash needs
// to be stable for the life of the process and no longer.
var instanceTypeContentSeed = maphash.MakeSeed()

// instanceTypesContentHash hashes what a consolidation verdict depends on in a list of instance
// types: each type's name, capacity, overhead, requirements, DRA device metadata, and offering
// capacity and overhead overrides together with the requirements of the offering they apply to,
// combined order-insensitively. Offering prices and availability are left out, and so are the type-level
// requirement keys offerings also carry (zone, capacity type, reservation): providers derive
// those from which offerings are available, so they move with availability, and their changes
// are bounded by the entry TTL like price moves.
func instanceTypesContentHash(instanceTypes []*cloudprovider.InstanceType) (uint64, bool) {
	// Each type is hashed on its own and the per-type hashes are summed, so the result does not
	// depend on list order even when several types share a name (one per region or backend).
	var combined uint64
	for _, instanceType := range instanceTypes {
		var hash maphash.Hash
		hash.SetSeed(instanceTypeContentSeed)
		hash.WriteString(instanceType.Name)
		hash.WriteByte(0)
		// Requirement hashes cover their own key, so summing them is an order-insensitive
		// combination that needs neither a sort nor an allocation.
		var requirements uint64
		for key, requirement := range instanceType.Requirements {
			if !carriedByOfferings(instanceType.Offerings, key) {
				requirements += requirement.ContentHash64(instanceTypeContentSeed)
			}
		}
		writeUint64(&hash, requirements)
		hash.WriteByte(1)
		writeResources(&hash, instanceType.Capacity)
		hash.WriteByte(2)
		if instanceType.Overhead != nil {
			writeResources(&hash, instanceType.Overhead.Total())
		}
		hash.WriteByte(3)
		writeUint64(&hash, offeringOverridesHash(instanceType.Offerings))
		hash.WriteByte(4)
		if len(instanceType.DynamicResources.ResourceSliceTemplates) > 0 || len(instanceType.DynamicResources.AttributeBindings) > 0 {
			// The device metadata holds interned handles that JSON encodes as empty objects, so
			// it is dumped by content, following pointers and sorting map keys.
			hash.WriteString(dump.ForHash(instanceType.DynamicResources))
		}
		hash.WriteByte(5)
		combined += hash.Sum64()
	}
	return maphash.Comparable(instanceTypeContentSeed, [2]uint64{combined, uint64(len(instanceTypes))}), true
}

// offeringOverridesHash combines, order-insensitively, each offering's capacity and overhead
// overrides with the requirements of the offering they apply to, so moving an override from one
// zone to another changes it. Offerings without overrides contribute nothing, which keeps
// availability and price out of it.
func offeringOverridesHash(offerings cloudprovider.Offerings) uint64 {
	var combined uint64
	for _, offering := range offerings {
		if offering.CapacityOverride == nil && offering.OverheadOverride == nil {
			continue
		}
		var hash maphash.Hash
		hash.SetSeed(instanceTypeContentSeed)
		var requirements uint64
		for _, requirement := range offering.Requirements {
			requirements += requirement.ContentHash64(instanceTypeContentSeed)
		}
		writeUint64(&hash, requirements)
		writeResources(&hash, offering.CapacityOverride)
		hash.WriteByte(0)
		if offering.OverheadOverride != nil {
			writeResources(&hash, offering.OverheadOverride.Total())
		}
		combined += hash.Sum64()
	}
	return combined
}

// carriedByOfferings reports whether any of the offerings constrains the requirement key.
func carriedByOfferings(offerings cloudprovider.Offerings, key string) bool {
	for _, offering := range offerings {
		if _, ok := offering.Requirements[key]; ok {
			return true
		}
	}
	return false
}

func writeUint64(hash *maphash.Hash, value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, _ = hash.Write(buf[:])
}

func writeResources(hash *maphash.Hash, resources corev1.ResourceList) {
	for _, name := range slices.Sorted(maps.Keys(resources)) {
		quantity := resources[name]
		hash.WriteString(string(name))
		hash.WriteByte(0)
		var buf [20]byte
		_, _ = hash.Write(strconv.AppendInt(buf[:0], quantity.MilliValue(), 10))
		hash.WriteByte(0)
	}
}
