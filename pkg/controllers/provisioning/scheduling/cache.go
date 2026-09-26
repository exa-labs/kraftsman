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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type daemonOverheadCacheContextKey struct{}

// DaemonOverheadCache memoizes candidate-invariant existing-node scheduling data for one scheduling pass.
// The cache must not be shared across passes because node labels and taints can change.
type DaemonOverheadCache struct {
	mu                  sync.RWMutex
	daemonPodsByKey     map[string][]*corev1.Pod
	daemonRequestsByKey map[string]corev1.ResourceList
	ingredientsByKey    map[string]existingNodeIngredients
	// overheadGroupsByTemplate is keyed by NodePool name and template fingerprint rather than by NodePool alone: a
	// consolidation pass interleaves a NodePool's unlimited template with the price-limited ones its split retries
	// build, and one entry per NodePool would make those overwrite each other on every candidate.
	overheadGroupsByTemplate map[string][]DaemonOverheadGroup
	daemonSetGeneration      string
	daemonSetGenerationValid bool
	// groupStore, when set, backs overhead group misses with entries from earlier passes.
	groupStore *DaemonOverheadGroupStore
}

// existingNodeIngredients holds the candidate-invariant inputs of one ExistingNode. Everything
// here is a pure function of the node's own state and the daemonset pod set, both covered by the
// cache key and the daemonset-generation flush; the values pin the view observed at first read for
// the rest of the pass, like every other pass-scoped cache. taints are shared and read-only;
// remainingBase is deep copied out because scheduling subtracts from an ExistingNode's remaining
// resources in place.
type existingNodeIngredients struct {
	taints        []corev1.Taint
	remainingBase corev1.ResourceList
}

func NewDaemonOverheadCache() *DaemonOverheadCache {
	return &DaemonOverheadCache{
		daemonPodsByKey:          map[string][]*corev1.Pod{},
		daemonRequestsByKey:      map[string]corev1.ResourceList{},
		ingredientsByKey:         map[string]existingNodeIngredients{},
		overheadGroupsByTemplate: map[string][]DaemonOverheadGroup{},
	}
}

// NewDaemonOverheadCacheWithGroupStore returns a pass-scoped cache whose overhead group misses fall
// back to, and populate, a DaemonOverheadGroupStore that outlives the pass.
func NewDaemonOverheadCacheWithGroupStore(store *DaemonOverheadGroupStore) *DaemonOverheadCache {
	c := NewDaemonOverheadCache()
	c.groupStore = store
	return c
}

func (c *DaemonOverheadCache) updateDaemonSetGeneration(daemonSetPods []*corev1.Pod) {
	generation, ok := daemonSetPodsGeneration(daemonSetPods)
	if c.groupStore != nil {
		c.groupStore.updateDaemonSetGeneration(generation, ok)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !ok || !c.daemonSetGenerationValid || c.daemonSetGeneration != generation {
		c.daemonPodsByKey = map[string][]*corev1.Pod{}
		c.daemonRequestsByKey = map[string]corev1.ResourceList{}
		c.ingredientsByKey = map[string]existingNodeIngredients{}
		c.overheadGroupsByTemplate = map[string][]DaemonOverheadGroup{}
		c.daemonSetGeneration = generation
		c.daemonSetGenerationValid = ok
	}
}

func WithDaemonOverheadCache(ctx context.Context, cache *DaemonOverheadCache) context.Context {
	return context.WithValue(ctx, daemonOverheadCacheContextKey{}, cache)
}

func DaemonOverheadCacheFromContext(ctx context.Context) *DaemonOverheadCache {
	cache, _ := ctx.Value(daemonOverheadCacheContextKey{}).(*DaemonOverheadCache)
	return cache
}

func daemonSetPodsGeneration(daemonSetPods []*corev1.Pod) (string, bool) {
	entries := make([]string, len(daemonSetPods))
	for i, pod := range daemonSetPods {
		content, err := json.Marshal(struct {
			Namespace string
			Name      string
			Spec      corev1.PodSpec
		}{
			Namespace: pod.Namespace,
			Name:      pod.Name,
			Spec:      pod.Spec,
		})
		if err != nil {
			return "", false
		}
		entries[i] = string(content)
	}
	sort.Strings(entries)
	sum := sha256.Sum256([]byte(strings.Join(entries, "\x01")))
	return hex.EncodeToString(sum[:]), true
}

func nodeCacheKey(node *state.StateNode, ignoreDRA bool) (string, bool) {
	if node == nil || (node.Node == nil && node.NodeClaim == nil) {
		return "", false
	}

	nodeUID, nodeResourceVersion := "", ""
	if node.Node != nil {
		var ok bool
		nodeUID, nodeResourceVersion, ok = cacheObjectKey(node.Node.UID, node.Node.ResourceVersion)
		if !ok {
			return "", false
		}
	}
	nodeClaimUID, nodeClaimResourceVersion := "", ""
	if node.NodeClaim != nil {
		var ok bool
		nodeClaimUID, nodeClaimResourceVersion, ok = cacheObjectKey(node.NodeClaim.UID, node.NodeClaim.ResourceVersion)
		if !ok {
			return "", false
		}
	}
	return strings.Join([]string{
		nodeUID,
		nodeResourceVersion,
		nodeClaimUID,
		nodeClaimResourceVersion,
		strconv.FormatBool(ignoreDRA),
	}, "\x00"), true
}

func cacheObjectKey(uid types.UID, resourceVersion string) (string, string, bool) {
	if uid == "" || resourceVersion == "" {
		return "", "", false
	}
	return string(uid), resourceVersion, true
}

func (c *DaemonOverheadCache) daemonPods(key string) ([]*corev1.Pod, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pods, ok := c.daemonPodsByKey[key]
	return pods, ok
}

func (c *DaemonOverheadCache) setDaemonPods(key string, pods []*corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.daemonPodsByKey[key] = pods
}

// DropStateDerived flushes the entries that depend on cluster-state pod tracking rather than on
// the keyed objects alone. An ExistingNode's available and remaining resources move when a pod
// binds or a command executes, without the node's ResourceVersion moving, so every point that
// drops the pass's pinned reads to observe churn must drop these too. The daemon pod and request
// entries survive: they are pure functions of the node object and the daemonset set, both of
// which the key and the generation flush cover.
func (c *DaemonOverheadCache) DropStateDerived() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ingredientsByKey = map[string]existingNodeIngredients{}
}

func (c *DaemonOverheadCache) ingredients(key string) (existingNodeIngredients, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ing, ok := c.ingredientsByKey[key]
	return ing, ok
}

func (c *DaemonOverheadCache) setIngredients(key string, ing existingNodeIngredients) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ingredientsByKey[key] = ing
}

// daemonRequests returns a deep copy of the cached summed daemon resource requests for a node.
// A deep copy is required because NewExistingNode mutates the ResourceList it is handed
// (SubtractFrom and clamping), and Quantity arithmetic can mutate shared inner state.
func (c *DaemonOverheadCache) daemonRequests(key string) (corev1.ResourceList, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	requests, ok := c.daemonRequestsByKey[key]
	if !ok {
		return nil, false
	}
	return requests.DeepCopy(), true
}

func (c *DaemonOverheadCache) setDaemonRequests(key string, requests corev1.ResourceList) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.daemonRequestsByKey[key] = requests.DeepCopy()
}

// overheadGroups returns the daemon overhead groups for a fingerprinted template from this pass or, failing that,
// from the group store, together with the lookup outcome. The returned slice and its contents are shared across
// schedulers (and, through the store, across passes) and MUST be treated as read-only; NewNodeClaim deep copies the
// per-NodeClaim mutable piece (HostPortUsage) before any mutation.
func (c *DaemonOverheadCache) overheadGroups(nct *NodeClaimTemplate) ([]DaemonOverheadGroup, string, bool) {
	key := overheadGroupsCacheKey(nct.NodePoolName, nct.cacheFingerprint)
	c.mu.RLock()
	groups, ok := c.overheadGroupsByTemplate[key]
	generation, generationValid := c.daemonSetGeneration, c.daemonSetGenerationValid
	c.mu.RUnlock()
	if ok {
		return groups, cacheOutcomeHit, true
	}
	if c.groupStore == nil || !generationValid {
		return nil, cacheOutcomeMiss, false
	}
	if groups, ok = c.groupStore.overheadGroups(generation, key, nct); !ok {
		return nil, cacheOutcomeMiss, false
	}
	c.mu.Lock()
	c.overheadGroupsByTemplate[key] = groups
	c.mu.Unlock()
	return groups, cacheOutcomeHitCrossPass, true
}

// setOverheadGroups records the groups computed for a fingerprinted template in this pass and in the group store.
func (c *DaemonOverheadCache) setOverheadGroups(nct *NodeClaimTemplate, groups []DaemonOverheadGroup) {
	key := overheadGroupsCacheKey(nct.NodePoolName, nct.cacheFingerprint)
	c.mu.Lock()
	c.overheadGroupsByTemplate[key] = groups
	generation, generationValid := c.daemonSetGeneration, c.daemonSetGenerationValid
	c.mu.Unlock()
	if c.groupStore != nil && generationValid {
		c.groupStore.setOverheadGroups(generation, key, groups)
	}
}

func overheadGroupsCacheKey(nodePoolName string, fingerprint uint64) string {
	return nodePoolName + "|" + strconv.FormatUint(fingerprint, 16)
}

// daemonOverheadGroupStoreMaxEntries bounds the store. Price-limited templates built by split retries
// carry per-candidate fingerprints, so entries accumulate between DaemonSet changes; the store starts
// over once it is full.
const daemonOverheadGroupStoreMaxEntries = 1024

// DaemonOverheadGroupStore keeps daemon overhead groups across scheduling passes. Groups are a pure
// function of the NodeClaimTemplate's candidate-invariant inputs (covered by its cache fingerprint:
// NodePool UID and generation, provider instance type revision, instance type count, minValues policy
// and price limit), the DaemonSet pod set (covered by the DaemonSet generation) and the process-wide
// IgnoreDRARequests option. The store holds entries for a single DaemonSet generation: moving to another
// flushes it, and reads and writes made under any other generation are ignored, so a pass that
// observed an older DaemonSet set can neither read nor publish groups for the current one.
// Entries hold instance type names rather than pointers because each pass resolves its own
// *InstanceType objects and the scheduler matches groups to a template's options by identity.
type DaemonOverheadGroupStore struct {
	mu                       sync.Mutex
	entries                  map[string]storedDaemonOverheadGroups
	daemonSetGeneration      string
	daemonSetGenerationValid bool
}

// storedDaemonOverheadGroups is one template's groups with instance types recorded by name. The overhead
// and host port usage are shared read-only, as they are between schedulers within a pass.
type storedDaemonOverheadGroups struct {
	groups        []storedDaemonOverheadGroup
	instanceTypes int
}

type storedDaemonOverheadGroup struct {
	instanceTypeNames []string
	daemonOverhead    corev1.ResourceList
	hostPortUsage     *scheduling.HostPortUsage
}

func NewDaemonOverheadGroupStore() *DaemonOverheadGroupStore {
	return &DaemonOverheadGroupStore{entries: map[string]storedDaemonOverheadGroups{}}
}

func (s *DaemonOverheadGroupStore) updateDaemonSetGeneration(generation string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !ok || !s.daemonSetGenerationValid || s.daemonSetGeneration != generation {
		s.entries = map[string]storedDaemonOverheadGroups{}
		s.daemonSetGeneration = generation
		s.daemonSetGenerationValid = ok
	}
}

func (s *DaemonOverheadGroupStore) generationMatches(generation string) bool {
	return s.daemonSetGenerationValid && s.daemonSetGeneration == generation
}

// overheadGroups returns the stored groups for the template, rebound to the template's own instance type
// objects. It misses unless the stored groups cover exactly the template's options, which the fingerprint
// implies but which would otherwise silently drop instance types or leave some in no group.
func (s *DaemonOverheadGroupStore) overheadGroups(generation, key string, nct *NodeClaimTemplate) ([]DaemonOverheadGroup, bool) {
	s.mu.Lock()
	stored, ok := s.entries[key]
	ok = ok && s.generationMatches(generation)
	s.mu.Unlock()
	if !ok || stored.instanceTypes != len(nct.InstanceTypeOptions) {
		return nil, false
	}
	byName := make(map[string]*cloudprovider.InstanceType, len(nct.InstanceTypeOptions))
	for _, it := range nct.InstanceTypeOptions {
		byName[it.Name] = it
	}
	groups := make([]DaemonOverheadGroup, len(stored.groups))
	for i, g := range stored.groups {
		its := make([]*cloudprovider.InstanceType, len(g.instanceTypeNames))
		for j, name := range g.instanceTypeNames {
			it, found := byName[name]
			if !found {
				return nil, false
			}
			its[j] = it
		}
		groups[i] = DaemonOverheadGroup{InstanceTypes: its, DaemonOverhead: g.daemonOverhead, HostPortUsage: g.hostPortUsage}
	}
	return groups, true
}

func (s *DaemonOverheadGroupStore) setOverheadGroups(generation, key string, groups []DaemonOverheadGroup) {
	stored := storedDaemonOverheadGroups{groups: make([]storedDaemonOverheadGroup, len(groups))}
	for i, g := range groups {
		stored.groups[i] = storedDaemonOverheadGroup{
			instanceTypeNames: lo.Map(g.InstanceTypes, func(it *cloudprovider.InstanceType, _ int) string { return it.Name }),
			daemonOverhead:    g.DaemonOverhead,
			hostPortUsage:     g.HostPortUsage,
		}
		stored.instanceTypes += len(g.InstanceTypes)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.generationMatches(generation) {
		return
	}
	if len(s.entries) >= daemonOverheadGroupStoreMaxEntries {
		s.entries = map[string]storedDaemonOverheadGroups{}
	}
	s.entries[key] = stored
}
