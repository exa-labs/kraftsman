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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

type inverseAffinityCacheContextKey struct{}

// InverseAffinityCache memoizes, for one scheduling pass, the per (pod, anti-affinity term) inputs
// that updateInverseAntiAffinity derives: the canonical identity key of the inverse topology group
// the term maps to and the term's resolved namespace set. Every candidate simulation in a
// consolidation pass rebuilds Topology and re-walks every anti-affinity pod in the cluster, so
// without this cache the same selector canonicalization and namespace resolution repeats for every
// candidate. Entries are keyed by pod UID, resource version, and term index, so any pod update
// naturally misses. Namespace resolution for terms with a namespace selector reads the informer;
// the cache pins the namespace view observed at first read for the rest of the pass, matching how
// TopologyPassCache pins pod and node reads. The cached namespace sets are shared across
// candidates and MUST be treated as read-only. The cache must not outlive a pass.
type InverseAffinityCache struct {
	mu      sync.Mutex
	entries map[string]inverseAffinityEntry
}

type inverseAffinityEntry struct {
	groupKey   string
	namespaces sets.Set[string]
}

func NewInverseAffinityCache() *InverseAffinityCache {
	return &InverseAffinityCache{entries: map[string]inverseAffinityEntry{}}
}

func WithInverseAffinityCache(ctx context.Context, cache *InverseAffinityCache) context.Context {
	return context.WithValue(ctx, inverseAffinityCacheContextKey{}, cache)
}

func InverseAffinityCacheFromContext(ctx context.Context) *InverseAffinityCache {
	cache, _ := ctx.Value(inverseAffinityCacheContextKey{}).(*InverseAffinityCache)
	return cache
}

// inverseGroupKeyAndNamespaces resolves the identity key and namespace set of the inverse topology
// group one anti-affinity term maps to, reusing the pass-scoped cached result when available.
func (t *Topology) inverseGroupKeyAndNamespaces(ctx context.Context, pod *corev1.Pod, termIndex int, term corev1.PodAffinityTerm) (string, sets.Set[string], error) {
	cache := InverseAffinityCacheFromContext(ctx)
	if cache == nil {
		return t.buildInverseGroupKeyAndNamespaces(ctx, pod, term)
	}
	if pod.UID == "" || pod.ResourceVersion == "" {
		InverseAffinityCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeBypass})
		return t.buildInverseGroupKeyAndNamespaces(ctx, pod, term)
	}
	entryKey := inverseAffinityEntryKey(string(pod.UID), pod.ResourceVersion, termIndex)
	cache.mu.Lock()
	entry, ok := cache.entries[entryKey]
	cache.mu.Unlock()
	if ok {
		InverseAffinityCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return entry.groupKey, entry.namespaces, nil
	}
	groupKey, namespaces, err := t.buildInverseGroupKeyAndNamespaces(ctx, pod, term)
	if err != nil {
		return "", nil, err
	}
	InverseAffinityCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	cache.mu.Lock()
	cache.entries[entryKey] = inverseAffinityEntry{groupKey: groupKey, namespaces: namespaces}
	cache.mu.Unlock()
	return groupKey, namespaces, nil
}

func (t *Topology) buildInverseGroupKeyAndNamespaces(ctx context.Context, pod *corev1.Pod, term corev1.PodAffinityTerm) (string, sets.Set[string], error) {
	namespaces, err := t.buildNamespaceList(ctx, pod.Namespace, term.Namespaces, term.NamespaceSelector)
	if err != nil {
		return "", nil, err
	}
	return inverseTopologyGroupKey(term.TopologyKey, namespaces, term.LabelSelector), namespaces, nil
}

// inverseTopologyGroupKey builds a canonical identity for an inverse anti-affinity topology group.
// Two terms map to the same group iff their topology key, resolved namespace set, and label
// selector content match. This is the same grouping TopologyGroup.Hash provided for the inverse
// map, minus the fields that are constant for every anti-affinity group (type, maxSkew, and the
// zero node filter), and with the identical-content guarantee a 64-bit hash could only approximate.
func inverseTopologyGroupKey(topologyKey string, namespaces sets.Set[string], selector *metav1.LabelSelector) string {
	parts := []string{topologyKey, strings.Join(sets.List(namespaces), "\x01"), canonicalLabelSelector(selector)}
	return strings.Join(parts, "\x00")
}

// canonicalLabelSelector renders a label selector into a canonical string: matchLabels sorted by
// key and matchExpressions sorted with their values sorted and repeated expressions collapsed
// (repeats can occur on k8s 1.34+ matchLabelKeys handling, and must not change the identity —
// mirroring the set semantics hashSelector used). A nil selector is distinct from an empty one:
// nil parses to a match-nothing selector while empty matches everything.
func canonicalLabelSelector(selector *metav1.LabelSelector) string {
	if selector == nil {
		return "<nil>"
	}
	labelPairs := make([]string, 0, len(selector.MatchLabels))
	for k, v := range selector.MatchLabels {
		labelPairs = append(labelPairs, k+"="+v)
	}
	sort.Strings(labelPairs)
	expressions := sets.New[string]()
	for _, expression := range selector.MatchExpressions {
		values := slices.Clone(expression.Values)
		sort.Strings(values)
		expressions.Insert(strings.Join(append([]string{expression.Key, string(expression.Operator)}, values...), "\x02"))
	}
	return strings.Join(labelPairs, "\x01") + "\x00" + strings.Join(sets.List(expressions), "\x01")
}

func inverseAffinityEntryKey(uid string, resourceVersion string, termIndex int) string {
	return strings.Join([]string{uid, resourceVersion, strconv.Itoa(termIndex)}, "\x00")
}
