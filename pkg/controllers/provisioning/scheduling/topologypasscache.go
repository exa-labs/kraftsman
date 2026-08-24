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
	"fmt"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type topologyPassCacheContextKey struct{}

// TopologyPassCache memoizes the API reads that Topology.countDomains performs for one scheduling
// pass: the pod lists per (namespace, selector) and the node lookups per name. Both reads hit the
// informer cache, but each call deep copies every returned object, which consolidation repeats for
// every topology group of every candidate simulation. The cached objects are shared across
// candidates and MUST be treated as read-only. A cached nil node records a NotFound result. The
// cache must not outlive a pass: it intentionally pins the informer view observed at first read so
// every candidate in the pass counts against a consistent snapshot.
type TopologyPassCache struct {
	mu          sync.Mutex
	podsByKey   map[string][]corev1.Pod
	nodesByName map[string]*corev1.Node
	// recordsByGroup holds each topology group's scanned pod domain records under the group's
	// hash, which covers everything the scan reads that is not already pinned by the pod and node
	// caches above. The records are shared across candidates and MUST be treated as read-only.
	recordsByGroup map[uint64][]podDomainRecord
}

func NewTopologyPassCache() *TopologyPassCache {
	return &TopologyPassCache{
		podsByKey:      map[string][]corev1.Pod{},
		nodesByName:    map[string]*corev1.Node{},
		recordsByGroup: map[uint64][]podDomainRecord{},
	}
}

func (c *TopologyPassCache) podDomainRecords(hash uint64) ([]podDomainRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	records, ok := c.recordsByGroup[hash]
	return records, ok
}

func (c *TopologyPassCache) setPodDomainRecords(hash uint64, records []podDomainRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recordsByGroup[hash] = records
}

func WithTopologyPassCache(ctx context.Context, cache *TopologyPassCache) context.Context {
	return context.WithValue(ctx, topologyPassCacheContextKey{}, cache)
}

func TopologyPassCacheFromContext(ctx context.Context) *TopologyPassCache {
	cache, _ := ctx.Value(topologyPassCacheContextKey{}).(*TopologyPassCache)
	return cache
}

// listTopologyPods returns the pods in a namespace matching the topology group's selector,
// reusing the pass-scoped cached list when available.
func listTopologyPods(ctx context.Context, kubeClient client.Client, namespace string, rawSelector *metav1.LabelSelector) ([]corev1.Pod, error) {
	cache := TopologyPassCacheFromContext(ctx)
	opts := TopologyListOptions(namespace, rawSelector)
	if cache == nil {
		podList := &corev1.PodList{}
		if err := kubeClient.List(ctx, podList, opts); err != nil {
			return nil, err
		}
		return podList.Items, nil
	}
	// Key on the raw selector rather than the parsed one: labels.Everything() (nil selector) and
	// labels.Nothing() (unparseable selector) both stringify to "" and must not share an entry.
	rawKey := "<nil>"
	if rawSelector != nil {
		rawKey = fmt.Sprintf("%v", rawSelector)
	}
	key := strings.Join([]string{namespace, rawKey}, "\x00")
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if pods, ok := cache.podsByKey[key]; ok {
		TopologyPassCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return pods, nil
	}
	podList := &corev1.PodList{}
	if err := kubeClient.List(ctx, podList, opts); err != nil {
		return nil, err
	}
	TopologyPassCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	cache.podsByKey[key] = podList.Items
	return podList.Items, nil
}

// getTopologyNode returns the named node, reusing the pass-scoped cached object when available.
// A nil node with a nil error records a NotFound result.
func getTopologyNode(ctx context.Context, kubeClient client.Client, name string) (*corev1.Node, error) {
	cache := TopologyPassCacheFromContext(ctx)
	if cache == nil {
		return getTopologyNodeUncached(ctx, kubeClient, name)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if node, ok := cache.nodesByName[name]; ok {
		TopologyPassCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return node, nil
	}
	node, err := getTopologyNodeUncached(ctx, kubeClient, name)
	if err != nil {
		return nil, err
	}
	TopologyPassCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	cache.nodesByName[name] = node
	return node, nil
}

func getTopologyNodeUncached(ctx context.Context, kubeClient client.Client, name string) (*corev1.Node, error) {
	node := &corev1.Node{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return node, nil
}
