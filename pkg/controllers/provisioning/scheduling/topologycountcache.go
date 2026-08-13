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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
)

// podDomainRecord is one pod's contribution to a topology group's domain counts: pod p, counted
// under domain d. The records of one scan replay to exactly the Record calls the scan would have
// made, so a candidate applies them minus its own excluded pods instead of re-walking the cluster.
type podDomainRecord struct {
	uid    types.UID
	domain string
}

// topologyPodRecords returns the pod domain records for a topology group, reusing the pass-scoped
// scan for the group's hash when the count cache is on. The records are a pure function of the
// group's hashed identity (key, type, namespaces, selector, node filter) and of reads the pass has
// already pinned in TopologyPassCache, so two groups with equal hashes in one pass scan
// identically; shadow mode verifies exactly that by computing both and comparing.
func topologyPodRecords(ctx context.Context, kubeClient client.Client, tg *TopologyGroup) ([]podDomainRecord, error) {
	mode := karpopts.FromContext(ctx).TopologyCountCacheMode
	cache := TopologyPassCacheFromContext(ctx)
	// Only the two explicitly requested modes leave the fresh scan; anything else — including an
	// Options value built without Parse, where the mode is the empty string — fails safe.
	if cache == nil || (mode != karpopts.TopologyCountCacheModeShadow && mode != karpopts.TopologyCountCacheModeOn) {
		return scanTopologyPodDomains(ctx, kubeClient, tg)
	}
	hash := tg.Hash()
	if mode == karpopts.TopologyCountCacheModeShadow {
		records, err := scanTopologyPodDomains(ctx, kubeClient, tg)
		if err != nil {
			return nil, err
		}
		if cached, ok := cache.podDomainRecords(hash); ok {
			outcome := cacheOutcomeShadowMatch
			if !podDomainRecordsEqual(cached, records) {
				outcome = cacheOutcomeShadowMismatch
			}
			TopologyCountCacheEventsTotal.Inc(map[string]string{outcomeLabel: outcome})
		}
		cache.setPodDomainRecords(hash, records)
		return records, nil
	}
	if records, ok := cache.podDomainRecords(hash); ok {
		TopologyCountCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return records, nil
	}
	records, err := scanTopologyPodDomains(ctx, kubeClient, tg)
	if err != nil {
		return nil, err
	}
	TopologyCountCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	cache.setPodDomainRecords(hash, records)
	return records, nil
}

func podDomainRecordsEqual(a, b []podDomainRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
