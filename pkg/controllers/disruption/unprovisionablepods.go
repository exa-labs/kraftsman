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
	"sync"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// A disruption simulation schedules the whole pending backlog alongside the candidate's pods, because
// the backlog competes for the same capacity. A pending pod the provisioner cannot place anywhere -
// pinned to a capacity type whose every offering is exhausted, to a NodePool at its limit, to
// requirements no NodePool satisfies - competes for nothing: it lands on no existing node and opens
// no NodeClaim, in the provisioner's simulation and in every disruption simulation alike, which
// only see the cluster minus the candidate. Simulating it is pure cost, paid once per candidate,
// and a backlog of thousands of such pods turns each candidate's budget into a timeout.
//
// The provisioner already computes the verdict: every provisioning pass places each pending pod or
// records an error for it, and the cluster state keeps the time of the latest such error. A
// disruption simulation excludes the pods whose latest verdict is an error younger than
// DISRUPTION_UNPROVISIONABLE_POD_TTL. Nothing here re-derives schedulability, so the exclusion
// follows the provisioner exactly, including reserved capacity, NodePool limits and instance type
// availability, and is refreshed as often as the provisioner runs. The TTL bounds what happens when
// it does not: a verdict older than the TTL is ignored and the pod is simulated again.
//
// Normal provisioning is untouched: it reads the backlog directly, never through this filter, so an
// excluded pod is retried by every provisioning pass and re-enters simulations the moment one places it.

const (
	simulationPodsDispositionSimulated = "simulated"
	simulationPodsDispositionExcluded  = "excluded_unprovisionable"

	// unprovisionablePodsLogInterval spaces the Info-level log of excluded pods. The gauge carries
	// the continuous signal; the log is a periodic, sampled confirmation of which pods it counts.
	unprovisionablePodsLogInterval = time.Minute
	unprovisionablePodsLogSample   = 5
)

// partitionUnprovisionablePods splits the pending backlog into the pods a disruption simulation
// should schedule and the pods whose latest provisioning verdict, younger than ttl, found nowhere for
// them. A ttl of zero or less excludes nothing. Both returned slices are freshly allocated; pods is
// not modified.
func partitionUnprovisionablePods(cluster *state.Cluster, clk clock.Clock, ttl time.Duration, pods []*corev1.Pod) (simulated, excluded []*corev1.Pod) {
	if ttl <= 0 {
		return append([]*corev1.Pod(nil), pods...), nil
	}
	now := clk.Now()
	return lo.FilterReject(pods, func(p *corev1.Pod, _ int) bool {
		verdict := cluster.PodUnprovisionableTime(client.ObjectKeyFromObject(p))
		return verdict.IsZero() || now.Sub(verdict) >= ttl
	})
}

// unprovisionablePodsLog rate-limits the Info-level report of excluded pods across every
// simulation of every disruption method, which otherwise run several times a minute.
type unprovisionablePodsLog struct {
	mu   sync.Mutex
	last time.Time
}

var excludedPodsLog unprovisionablePodsLog

// shouldLog reports whether at least unprovisionablePodsLogInterval has passed since the last
// accepted log and, if so, records now as the last accepted time.
func (l *unprovisionablePodsLog) shouldLog(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.last.IsZero() && now.Sub(l.last) < unprovisionablePodsLogInterval {
		return false
	}
	l.last = now
	return true
}

// excludeUnprovisionablePods applies partitionUnprovisionablePods to a simulation's pending backlog
// using the configured TTL, records both populations on the simulation pending pods gauge, and logs
// a rate-limited sample of the excluded pods.
func excludeUnprovisionablePods(ctx context.Context, cluster *state.Cluster, clk clock.Clock, pods []*corev1.Pod) []*corev1.Pod {
	ttl := options.FromContext(ctx).DisruptionUnprovisionablePodTTL
	simulated, excluded := partitionUnprovisionablePods(cluster, clk, ttl, pods)
	SimulationPendingPods.Set(float64(len(simulated)), map[string]string{dispositionLabel: simulationPodsDispositionSimulated})
	SimulationPendingPods.Set(float64(len(excluded)), map[string]string{dispositionLabel: simulationPodsDispositionExcluded})
	if len(excluded) == 0 {
		return simulated
	}
	logger := log.FromContext(ctx).WithValues(
		"excluded", len(excluded),
		"simulated", len(simulated),
		"ttl", ttl,
		"sample", lo.Map(lo.Subset(excluded, 0, unprovisionablePodsLogSample), func(p *corev1.Pod, _ int) string { return klog.KObj(p).String() }),
	)
	if excludedPodsLog.shouldLog(clk.Now()) {
		logger.Info("excluding pending pods the provisioner could not place from disruption simulation")
	} else {
		logger.V(1).Info("excluding pending pods the provisioner could not place from disruption simulation")
	}
	return simulated
}
