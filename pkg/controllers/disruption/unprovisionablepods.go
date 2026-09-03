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
// the backlog competes for the same capacity. A pending pod that every NodePool rejects on the pod and
// the NodePool alone - it tolerates none of the NodePool's taints, its requirements contradict the
// NodePool's, or no instance type with an available offering meets them (a capacity type whose every
// offering is exhausted, say) - opens no NodeClaim in the provisioner's simulation or in any
// disruption simulation, since those differ only in which nodes exist and what runs on them, and
// never shares or sizes a replacement. Simulating it is pure cost, paid once per candidate, and a
// backlog of thousands of such pods turns each candidate's budget into a timeout.
//
// What such a pod can still do is bind to an existing node another pod vacates, which kube-scheduler
// decides after the fact whether or not the simulation modeled it. Leaving the pod out lets a
// simulation hand that room to a candidate's pods instead; if the pending pod takes it first, the
// candidate's pods return to the backlog and provisioning launches for them - the outcome any pod that
// turns pending between simulation and execution already produces.
//
// The provisioner already computes the verdict: every provisioning pass places each pending pod or
// records an error for it, and only an error of that pass-invariant kind (see
// scheduling.IsIncompatibleWithAllNodePools) becomes an unprovisionable verdict in the cluster state.
// Errors that hinge on the pass - a NodePool at its limit, which a candidate's removal can lift; a
// reserved offering the strict provisioning mode deferred but the fallback mode of a disruption
// simulation may grant; topology, DRA or minValues outcomes - record no verdict, so those pods stay
// in every simulation. A disruption simulation excludes the pods whose verdict is younger than
// DISRUPTION_UNPROVISIONABLE_POD_TTL. Nothing here re-derives schedulability, so the exclusion
// follows the provisioner exactly and is refreshed as often as it runs. The TTL bounds what happens
// when it does not, and when instance type availability moves under a verdict: a verdict older than
// the TTL is ignored and the pod is simulated again.
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
// should schedule and the pods whose latest provisioning verdict, younger than ttl, found every
// NodePool incompatible with them. A ttl of zero or less excludes nothing. Both returned slices are
// freshly allocated; pods is not modified.
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
