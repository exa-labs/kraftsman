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

// The disruption controller's cluster sync gate.
//
// Cluster state counts as synced only when every NodeClaim has a provider ID. The gate exists for
// disruption commands: a replacement NodeClaim is not a node in cluster state until it launches, and
// a pass that runs before then sees the command's candidates marked for deletion with no capacity
// for their pods, so its simulation launches that capacity again. A NodeClaim that is not a pending
// replacement cannot cause that. It is not in the simulation's node set whether or not the pass
// waits for it, so skipping it only lets the pods bound for it count as pending, which is how a
// pass sees any pod the provisioner has not launched capacity for yet.
//
// The policy (options.DisruptionSyncPolicy) chooses which of those NodeClaims a pass may skip. Every
// check that found cluster state unsynced is classified and counted whatever the policy, so a
// looser policy's effect is measurable before it is enabled.

package disruption

import (
	"context"

	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// Classes of unlaunched NodeClaims, from the one any policy waits on to the ones the narrowest
// looser policy skips. They label voluntary_disruption_unlaunched_nodeclaim_sync_checks_total.
const (
	// unlaunchedReplacement is the replacement of an in-flight disruption command.
	unlaunchedReplacement = "replacement"
	// unlaunchedInFlight is any other NodeClaim whose launch has not answered yet, usually the
	// provisioner's.
	unlaunchedInFlight = "in_flight"
	// unlaunchedLaunchDeferred is a NodeClaim a launch attempt left unlaunched with a reason of its
	// cloud provider's choosing, such as a deferral, to be retried later.
	unlaunchedLaunchDeferred = "launch_deferred"
	// unlaunchedLaunchFailed is a NodeClaim whose launch attempt failed and is being retried.
	unlaunchedLaunchFailed = "launch_failed"
	// unlaunchedDeleting is a NodeClaim that is deleting before it launched, as after insufficient
	// capacity; it will never become a node.
	unlaunchedDeleting = "deleting"
	// stateNotHydrated labels a check made before cluster state first matched the API server, which
	// no policy relaxes.
	stateNotHydrated = "not_hydrated"
)

const (
	syncOutcomeProceeded = "proceeded"
	syncOutcomeWaited    = "waited"
)

// launchFailedReason is the Launched condition reason the lifecycle controller records for a launch
// error that carries no reason of its own.
const launchFailedReason = "LaunchFailed"

// unlaunchedClassRank orders the classes by how loose a policy must be to skip a NodeClaim of that
// class. A check is labeled with the highest-ranked class present.
var unlaunchedClassRank = map[string]int{
	unlaunchedDeleting:       1,
	unlaunchedLaunchFailed:   2,
	unlaunchedLaunchDeferred: 3,
	unlaunchedInFlight:       4,
	unlaunchedReplacement:    5,
}

// skippableUnder reports whether a policy lets a pass skip NodeClaims of the class.
func skippableUnder(policy options.DisruptionSyncPolicy, class string) bool {
	switch class {
	case unlaunchedDeleting, unlaunchedLaunchFailed, unlaunchedLaunchDeferred:
		return policy == options.DisruptionSyncPolicyIgnoreFailedOrDeferred || policy == options.DisruptionSyncPolicyIgnoreNonReplacements
	case unlaunchedInFlight:
		return policy == options.DisruptionSyncPolicyIgnoreNonReplacements
	default:
		return false
	}
}

// classifyUnlaunched names the class of an unlaunched NodeClaim. Replacements come first: a deleting
// replacement still has a command waiting on it, and that command's candidates stay marked for
// deletion until the queue notices the replacement is gone.
func classifyUnlaunched(nodeClaim state.UnlaunchedNodeClaim, replacements sets.Set[string]) string {
	switch {
	case replacements.Has(nodeClaim.Name):
		return unlaunchedReplacement
	case nodeClaim.Deleting:
		return unlaunchedDeleting
	case nodeClaim.LaunchAttemptReason == launchFailedReason:
		return unlaunchedLaunchFailed
	case nodeClaim.LaunchAttemptReason != "":
		return unlaunchedLaunchDeferred
	default:
		return unlaunchedInFlight
	}
}

// syncVerdict is the outcome of one sync check.
type syncVerdict struct {
	// Proceed is true when the pass may compute disruption decisions.
	Proceed bool
	// Reason is empty when cluster state was strictly synced. Otherwise it is the highest-ranked
	// class of the unlaunched NodeClaims present, or stateNotHydrated.
	Reason string
}

// judgeUnlaunched decides whether a pass may proceed past the given unlaunched NodeClaims under the
// policy. It is pure so the policy's semantics are testable without a cluster.
func judgeUnlaunched(policy options.DisruptionSyncPolicy, unlaunched []state.UnlaunchedNodeClaim, replacements sets.Set[string]) syncVerdict {
	verdict := syncVerdict{Proceed: true}
	for _, nodeClaim := range unlaunched {
		class := classifyUnlaunched(nodeClaim, replacements)
		if !skippableUnder(policy, class) {
			verdict.Proceed = false
		}
		if unlaunchedClassRank[class] > unlaunchedClassRank[verdict.Reason] {
			verdict.Reason = class
		}
	}
	return verdict
}

// checkDisruptionSync decides whether a disruption pass may run against cluster state. It defers to
// Cluster.Synced, which also maintains the cluster_state_synced gauge, and relaxes it only once
// cluster state has hydrated and only by the NodeClaims the configured policy skips.
func checkDisruptionSync(ctx context.Context, cluster *state.Cluster, queue *Queue) syncVerdict {
	if cluster.Synced(ctx) {
		return syncVerdict{Proceed: true}
	}
	if !cluster.HasSynced() {
		return syncVerdict{Proceed: false, Reason: stateNotHydrated}
	}
	// A NodeClaim that launched between Synced and this snapshot drops out of it, which is correct:
	// it is a node in cluster state now. A controller built without a queue has no commands in flight.
	replacements := sets.New[string]()
	if queue != nil {
		replacements = queue.ReplacementNames()
	}
	return judgeUnlaunched(options.FromContext(ctx).DisruptionSyncPolicy, cluster.UnlaunchedNodeClaims(), replacements)
}

// record counts a check that found cluster state unsynced.
func (v syncVerdict) record() {
	if v.Reason == "" {
		return
	}
	outcome := syncOutcomeWaited
	if v.Proceed {
		outcome = syncOutcomeProceeded
	}
	UnlaunchedNodeClaimSyncChecksTotal.Inc(map[string]string{reasonLabel: v.Reason, outcomeLabel: outcome})
}
