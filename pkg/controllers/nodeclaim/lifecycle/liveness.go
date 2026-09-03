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

package lifecycle

import (
	"context"
	"time"

	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolhealth"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

type Liveness struct {
	clock      clock.Clock
	kubeClient client.Client
	npState    *nodepoolhealth.State
}

// registrationTimeout is a heuristic time that we expect the node to register within
// If we don't see the node within this time, then we should delete the NodeClaim and try again

const (
	registrationTimeout         = time.Minute * 15
	registrationTimeoutReason   = "registration_timeout"
	launchTimeoutReason         = "launch_timeout"
	initializationTimeoutReason = "initialization_timeout"
	// initializationTimeoutWorkloadRecheck is how often a timed-out but still-working node is re-examined.
	initializationTimeoutWorkloadRecheck = 5 * time.Minute
)

// LaunchTimeout is a heuristic time that we expect to be able to launch within
// If we don't launch within this time, then we should delete the NodeClaim and try again
var LaunchTimeout = time.Minute * 5

//nolint:gocyclo
func (l *Liveness) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	registered := nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered)
	if registered.IsTrue() {
		return l.reconcileInitializationTimeout(ctx, nodeClaim, registered)
	}
	launched := nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched)
	if launched == nil {
		return reconcile.Result{Requeue: true}, nil
	}
	if !launched.IsTrue() {
		if timeUntilTimeout := LaunchTimeout - l.clock.Since(launched.LastTransitionTime.Time); timeUntilTimeout > 0 {
			// This should never occur because if we failed to launch we requeue the object with error instead of this requeueAfter
			return reconcile.Result{RequeueAfter: timeUntilTimeout}, nil
		}
		if err := l.updateNodePoolRegistrationHealth(ctx, nodeClaim); client.IgnoreNotFound(err) != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, err
		}
		if err := l.deleteNodeClaimForTimeout(ctx, LaunchTimeout, launchTimeoutReason, nodeClaim); err != nil {
			if client.IgnoreNotFound(err) != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, nil
		}
	}
	if registered == nil {
		return reconcile.Result{Requeue: true}, nil
	}
	// If the Registered statusCondition hasn't gone True during the timeout since we first updated it, we should terminate the NodeClaim
	// NOTE: Timeout has to be stored and checked in the same place since l.clock can advance after the check causing a race
	if timeUntilTimeout := registrationTimeout - l.clock.Since(registered.LastTransitionTime.Time); timeUntilTimeout > 0 {
		return reconcile.Result{RequeueAfter: timeUntilTimeout}, nil
	}
	if err := l.updateNodePoolRegistrationHealth(ctx, nodeClaim); client.IgnoreNotFound(err) != nil {
		if errors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, err
	}
	// Delete the NodeClaim if we believe the NodeClaim won't register since we haven't seen the node
	if err := l.deleteNodeClaimForTimeout(ctx, registrationTimeout, registrationTimeoutReason, nodeClaim); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

// reconcileInitializationTimeout deletes a NodeClaim whose node registered but never initialized. Registration only
// means the kubelet joined; initialization additionally waits for the node to go Ready, for its startup taints to be
// removed, and for its requested extended resources to be registered. A bootstrap DaemonSet that never converges
// leaves the NodeClaim registered and uninitialized forever: it holds an instance we pay for, it runs no workload,
// and the scheduler still models it with its full instance type capacity, so disruption keeps simulating pods onto a
// node it can never act on.
func (l *Liveness) reconcileInitializationTimeout(ctx context.Context, nodeClaim *v1.NodeClaim, registered *status.Condition) (reconcile.Result, error) {
	initializationTimeout := options.FromContext(ctx).NodeClaimInitializationTimeout
	if initializationTimeout <= 0 {
		return reconcile.Result{}, nil
	}
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue() {
		return reconcile.Result{}, nil
	}
	// The clock is measured from the registration transition rather than the NodeClaim's creation so that a slow launch
	// or a slow registration doesn't eat into the time a node gets to initialize.
	if registered.LastTransitionTime.IsZero() {
		return reconcile.Result{Requeue: true}, nil
	}
	// NOTE: Timeout has to be stored and checked in the same place since l.clock can advance after the check causing a race
	if timeUntilTimeout := initializationTimeout - l.clock.Since(registered.LastTransitionTime.Time); timeUntilTimeout > 0 {
		return reconcile.Result{RequeueAfter: timeUntilTimeout}, nil
	}
	// A node that is serving workload pods is not stuck in the sense this timeout guards against, even if it never
	// reports Initialized (e.g. an extended resource it advertised at launch never shows up). Deleting it would evict
	// running workloads for no gain, so it is left alone and re-checked in case the pods later drain away.
	hasWorkload, err := l.hasWorkloadPods(ctx, nodeClaim)
	if err != nil {
		return reconcile.Result{}, err
	}
	if hasWorkload {
		log.FromContext(ctx).V(1).WithValues("timeout", initializationTimeout).Info("skipping initialization timeout for node running workload pods")
		return reconcile.Result{RequeueAfter: initializationTimeoutWorkloadRecheck}, nil
	}
	if err := l.deleteNodeClaimForTimeout(ctx, initializationTimeout, initializationTimeoutReason, nodeClaim); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

// hasWorkloadPods reports whether the NodeClaim's node is running any pod that isn't part of the node's own
// bootstrap, i.e. a non-DaemonSet pod that hasn't finished or started terminating. A NodeClaim whose node is not
// found has no workload.
func (l *Liveness) hasWorkloadPods(ctx context.Context, nodeClaim *v1.NodeClaim) (bool, error) {
	node, err := nodeclaimutils.NodeForNodeClaim(ctx, l.kubeClient, nodeClaim)
	if err != nil {
		if nodeclaimutils.IsNodeNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	pods, err := nodeutils.GetPods(ctx, l.kubeClient, node)
	if err != nil {
		return false, err
	}
	return lo.ContainsBy(pods, func(pod *corev1.Pod) bool {
		return !podutils.IsOwnedByDaemonSet(pod) && !podutils.IsTerminal(pod) && !podutils.IsTerminating(pod)
	}), nil
}

// updateNodePoolRegistrationHealth sets the NodeRegistrationHealthy=False
// on the NodePool if the nodeClaim fails to launch/register
func (l *Liveness) updateNodePoolRegistrationHealth(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	nodePoolName := nodeClaim.Labels[v1.NodePoolLabelKey]
	if nodePoolName != "" {
		nodePool := &v1.NodePool{}
		if err := l.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
			return err
		}
		if _, found := lo.Find(nodeClaim.GetOwnerReferences(), func(o metav1.OwnerReference) bool {
			return o.Kind == object.GVK(nodePool).Kind && o.UID == nodePool.UID
		}); !found {
			return nil
		}
		stored := nodePool.DeepCopy()
		if l.npState.DryRun(nodePool.UID, false).Status() == nodepoolhealth.StatusUnhealthy && !nodePool.StatusConditions().Get(v1.ConditionTypeNodeRegistrationHealthy).IsFalse() {
			// If the nodeClaim failed to register during the timeout set NodeRegistrationHealthy status condition on
			// NodePool to False. If the launch failed get the launch failure reason and message from nodeClaim.
			if launchCondition := nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched); launchCondition.IsTrue() {
				nodePool.StatusConditions(status.WithClock(l.clock)).SetFalse(v1.ConditionTypeNodeRegistrationHealthy, "RegistrationFailed", "Failed to register node")
			} else {
				nodePool.StatusConditions(status.WithClock(l.clock)).SetFalse(v1.ConditionTypeNodeRegistrationHealthy, launchCondition.Reason, launchCondition.Message)
			}
			// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch
			// can cause races due to the fact that it fully replaces the list on a change
			// Here, we are updating the status condition list
			if err := l.kubeClient.Status().Patch(ctx, nodePool, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
		l.npState.Update(nodePool.UID, false)
	}
	return nil
}

func (l *Liveness) deleteNodeClaimForTimeout(ctx context.Context, timeout time.Duration, reason string, nodeClaim *v1.NodeClaim) error {
	if err := l.kubeClient.Delete(ctx, nodeClaim); err != nil {
		return err
	}
	log.FromContext(ctx).V(1).WithValues("timeout", timeout, "reason", reason).Info("terminating due to timeout")
	metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
		metrics.ReasonLabel:       reason,
		metrics.NodePoolLabel:     nodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel: nodeClaim.Labels[v1.CapacityTypeLabelKey],
	})
	return nil
}
