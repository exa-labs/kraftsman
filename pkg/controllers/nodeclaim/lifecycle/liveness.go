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
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/errors"

	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolhealth"
)

type Liveness struct {
	clock      clock.Clock
	kubeClient client.Client
	recorder   events.Recorder
	npState    *nodepoolhealth.State
}

// registrationTimeout is a heuristic time that we expect the node to register within
// If we don't see the node within this time, then we should delete the NodeClaim and try again.
// A NodePool overrides it with v1.NodePoolRegistrationTimeoutAnnotationKey (see registrationTimeoutFor).

const (
	registrationTimeout         = time.Minute * 15
	registrationTimeoutReason   = "registration_timeout"
	launchTimeoutReason         = "launch_timeout"
	initializationTimeoutReason = "initialization_timeout"
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
	timeout, err := l.registrationTimeoutFor(ctx, nodeClaim)
	if err != nil {
		return reconcile.Result{}, err
	}
	// If the Registered statusCondition hasn't gone True during the timeout since we first updated it, we should terminate the NodeClaim
	// NOTE: Timeout has to be stored and checked in the same place since l.clock can advance after the check causing a race
	if timeUntilTimeout := timeout - l.clock.Since(registered.LastTransitionTime.Time); timeUntilTimeout > 0 {
		return reconcile.Result{RequeueAfter: timeUntilTimeout}, nil
	}
	if err := l.updateNodePoolRegistrationHealth(ctx, nodeClaim); client.IgnoreNotFound(err) != nil {
		if errors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, err
	}
	// Delete the NodeClaim if we believe the NodeClaim won't register since we haven't seen the node
	if err := l.deleteNodeClaimForTimeout(ctx, timeout, registrationTimeoutReason, nodeClaim); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

// RegistrationTimeoutForNodePool reads the NodePool's v1.NodePoolRegistrationTimeoutAnnotationKey. A missing or
// empty annotation selects the controller default; a value that is not a positive Go duration also selects it and is
// reported through the returned error.
func RegistrationTimeoutForNodePool(np *v1.NodePool) (time.Duration, error) {
	value := np.Annotations[v1.NodePoolRegistrationTimeoutAnnotationKey]
	if value == "" {
		return registrationTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return registrationTimeout, fmt.Errorf("parsing %s annotation value %q, %w", v1.NodePoolRegistrationTimeoutAnnotationKey, value, err)
	}
	if timeout <= 0 {
		return registrationTimeout, fmt.Errorf("invalid %s annotation value %q, expected a positive duration", v1.NodePoolRegistrationTimeoutAnnotationKey, value)
	}
	return timeout, nil
}

// registrationTimeoutFor resolves the registration timeout that governs nodeClaim: its owning NodePool's
// RegistrationTimeoutForNodePool, or the controller default when the NodeClaim has no NodePool or the NodePool is
// gone. An invalid annotation keeps the default so unregistered NodeClaims are still reclaimed, and is surfaced as a
// NodePool event and a log line on every evaluation until it is fixed.
func (l *Liveness) registrationTimeoutFor(ctx context.Context, nodeClaim *v1.NodeClaim) (time.Duration, error) {
	nodePoolName, ok := nodeClaim.Labels[v1.NodePoolLabelKey]
	if !ok {
		return registrationTimeout, nil
	}
	nodePool := &v1.NodePool{}
	if err := l.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
		if errors.IsNotFound(err) {
			return registrationTimeout, nil
		}
		return 0, fmt.Errorf("getting nodepool %s for the registration timeout, %w", nodePoolName, err)
	}
	timeout, err := RegistrationTimeoutForNodePool(nodePool)
	if err != nil {
		l.recorder.Publish(InvalidRegistrationTimeoutEvent(nodePool, err))
		log.FromContext(ctx).WithValues("NodePool", klog.KObj(nodePool)).Error(err, "using the default registration timeout", "timeout", registrationTimeout)
	}
	return timeout, nil
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
	if err := l.deleteNodeClaimForTimeout(ctx, initializationTimeout, initializationTimeoutReason, nodeClaim); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
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
