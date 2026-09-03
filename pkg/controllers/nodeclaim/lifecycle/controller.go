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

	"github.com/awslabs/operatorpkg/status"
	"github.com/patrickmn/go-cache"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	terminatorevents "sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator/events"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolhealth"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
	"sigs.k8s.io/karpenter/pkg/utils/result"
)

const (
	minReconciles = 1000
	maxReconciles = 5000
)

// Controller is a NodeClaim Lifecycle controller that manages the lifecycle of the NodeClaim up until its termination
// The controller is responsible for ensuring that new Nodes get launched, that they have properly registered with
// the cluster as nodes and that they are properly initialized, ensuring that nodeclaims that do not have matching nodes
// after some liveness TTL are removed
type Controller struct {
	clock         clock.Clock
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder
	nodePoolState *nodepoolhealth.State

	launch         *Launch
	registration   *Registration
	initialization *Initialization
	liveness       *Liveness
}

func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder, nodePoolState *nodepoolhealth.State, registrationHooks []cloudprovider.NodeLifecycleHook) *Controller {
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		recorder:      recorder,
		nodePoolState: nodePoolState,

		launch:         &Launch{kubeClient: kubeClient, cloudProvider: cloudProvider, cache: cache.New(time.Hour, time.Minute), recorder: recorder, clock: clk},
		registration:   &Registration{kubeClient: kubeClient, recorder: recorder, npState: nodePoolState, registrationHooks: registrationHooks, clock: clk},
		initialization: &Initialization{kubeClient: kubeClient, clock: clk},
		liveness:       &Liveness{clock: clk, kubeClient: kubeClient, npState: nodePoolState},
	}
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	// higher concurrency limit since we want fast reaction to node syncing and launch
	maxConcurrentReconciles := utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)
	qps, bucketSize := utilscontroller.GetTypedBucketConfigs(10, minReconciles, maxConcurrentReconciles)
	b := controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1.NodeClaim{}, builder.WithPredicates(nodeclaimutils.IsManagedPredicateFuncs(c.cloudProvider))).
		Watches(
			&corev1.Node{},
			nodeclaimutils.NodeEventHandler(c.kubeClient, c.cloudProvider),
		)
	// When DRA is enabled, watch ResourceSlices so a NodeClaim is re-evaluated for initialization as its DRA drivers
	// publish their pools. The watch (and its resource.k8s.io RBAC) is only wired up when DRA support is on.
	if !options.FromContext(ctx).IgnoreDRARequests {
		b = b.Watches(
			&resourcev1.ResourceSlice{},
			nodeclaimutils.ResourceSliceEventHandler(c.kubeClient, c.cloudProvider),
		)
	}
	return b.
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedMaxOfRateLimiter[reconcile.Request](
				// back off until last attempt occurs ~90 seconds before nodeclaim expiration
				workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, time.Minute),
				// qps scales linearly at 1% of concurrentReconciles, bucket size is 10 * qps
				&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(qps), bucketSize)},
			),
			MaxConcurrentReconciles: maxConcurrentReconciles,
		}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

func (c *Controller) Name() string {
	return "nodeclaim.lifecycle"
}

// nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	if nodeClaim.Status.ProviderID != "" {
		ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("provider-id", nodeClaim.Status.ProviderID))
	}
	if nodeClaim.Status.NodeName != "" {
		ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("Node", klog.KRef("", nodeClaim.Status.NodeName)))
	}
	if !nodeclaimutils.IsManaged(nodeClaim, c.cloudProvider) {
		return reconcile.Result{}, nil
	}
	if !nodeClaim.DeletionTimestamp.IsZero() {
		return c.finalize(ctx, nodeClaim)
	}

	// Add the finalizer immediately since we shouldn't launch if we don't yet have the finalizer.
	// Otherwise, we could leak resources
	stored := nodeClaim.DeepCopy()
	controllerutil.AddFinalizer(nodeClaim, v1.TerminationFinalizer)
	if !equality.Semantic.DeepEqual(nodeClaim, stored) {
		// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch
		// can cause races due to the fact that it fully replaces the list on a change
		// Here, we are updating the finalizer list
		if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}

	stored = nodeClaim.DeepCopy()
	var results []reconcile.Result
	var errs error
	for _, reconciler := range []reconcile.TypedReconciler[*v1.NodeClaim]{
		c.launch,
		c.registration,
		c.initialization,
		c.liveness,
	} {
		res, err := reconciler.Reconcile(ctx, nodeClaim)
		errs = multierr.Append(errs, err)
		results = append(results, res)
	}
	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		statusCopy := nodeClaim.DeepCopy()
		if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(multierr.Append(errs, err))
		}

		if err := c.kubeClient.Status().Patch(ctx, statusCopy, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(multierr.Append(errs, err))
		}
		// We sleep here after a patch operation since we want to ensure that we are able to read our own writes
		// so that we avoid duplicating metrics and log lines due to quick re-queues from our node watcher
		// USE CAUTION when determining whether to increase this timeout or remove this line
		c.clock.Sleep(time.Second)
	}
	if errs != nil {
		return reconcile.Result{}, errs
	}
	return result.Min(results...), nil
}

//nolint:gocyclo
func (c *Controller) finalize(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(nodeClaim, v1.TerminationFinalizer) {
		return reconcile.Result{}, nil
	}
	if err := c.ensureTerminationGracePeriodTerminationTimeAnnotation(ctx, nodeClaim); err != nil {
		if errors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, fmt.Errorf("adding nodeclaim terminationGracePeriod annotation, %w", err)
	}

	// Only drain Nodes through the termination finalizer if the NodeClaim has been registered: registration is what
	// stamps the finalizer on the Node. A Node that joined without registering is deleted below, once its instance is
	// gone, so a still-running kubelet cannot re-create it in between.
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue() {
		nodes, err := nodeclaimutils.AllNodesForNodeClaim(ctx, c.kubeClient, nodeClaim)
		if err != nil {
			return reconcile.Result{}, err
		}
		for _, node := range nodes {
			// If we still get the Node, but it's already marked as terminating, we don't need to call Delete again
			if !node.DeletionTimestamp.IsZero() {
				continue
			}
			// We delete nodes to trigger the node finalization and deletion flow
			if err = c.kubeClient.Delete(ctx, node); client.IgnoreNotFound(err) != nil {
				return reconcile.Result{}, err
			}
		}
		// We wait until all the nodes associated with this nodeClaim have completed their deletion before triggering the finalization of the nodeClaim
		if len(nodes) > 0 {
			return reconcile.Result{}, nil
		}
	}
	// We can expect ProviderID to be empty when there is a failure while launching the nodeClaim
	if nodeClaim.Status.ProviderID != "" {
		deleteErr := c.cloudProvider.Delete(ctx, nodeClaim)
		if cloudprovider.IgnoreNodeClaimNotFoundError(deleteErr) != nil {
			return reconcile.Result{}, deleteErr
		}
		stored := nodeClaim.DeepCopy()
		nodeClaim.StatusConditions(status.WithClock(c.clock)).SetTrue(v1.ConditionTypeInstanceTerminating)
		if !equality.Semantic.DeepEqual(stored, nodeClaim) {
			// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch
			// can cause races due to the fact that it fully replaces the list on a change
			// Here, we are updating the status condition list
			if err := c.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
				if errors.IsNotFound(err) {
					return reconcile.Result{}, nil
				}
				if errors.IsConflict(err) {
					return reconcile.Result{Requeue: true}, nil
				}
				return reconcile.Result{}, err
			}
		}
		if !cloudprovider.IsNodeClaimNotFoundError(deleteErr) {
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if err := c.deleteUnregisteredNodes(ctx, nodeClaim); err != nil {
			return reconcile.Result{}, err
		}
		InstanceTerminationDurationSeconds.Observe(time.Since(nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).LastTransitionTime.Time).Seconds(), map[string]string{
			metrics.NodePoolLabel: nodeClaim.Labels[v1.NodePoolLabelKey],
			causeLabel:            terminationCause(nodeClaim),
		})
	}
	stored := nodeClaim.DeepCopy() // The NodeClaim may have been modified in the EnsureTerminated function
	controllerutil.RemoveFinalizer(nodeClaim, v1.TerminationFinalizer)
	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch
		// can cause races due to the fact that it fully replaces the list on a change
		// Here, we are updating the finalizer list
		// https://github.com/kubernetes/kubernetes/issues/111643#issuecomment-2016489732
		if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, client.IgnoreNotFound(fmt.Errorf("removing termination finalizer, %w", err))
		}
		log.FromContext(ctx).Info("deleted nodeclaim")
		NodeClaimTerminationDurationSeconds.Observe(time.Since(stored.DeletionTimestamp.Time).Seconds(), map[string]string{
			metrics.NodePoolLabel: nodeClaim.Labels[v1.NodePoolLabelKey],
			causeLabel:            terminationCause(nodeClaim),
		})
		metrics.NodeClaimsTerminatedTotal.Inc(map[string]string{
			metrics.NodePoolLabel:     nodeClaim.Labels[v1.NodePoolLabelKey],
			metrics.CapacityTypeLabel: nodeClaim.Labels[v1.CapacityTypeLabelKey],
			metrics.ZoneLabel:         nodeClaim.Labels[corev1.LabelTopologyZone],
		})
		observeLifetime(nodeClaim)
	}
	return reconcile.Result{}, nil

}

// deleteUnregisteredNodes removes the Nodes still mapped to a NodeClaim whose instance is already gone. A kubelet that
// joins after the NodeClaim was marked for deletion (typically the registration timeout winning the race against a slow
// boot) creates a Node that never registers: it carries no owner reference and no termination finalizer, so the
// registered-node drain above skips it, and without a cloud-controller-manager (self-managed GCE) nothing reaps the
// Node of a missing instance. It would sit NotReady forever, holding the ClusterNodeNotReady alert. Only called once the
// cloud provider reports the instance gone, so the kubelet cannot re-create the Node after it is deleted here.
func (c *Controller) deleteUnregisteredNodes(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	nodes, err := nodeclaimutils.AllNodesForNodeClaim(ctx, c.kubeClient, nodeClaim)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if !node.DeletionTimestamp.IsZero() {
			continue
		}
		if err := c.kubeClient.Delete(ctx, node); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting unregistered node, %w", err)
		}
		log.FromContext(ctx).WithValues("Node", klog.KRef("", node.Name)).Info("deleted unregistered node of terminated instance")
	}
	return nil
}

// observeLifetime records a finalized NodeClaim's lifetime under the origin that launched it and the
// cause of its deletion, so replacement nodes can be compared with provisioned ones.
func observeLifetime(nodeClaim *v1.NodeClaim) {
	if nodeClaim.CreationTimestamp.IsZero() || nodeClaim.DeletionTimestamp.IsZero() {
		return
	}
	NodeClaimLifetimeSeconds.Observe(nodeClaim.DeletionTimestamp.Sub(nodeClaim.CreationTimestamp.Time).Seconds(), map[string]string{
		metrics.NodePoolLabel:     nodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel: nodeClaim.Labels[v1.CapacityTypeLabelKey],
		originLabel:               lo.ValueOr(nodeClaim.Annotations, v1.NodeClaimReplacementOriginAnnotationKey, provisioningOrigin),
		causeLabel:                terminationCause(nodeClaim),
	})
}

// terminationCause names why a NodeClaim was deleted from what the NodeClaim itself records: the
// termination-cause annotation a deleter outside the disruption queue stamps (only known values,
// so arbitrary annotations cannot explode metric cardinality), the DisruptionReason condition the
// disruption queue sets before deleting, whether the node ever initialized, and otherwise nothing.
func terminationCause(nodeClaim *v1.NodeClaim) string {
	if cause := nodeClaim.Annotations[v1.NodeClaimTerminationCauseAnnotationKey]; cause == v1.NodeClaimTerminationCauseCloudInterrupted {
		return cause
	}
	if cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeDisruptionReason); cond.IsTrue() && cond.Reason != "" {
		return pretty.ToSnakeCase(cond.Reason)
	}
	if !nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue() {
		return terminationCauseNeverInitialized
	}
	return terminationCauseOther
}

func (c *Controller) ensureTerminationGracePeriodTerminationTimeAnnotation(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	// if the expiration annotation is already set, we don't need to do anything
	if _, exists := nodeClaim.Annotations[v1.NodeClaimTerminationTimestampAnnotationKey]; exists {
		return nil
	}

	// In Kubernetes, every object has a terminationGracePeriodSeconds, defaulted to and un-changeable from 0. There is an additional TerminationGracePeriodSeconds in the PodSpec which can be configured.
	// We use the kubernetes object TerminationGracePeriod to infer that the DeletionTimestamp is always equal to the time the NodeClaim is deleted.
	// This should not be confused with the NodeClaim.spec.terminationGracePeriod field introduced in Karpenter Custom Resources.
	if nodeClaim.Spec.TerminationGracePeriod != nil && !nodeClaim.DeletionTimestamp.IsZero() {
		terminationTimeString := nodeClaim.DeletionTimestamp.Time.Add(nodeClaim.Spec.TerminationGracePeriod.Duration).Format(time.RFC3339)
		return c.annotateTerminationGracePeriodTerminationTime(ctx, nodeClaim, terminationTimeString)
	}

	return nil
}

func (c *Controller) annotateTerminationGracePeriodTerminationTime(ctx context.Context, nodeClaim *v1.NodeClaim, terminationTime string) error {
	stored := nodeClaim.DeepCopy()
	nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.NodeClaimTerminationTimestampAnnotationKey: terminationTime})

	// We use client.MergeFromWithOptimisticLock because patching a terminationGracePeriod annotation
	// can cause races with the health controller, as that controller sets the current time as the terminationGracePeriod annotation
	// Here, We want to resolve any conflict and not overwrite the terminationGracePeriod annotation
	if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return client.IgnoreNotFound(err)
	}
	log.FromContext(ctx).WithValues(v1.NodeClaimTerminationTimestampAnnotationKey, terminationTime).Info("annotated nodeclaim")
	c.recorder.Publish(terminatorevents.NodeClaimTerminationGracePeriodExpiring(nodeClaim, terminationTime))

	return nil
}
