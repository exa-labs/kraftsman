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

// Surge eviction drains a ReplicaSet pod by starting its replacement before removing it.
//
// The eviction API removes a pod before anything replaces it, so a single-replica Deployment is unavailable for as long
// as its replacement takes to start, and a PodDisruptionBudget that allows no disruptions blocks the drain outright.
// A ReplicaSet selects its pods by the Deployment's labels plus pod-template-hash; removing that label from a running
// pod makes the ReplicaSet controller release it (drop its controller reference) and create a replacement while the
// released pod keeps running and keeps matching its Services. Once the ReplicaSet again has its full count of available
// pods the released pod is surplus, and evicting it leaves the workload exactly as available as it was before the drain.
//
// Per pod the state machine is:
//  1. eligible, not isolated: release it with one optimistic-lock patch (remove the label, record the ReplicaSet, the
//     removed label value and the start time in annotations, and add a non-controller owner reference to the
//     Deployment so garbage collection still follows the workload). A ReplicaSet that is gone, scaled to zero or does
//     not select on pod-template-hash, or a patch the API server refuses, falls back to the eviction API.
//  2. isolated: once the ReplicaSet controller has dropped the pod's controller reference and the ReplicaSet's own pods
//     are all available again, hand the pod to the eviction queue. A ReplicaSet that is gone or scaled to zero no
//     longer wants a replacement, so the pod is handed over at once. The pod keeps the node draining until it is gone.
//  3. isolated past the surge eviction timeout while the ReplicaSet is not fully available (or cannot be read): roll
//     back with one optimistic-lock patch (restore the label, drop the bookkeeping and the Deployment reference, mark
//     the pod aborted). The ReplicaSet re-adopts the pod and removes its surplus replacement, and the pod drains through
//     the eviction API like any other.
//
// Every step is idempotent and reads its state from the pod, so a restart or a conflicting write only delays it.

package terminator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	terminatorevents "sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator/events"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	podutil "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// surgeEvictor advances surge eviction for the pods of a draining node. It keeps no state of its own.
type surgeEvictor struct {
	clock      clock.Clock
	kubeClient client.Client
	recorder   events.Recorder
}

// drain advances surge eviction for one priority group of a draining node. group is every pod of the group still
// waiting to leave the node and evictable the subset Drain may hand to the eviction queue. It returns the pods the
// eviction queue should take: evictable pods that are not surge evicted (or fell back), and isolated pods whose surge
// has completed. Isolated pods still waiting for their ReplicaSet are not returned, so they keep the node from draining
// without reaching the eviction API. inQueue reports pods the eviction queue already holds.
func (s *surgeEvictor) drain(ctx context.Context, node *corev1.Node, group, evictable []*corev1.Pod, inQueue func(*corev1.Pod) bool) ([]*corev1.Pod, error) {
	var errs []error
	toEvict := make([]*corev1.Pod, 0, len(evictable))
	evictableUIDs := sets.New(lo.Map(evictable, func(p *corev1.Pod, _ int) types.UID { return p.UID })...)
	for _, pod := range group {
		if !podutil.IsSurgeIsolated(pod) || podutil.IsTerminating(pod) {
			continue
		}
		reason, err := s.advance(ctx, pod)
		errs = append(errs, err)
		// An isolated pod that has since become non-evictable (for example do-not-disrupt) waits like any other.
		if reason != "" && evictableUIDs.Has(pod.UID) {
			s.complete(ctx, pod, reason, inQueue(pod))
			toEvict = append(toEvict, pod)
		}
	}
	pool := &nodePoolResolver{kubeClient: s.kubeClient, node: node}
	for _, pod := range evictable {
		evict, err := s.route(ctx, pod, pool, inQueue)
		errs = append(errs, err)
		if evict {
			toEvict = append(toEvict, pod)
		}
	}
	return toEvict, errors.Join(errs...)
}

// route decides whether an evictable pod that is not isolated goes to the eviction queue, isolating it first when it is
// surge evicted. It returns true for pods the eviction queue should take; an error leaves the pod off the queue until
// the next reconcile, since neither path is safe to commit to while the pod's NodePool or ReplicaSet cannot be read.
func (s *surgeEvictor) route(ctx context.Context, pod *corev1.Pod, pool *nodePoolResolver, inQueue func(*corev1.Pod) bool) (bool, error) {
	if podutil.IsSurgeIsolated(pod) {
		return false, nil
	}
	// Decide what the pod alone decides before reading the NodePool, so pods that cannot be surge evicted cost nothing.
	if inQueue(pod) || !podutil.HasSurgeEvictableShape(pod) || pod.Annotations[v1.SurgeEvictionAnnotationKey] == "false" {
		return true, nil
	}
	nodePool, err := pool.get(ctx)
	if err != nil {
		return false, err
	}
	if !podutil.IsSurgeEvictionEnabled(pod, nodePool) {
		return true, nil
	}
	return s.isolate(ctx, pod)
}

// nodePoolResolver looks up the draining node's NodePool once, and only when a pod needs it.
type nodePoolResolver struct {
	kubeClient client.Client
	node       *corev1.Node
	resolved   bool
	nodePool   *v1.NodePool
}

// get returns the node's NodePool, or nil when the node has no NodePool label or the NodePool no longer exists.
func (r *nodePoolResolver) get(ctx context.Context) (*v1.NodePool, error) {
	if r.resolved {
		return r.nodePool, nil
	}
	if name := r.node.Labels[v1.NodePoolLabelKey]; name != "" {
		nodePool := &v1.NodePool{}
		if err := r.kubeClient.Get(ctx, types.NamespacedName{Name: name}, nodePool); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("getting nodepool, %w", err)
			}
		} else {
			r.nodePool = nodePool
		}
	}
	r.resolved = true
	return r.nodePool, nil
}

// isolate releases an eligible pod from its ReplicaSet. It returns true when the pod falls back to the eviction API
// instead: its ReplicaSet is gone, scaled to zero or would not release it, or the API server refused the patch (for
// example RBAC without patch on pods, or an admission webhook). It returns false when the pod was released or the
// patch lost a race and is retried on the next reconcile, and an error when the ReplicaSet cannot be read.
func (s *surgeEvictor) isolate(ctx context.Context, pod *corev1.Pod) (bool, error) {
	ref := podutil.ReplicaSetControllerRef(pod)
	rs, err := podutil.GetReplicaSet(ctx, s.kubeClient, pod.Namespace, ref.Name, ref.UID)
	if err != nil {
		return false, err
	}
	switch {
	case rs == nil:
		return s.fallback(ctx, pod, "its ReplicaSet no longer exists", nil)
	case podutil.ReplicaSetReplicas(rs) == 0:
		return s.fallback(ctx, pod, "its ReplicaSet is scaled to zero", nil)
	case !podutil.CanReleaseFromReplicaSet(pod, rs):
		return s.fallback(ctx, pod, fmt.Sprintf("its ReplicaSet does not select on %s", podutil.PodTemplateHashLabelKey), nil)
	}
	stored := pod.DeepCopy()
	released := pod.DeepCopy()
	delete(released.Labels, podutil.PodTemplateHashLabelKey)
	released.Annotations = lo.Assign(released.Annotations, map[string]string{
		v1.SurgeEvictionReplicaSetAnnotationKey:      formatReplicaSetRef(rs),
		v1.SurgeEvictionPodTemplateHashAnnotationKey: pod.Labels[podutil.PodTemplateHashLabelKey],
		v1.SurgeEvictionStartedAnnotationKey:         s.clock.Now().UTC().Format(time.RFC3339),
	})
	if deployment := deploymentControllerRef(rs); deployment != nil && !lo.ContainsBy(released.OwnerReferences, func(o metav1.OwnerReference) bool { return o.UID == deployment.UID }) {
		released.OwnerReferences = append(released.OwnerReferences, metav1.OwnerReference{
			APIVersion:         deployment.APIVersion,
			Kind:               deployment.Kind,
			Name:               deployment.Name,
			UID:                deployment.UID,
			Controller:         lo.ToPtr(false),
			BlockOwnerDeletion: lo.ToPtr(false),
		})
	}
	// The optimistic lock makes the label removal, the bookkeeping and the owner reference list land together on the
	// version of the pod they were computed from; the ReplicaSet controller edits the same owner reference list.
	if err := s.kubeClient.Patch(ctx, released, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return false, nil
		}
		// Retrying a refused patch cannot succeed until someone changes the cluster, and the pod must not be stranded
		// meanwhile: drain it the ordinary way.
		return s.fallback(ctx, pod, "releasing it from its ReplicaSet failed", err)
	}
	s.recorder.Publish(terminatorevents.SurgeEvictionIsolated(pod, rs.Name))
	PodsSurgeEvictionsTotal.Inc(map[string]string{OutcomeLabel: SurgeEvictionOutcomeIsolated})
	log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "ReplicaSet", klog.KObj(rs)).Info("released pod from its replicaset for surge eviction")
	return false, nil
}

// fallback records that an eligible pod drains through the eviction API instead of surge eviction. cause is the error
// that forced it, if any; it is logged and included in the event but does not fail the drain. It returns isolate's
// verdict for a pod that falls back.
func (s *surgeEvictor) fallback(ctx context.Context, pod *corev1.Pod, reason string, cause error) (bool, error) {
	logger := log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "reason", reason)
	if cause != nil {
		reason = fmt.Sprintf("%s: %s", reason, cause)
		logger.Error(cause, "falling back to eviction")
	} else {
		logger.V(1).Info("falling back to eviction")
	}
	s.recorder.Publish(terminatorevents.SurgeEvictionFallback(pod, reason))
	PodsSurgeEvictionsTotal.Inc(map[string]string{OutcomeLabel: SurgeEvictionOutcomeFallback})
	return true, nil
}

// advance moves an isolated pod forward. It returns a non-empty reason once the pod can leave: its ReplicaSet has
// released it and is fully available again, or no longer wants a replacement. Past the surge eviction timeout it
// otherwise rolls the surge back, also when the ReplicaSet or its pods cannot be read, so an unreadable ReplicaSet
// cannot hold the pod isolated forever.
func (s *surgeEvictor) advance(ctx context.Context, pod *corev1.Pod) (string, error) {
	name, uid, ok := parseReplicaSetRef(pod.Annotations[v1.SurgeEvictionReplicaSetAnnotationKey])
	if !ok {
		return "", s.rollback(ctx, pod, nil, "its surge eviction bookkeeping does not name a ReplicaSet")
	}
	reason, rs, err := s.completion(ctx, pod, name, uid)
	if reason != "" || !s.timedOut(ctx, pod) {
		return reason, err
	}
	timeout := options.FromContext(ctx).SurgeEvictionTimeout
	if err != nil {
		log.FromContext(ctx).WithValues("Pod", klog.KObj(pod)).Error(err, "reading surge eviction state past its timeout")
		return "", s.rollback(ctx, pod, rs, fmt.Sprintf("its ReplicaSet could not be read within %s", timeout))
	}
	return "", s.rollback(ctx, pod, rs, fmt.Sprintf("its ReplicaSet was not fully available within %s", timeout))
}

// completion decides whether an isolated pod can leave, returning a non-empty reason when it can. It also returns the
// ReplicaSet the pod was released from when it could be read, for a rollback to clean up after.
func (s *surgeEvictor) completion(ctx context.Context, pod *corev1.Pod, name string, uid types.UID) (string, *appsv1.ReplicaSet, error) {
	rs, err := podutil.GetReplicaSet(ctx, s.kubeClient, pod.Namespace, name, uid)
	if err != nil {
		return "", nil, err
	}
	if rs == nil {
		return "its ReplicaSet no longer exists", nil, nil
	}
	replicas := podutil.ReplicaSetReplicas(rs)
	if replicas == 0 {
		return "its ReplicaSet is scaled to zero", rs, nil
	}
	// Until the ReplicaSet controller has dropped the pod's controller reference it has not created the replacement
	// either, and its pods cannot be trusted to show it.
	if metav1.GetControllerOf(pod) != nil {
		return "", rs, nil
	}
	full, available, err := podutil.IsReplicaSetFullyAvailable(ctx, s.kubeClient, rs, s.clock.Now())
	if err != nil || !full {
		return "", rs, err
	}
	return fmt.Sprintf("its ReplicaSet has %d/%d available pods", available, replicas), rs, nil
}

// timedOut reports whether the pod has been isolated for longer than the surge eviction timeout. An unreadable start
// time counts as timed out, so a corrupted pod is rolled back rather than isolated forever.
func (s *surgeEvictor) timedOut(ctx context.Context, pod *corev1.Pod) bool {
	started, err := time.Parse(time.RFC3339, pod.Annotations[v1.SurgeEvictionStartedAnnotationKey])
	if err != nil {
		return true
	}
	return s.clock.Since(started) > options.FromContext(ctx).SurgeEvictionTimeout
}

// complete records that an isolated pod is handed to the eviction queue, once: a pod the queue already holds was
// recorded when it was first handed over.
//
// The pod goes through the eviction API rather than a plain delete so that its PodDisruptionBudget, not a possibly
// stale cached pod list, arbitrates races with other drains. Once the ReplicaSet's own pods are all available the
// isolated pod is extra healthy capacity: the disruption controller counts a pod with no controller as healthy but
// leaves it out of the expected count (it reports it as unmanaged), so the budget allows the eviction exactly when the
// surge has succeeded.
func (s *surgeEvictor) complete(ctx context.Context, pod *corev1.Pod, reason string, queued bool) {
	if queued {
		return
	}
	s.recorder.Publish(terminatorevents.SurgeEvictionCompleted(pod, reason))
	PodsSurgeEvictionsTotal.Inc(map[string]string{OutcomeLabel: SurgeEvictionOutcomeCompleted})
	log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "reason", reason).Info("evicting pod released for surge eviction")
}

// rollback returns an isolated pod to its ReplicaSet and marks it so it drains through the eviction API. rs is the
// ReplicaSet the pod was released from, or nil when it is unknown; its Deployment's owner reference is removed only
// when it is known.
func (s *surgeEvictor) rollback(ctx context.Context, pod *corev1.Pod, rs *appsv1.ReplicaSet, reason string) error {
	stored := pod.DeepCopy()
	restored := pod.DeepCopy()
	if hash := restored.Annotations[v1.SurgeEvictionPodTemplateHashAnnotationKey]; hash != "" {
		restored.Labels = lo.Assign(restored.Labels, map[string]string{podutil.PodTemplateHashLabelKey: hash})
	}
	delete(restored.Annotations, v1.SurgeEvictionStartedAnnotationKey)
	delete(restored.Annotations, v1.SurgeEvictionReplicaSetAnnotationKey)
	delete(restored.Annotations, v1.SurgeEvictionPodTemplateHashAnnotationKey)
	restored.Annotations[v1.SurgeEvictionAbortedAnnotationKey] = s.clock.Now().UTC().Format(time.RFC3339)
	if rs != nil {
		if deployment := deploymentControllerRef(rs); deployment != nil {
			restored.OwnerReferences = lo.Reject(restored.OwnerReferences, func(o metav1.OwnerReference, _ int) bool {
				return o.UID == deployment.UID && !lo.FromPtr(o.Controller)
			})
		}
	}
	if err := s.kubeClient.Patch(ctx, restored, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return ignoreRetryable(err, "rolling back surge eviction")
	}
	s.recorder.Publish(terminatorevents.SurgeEvictionAborted(pod, reason))
	PodsSurgeEvictionsTotal.Inc(map[string]string{OutcomeLabel: SurgeEvictionOutcomeAborted})
	log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "reason", reason).Info("rolled back surge eviction")
	return nil
}

// deploymentControllerRef returns the ReplicaSet's controller owner reference when it is an apps/v1 Deployment.
func deploymentControllerRef(rs *appsv1.ReplicaSet) *metav1.OwnerReference {
	ref := metav1.GetControllerOf(rs)
	if ref == nil || ref.Kind != "Deployment" || ref.APIVersion != appsv1.SchemeGroupVersion.String() {
		return nil
	}
	return ref
}

// formatReplicaSetRef renders the value of the karpenter.sh/surge-eviction-replicaset annotation.
func formatReplicaSetRef(rs *appsv1.ReplicaSet) string {
	return rs.Name + "/" + string(rs.UID)
}

// parseReplicaSetRef parses the value of the karpenter.sh/surge-eviction-replicaset annotation.
func parseReplicaSetRef(value string) (string, types.UID, bool) {
	name, uid, ok := strings.Cut(value, "/")
	if !ok || name == "" || uid == "" {
		return "", "", false
	}
	return name, types.UID(uid), true
}

// ignoreRetryable drops errors that only mean the pod changed or disappeared under a write: the next reconcile reads
// the pod again and decides afresh.
func ignoreRetryable(err error, action string) error {
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return fmt.Errorf("%s, %w", action, err)
}
