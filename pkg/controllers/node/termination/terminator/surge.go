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
// pods the released pod is surplus, and deleting it restores exactly the availability the workload had before the drain.
//
// Per pod the state machine is:
//  1. eligible, not isolated: release it with one optimistic-lock patch (remove the label, record the ReplicaSet, the
//     removed label value and the start time in annotations, and add a non-controller owner reference to the
//     Deployment so garbage collection still follows the workload). A ReplicaSet that is gone or scaled to zero falls
//     back to the eviction API.
//  2. isolated: once the ReplicaSet controller has dropped the pod's controller reference and the ReplicaSet's own pods
//     are all available again, delete the pod. A ReplicaSet that is gone or scaled to zero no longer wants a
//     replacement, so the pod is deleted at once.
//  3. isolated past the surge eviction timeout: roll back with one optimistic-lock patch (restore the label, drop the
//     bookkeeping and the Deployment reference, mark the pod aborted). The ReplicaSet re-adopts the pod and removes its
//     surplus replacement, and the pod drains through the eviction API.
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
// waiting to leave the node and evictable the subset Drain would hand to the eviction queue. It returns the pods that
// still go to the eviction queue: evictable pods that are not surge evicted. Isolated pods are never returned, so they
// keep the node from draining without ever reaching the eviction API. inQueue reports pods the eviction queue already
// holds; they stay on that path.
func (s *surgeEvictor) drain(ctx context.Context, node *corev1.Node, group, evictable []*corev1.Pod, inQueue func(*corev1.Pod) bool) ([]*corev1.Pod, error) {
	var errs []error
	for _, pod := range group {
		if podutil.IsSurgeIsolated(pod) && !podutil.IsTerminating(pod) {
			errs = append(errs, s.advance(ctx, pod))
		}
	}
	pool := &nodePoolResolver{kubeClient: s.kubeClient, node: node}
	toEvict := make([]*corev1.Pod, 0, len(evictable))
	for _, pod := range evictable {
		evict, err := s.route(ctx, pod, pool, inQueue)
		errs = append(errs, err)
		if evict {
			toEvict = append(toEvict, pod)
		}
	}
	return toEvict, errors.Join(errs...)
}

// route decides whether an evictable pod goes to the eviction queue, isolating it first when it is surge evicted.
// It returns true for pods the eviction queue should take; an error leaves the pod off the queue until the next
// reconcile, since neither path is safe to commit to while the pod's NodePool or ReplicaSet cannot be read.
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
	isolated, err := s.isolate(ctx, pod)
	return !isolated && err == nil, err
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

// isolate releases an eligible pod from its ReplicaSet. It returns false when the pod falls back to the eviction API
// because its ReplicaSet is gone or scaled to zero, and true when the pod was released or its patch must be retried.
func (s *surgeEvictor) isolate(ctx context.Context, pod *corev1.Pod) (bool, error) {
	ref := podutil.ReplicaSetControllerRef(pod)
	rs, err := s.replicaSet(ctx, pod.Namespace, ref.Name, ref.UID)
	if err != nil {
		return true, err
	}
	if rs == nil || replicas(rs) == 0 {
		reason := lo.Ternary(rs == nil, "its ReplicaSet no longer exists", "its ReplicaSet is scaled to zero")
		s.recorder.Publish(terminatorevents.SurgeEvictionFallback(pod, reason))
		PodsSurgeEvictionsTotal.Inc(map[string]string{OutcomeLabel: SurgeEvictionOutcomeFallback})
		return false, nil
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
		return true, ignoreRetryable(err, "releasing pod from its replicaset")
	}
	s.recorder.Publish(terminatorevents.SurgeEvictionIsolated(pod, rs.Name))
	PodsSurgeEvictionsTotal.Inc(map[string]string{OutcomeLabel: SurgeEvictionOutcomeIsolated})
	log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "ReplicaSet", klog.KObj(rs)).Info("released pod from its replicaset for surge eviction")
	return true, nil
}

// advance moves an isolated pod forward: it deletes the pod once its ReplicaSet no longer needs it, and rolls the
// surge back once the timeout has passed.
func (s *surgeEvictor) advance(ctx context.Context, pod *corev1.Pod) error {
	name, uid, ok := parseReplicaSetRef(pod.Annotations[v1.SurgeEvictionReplicaSetAnnotationKey])
	if !ok {
		return s.rollback(ctx, pod, nil, "its surge eviction bookkeeping does not name a ReplicaSet")
	}
	rs, err := s.replicaSet(ctx, pod.Namespace, name, uid)
	if err != nil {
		return err
	}
	if rs == nil {
		return s.complete(ctx, pod, "its ReplicaSet no longer exists")
	}
	if replicas(rs) == 0 {
		return s.complete(ctx, pod, "its ReplicaSet is scaled to zero")
	}
	// Until the ReplicaSet controller has dropped the pod's controller reference it has not created the replacement
	// either, and its pods cannot be trusted to show it.
	if metav1.GetControllerOf(pod) == nil {
		available, err := s.availableReplicas(ctx, rs)
		if err != nil {
			return err
		}
		if available >= replicas(rs) {
			return s.complete(ctx, pod, fmt.Sprintf("its ReplicaSet has %d/%d available pods", available, replicas(rs)))
		}
	}
	if s.timedOut(ctx, pod) {
		return s.rollback(ctx, pod, rs, fmt.Sprintf("its ReplicaSet was not fully available within %s", options.FromContext(ctx).SurgeEvictionTimeout))
	}
	return nil
}

// availableReplicas counts the ReplicaSet's own pods that are available. It reads pods rather than the ReplicaSet's
// status, which lags the release of the isolated pod and the start of its replacement.
func (s *surgeEvictor) availableReplicas(ctx context.Context, rs *appsv1.ReplicaSet) (int32, error) {
	selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
	if err != nil {
		return 0, fmt.Errorf("parsing replicaset selector, %w", err)
	}
	podList := &corev1.PodList{}
	if err := s.kubeClient.List(ctx, podList, client.InNamespace(rs.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return 0, fmt.Errorf("listing replicaset pods, %w", err)
	}
	pods := lo.ToSlicePtr(podList.Items)
	return podutil.AvailableControlledPods(pods, rs.UID, rs.Spec.MinReadySeconds, s.clock.Now()), nil
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

// complete deletes an isolated pod. It bypasses the eviction API on purpose: the pod is surplus to its ReplicaSet, so
// removing it leaves the workload exactly as available as it was before the drain.
func (s *surgeEvictor) complete(ctx context.Context, pod *corev1.Pod, reason string) error {
	if err := s.kubeClient.Delete(ctx, pod, client.Preconditions{UID: lo.ToPtr(pod.UID)}); err != nil {
		return ignoreRetryable(err, "deleting pod released for surge eviction")
	}
	s.recorder.Publish(terminatorevents.SurgeEvictionCompleted(pod, reason))
	PodsSurgeEvictionsTotal.Inc(map[string]string{OutcomeLabel: SurgeEvictionOutcomeCompleted})
	log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "reason", reason).Info("deleted pod released for surge eviction")
	return nil
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

// replicaSet returns the ReplicaSet with the given name and UID, or nil when it no longer exists (a ReplicaSet
// recreated under the same name is a different ReplicaSet).
func (s *surgeEvictor) replicaSet(ctx context.Context, namespace, name string, uid types.UID) (*appsv1.ReplicaSet, error) {
	rs := &appsv1.ReplicaSet{}
	if err := s.kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, rs); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting replicaset, %w", err)
	}
	if rs.UID != uid {
		return nil, nil
	}
	return rs, nil
}

// replicas is the ReplicaSet's desired pod count; the API defaults an unset count to 1.
func replicas(rs *appsv1.ReplicaSet) int32 {
	return lo.FromPtrOr(rs.Spec.Replicas, 1)
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
