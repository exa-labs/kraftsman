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

// Surge eviction predicates. Surge eviction drains a ReplicaSet pod by removing its pod-template-hash label, which makes
// the ReplicaSet release the pod and create a replacement while the released ("isolated") pod keeps running; the
// isolated pod is evicted once the ReplicaSet is fully available again. The predicates here decide from the pod, its
// NodePool, its ReplicaSet and the ReplicaSet's pods, and are shared by the disruption and termination controllers so
// that both agree on which pods are surge evicted and when a surge can complete. See the SurgeEviction* annotation keys
// in pkg/apis/v1 for the API.

package pod

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

// PodTemplateHashLabelKey is the label a Deployment adds to its ReplicaSets' selectors and pods so that each
// ReplicaSet selects only the pods of its own template revision.
const PodTemplateHashLabelKey = appsv1.DefaultDeploymentUniqueLabelKey

// IsSurgeEvictionEnabled reports whether surge eviction is requested for the pod: the pod's annotation is "true", or
// its NodePool's annotation is "true" and the pod's annotation is not "false". A nil NodePool counts as unset.
func IsSurgeEvictionEnabled(pod *corev1.Pod, nodePool *v1.NodePool) bool {
	switch pod.Annotations[v1.SurgeEvictionAnnotationKey] {
	case "true":
		return true
	case "false":
		return false
	}
	return nodePool != nil && nodePool.Annotations[v1.SurgeEvictionAnnotationKey] == "true"
}

// IsSurgeEvictable reports whether Karpenter drains the pod by surge eviction (release it from its ReplicaSet, then
// evict it once the ReplicaSet is fully available again) rather than evicting it straight away. The pod must:
// - Have surge eviction enabled (see IsSurgeEvictionEnabled)
// - Carry no karpenter.sh/surge-eviction-aborted annotation from an earlier surge that was rolled back
// - Be controlled by an apps/v1 ReplicaSet and carry a non-empty pod-template-hash label
// - Be evictable (see IsEvictable)
// An isolated pod is never surge evictable: it has already lost the label and, once released, its controller.
func IsSurgeEvictable(pod *corev1.Pod, nodePool *v1.NodePool, clk clock.Clock, recorder events.Recorder) bool {
	return IsSurgeEvictionEnabled(pod, nodePool) && HasSurgeEvictableShape(pod) && IsEvictable(pod, clk, recorder)
}

// HasSurgeEvictableShape checks the parts of IsSurgeEvictable that do not depend on configuration or time: the pod is
// not isolated, has not had a surge rolled back, is controlled by an apps/v1 ReplicaSet and carries a non-empty
// pod-template-hash label.
func HasSurgeEvictableShape(pod *corev1.Pod) bool {
	if _, aborted := pod.Annotations[v1.SurgeEvictionAbortedAnnotationKey]; aborted {
		return false
	}
	return !IsSurgeIsolated(pod) &&
		pod.Labels[PodTemplateHashLabelKey] != "" &&
		ReplicaSetControllerRef(pod) != nil
}

// IsSurgeIsolated reports whether Karpenter has released the pod from its ReplicaSet for surge eviction and has not
// rolled that back. An isolated pod's replacement already exists, so it is never rescheduled itself.
func IsSurgeIsolated(pod *corev1.Pod) bool {
	_, ok := pod.Annotations[v1.SurgeEvictionStartedAnnotationKey]
	return ok
}

// ReplicaSetControllerRef returns the pod's controller owner reference when that controller is an apps/v1 ReplicaSet.
func ReplicaSetControllerRef(pod *corev1.Pod) *metav1.OwnerReference {
	ref := metav1.GetControllerOf(pod)
	if ref == nil || ref.Kind != "ReplicaSet" || ref.APIVersion != appsv1.SchemeGroupVersion.String() {
		return nil
	}
	return ref
}

// IsAvailable reports whether the pod is available in the sense a ReplicaSet counts availableReplicas: it is not
// terminating and its Ready condition has been true for at least minReadySeconds as of now.
func IsAvailable(pod *corev1.Pod, minReadySeconds int32, now time.Time) bool {
	if IsTerminating(pod) {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type != corev1.PodReady {
			continue
		}
		if c.Status != corev1.ConditionTrue {
			return false
		}
		if minReadySeconds == 0 {
			return true
		}
		return !c.LastTransitionTime.IsZero() && c.LastTransitionTime.Add(time.Duration(minReadySeconds)*time.Second).Before(now)
	}
	return false
}

// AvailableControlledPods counts the pods controlled by the object with the given UID that are available (see
// IsAvailable). It decides from the pods themselves rather than the controller's status, which lags a release.
func AvailableControlledPods(pods []*corev1.Pod, controllerUID types.UID, minReadySeconds int32, now time.Time) int32 {
	var count int32
	for _, p := range pods {
		if ref := metav1.GetControllerOf(p); ref == nil || ref.UID != controllerUID {
			continue
		}
		if IsAvailable(p, minReadySeconds, now) {
			count++
		}
	}
	return count
}

// GetReplicaSet returns the ReplicaSet with the given name and UID, or nil when it no longer exists. A ReplicaSet
// recreated under the same name is a different ReplicaSet and also yields nil.
func GetReplicaSet(ctx context.Context, kubeClient client.Client, namespace, name string, uid types.UID) (*appsv1.ReplicaSet, error) {
	rs := &appsv1.ReplicaSet{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, rs); err != nil {
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

// ReplicaSetReplicas is the ReplicaSet's desired pod count; the API defaults an unset count to 1.
func ReplicaSetReplicas(rs *appsv1.ReplicaSet) int32 {
	return lo.FromPtrOr(rs.Spec.Replicas, 1)
}

// CanReleaseFromReplicaSet reports whether removing the pod's pod-template-hash label makes the ReplicaSet release it:
// the ReplicaSet must select on that label with the pod's value, as the ReplicaSets a Deployment creates do.
func CanReleaseFromReplicaSet(pod *corev1.Pod, rs *appsv1.ReplicaSet) bool {
	hash := pod.Labels[PodTemplateHashLabelKey]
	return hash != "" && rs.Spec.Selector != nil && rs.Spec.Selector.MatchLabels[PodTemplateHashLabelKey] == hash
}

// AvailableReplicaSetPods counts the ReplicaSet's own pods that are available (see AvailableControlledPods). It reads
// pods rather than the ReplicaSet's status, which lags the release of a pod and the start of its replacement. A pod
// still controlled by the ReplicaSet but no longer matching its selector is not counted.
func AvailableReplicaSetPods(ctx context.Context, kubeClient client.Client, rs *appsv1.ReplicaSet, now time.Time) (int32, error) {
	selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
	if err != nil {
		return 0, fmt.Errorf("parsing replicaset selector, %w", err)
	}
	podList := &corev1.PodList{}
	if err := kubeClient.List(ctx, podList, client.InNamespace(rs.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return 0, fmt.Errorf("listing replicaset pods, %w", err)
	}
	return AvailableControlledPods(lo.ToSlicePtr(podList.Items), rs.UID, rs.Spec.MinReadySeconds, now), nil
}

// IsReplicaSetFullyAvailable reports whether the ReplicaSet wants at least one pod and has all of them available.
func IsReplicaSetFullyAvailable(ctx context.Context, kubeClient client.Client, rs *appsv1.ReplicaSet, now time.Time) (bool, int32, error) {
	replicas := ReplicaSetReplicas(rs)
	if replicas < 1 {
		return false, 0, nil
	}
	available, err := AvailableReplicaSetPods(ctx, kubeClient, rs, now)
	if err != nil {
		return false, 0, err
	}
	return available >= replicas, available, nil
}

// CanCompleteSurgeEviction reports whether a surge eviction of the pod, started now, would end with the ReplicaSet
// as available as it is now: the pod's ReplicaSet releases it when its pod-template-hash label is removed, and has all
// of its replicas available counting the pod itself. A degraded ReplicaSet's surge cannot complete before it recovers,
// so its pods are not exempt from the checks that guard the eviction API. Any read error yields false.
func CanCompleteSurgeEviction(ctx context.Context, kubeClient client.Client, pod *corev1.Pod, now time.Time) bool {
	ref := ReplicaSetControllerRef(pod)
	if ref == nil {
		return false
	}
	rs, err := GetReplicaSet(ctx, kubeClient, pod.Namespace, ref.Name, ref.UID)
	if err != nil || rs == nil || !CanReleaseFromReplicaSet(pod, rs) {
		return false
	}
	full, _, err := IsReplicaSetFullyAvailable(ctx, kubeClient, rs, now)
	return err == nil && full
}
