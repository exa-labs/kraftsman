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
// isolated pod is deleted once the ReplicaSet is fully available again. The functions here are pure: they decide from
// the pod, its NodePool and the ReplicaSet's pods alone, so the disruption and termination controllers agree on which
// pods are surge evicted. See the SurgeEviction* annotation keys in pkg/apis/v1 for the API.

package pod

import (
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

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

// IsSurgeEvictable reports whether Karpenter drains the pod by surge eviction rather than the eviction API. The pod must:
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
