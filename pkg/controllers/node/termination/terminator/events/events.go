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

package events

import (
	"fmt"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	storagev1 "k8s.io/api/storage/v1"
)

func EvictPod(pod *corev1.Pod, reason string) events.Event {
	return events.Event{
		InvolvedObject: pod,
		Type:           corev1.EventTypeNormal,
		Reason:         events.Evicted,
		Message:        "Evicted pod: " + reason,
		DedupeValues:   []string{pod.Name},
	}
}

func DisruptPodDelete(pod *corev1.Pod, gracePeriodSeconds *int64, nodeGracePeriodTerminationTime *time.Time) events.Event {
	return events.Event{
		InvolvedObject: pod,
		Type:           corev1.EventTypeNormal,
		Reason:         events.Disrupted,
		Message:        fmt.Sprintf("Deleting the pod to accommodate the terminationTime %v of the node. The pod was granted %v seconds of grace-period of its %v terminationGracePeriodSeconds. This bypasses the PDB of the pod and the do-not-disrupt annotation.", lo.FromPtr(nodeGracePeriodTerminationTime), lo.FromPtr(gracePeriodSeconds), lo.FromPtr(pod.Spec.TerminationGracePeriodSeconds)),
		DedupeValues:   []string{pod.Name},
	}
}

// SurgeEvictionIsolated is published when a pod is released from its ReplicaSet so that the ReplicaSet starts its
// replacement while the pod keeps running.
func SurgeEvictionIsolated(pod *corev1.Pod, replicaSet string) events.Event {
	return surgeEviction(pod, corev1.EventTypeNormal, "isolated", fmt.Sprintf("Released pod from ReplicaSet %s so it starts a replacement; the pod is deleted once the ReplicaSet is fully available", replicaSet))
}

// SurgeEvictionCompleted is published when a pod released for surge eviction is handed to the eviction queue.
func SurgeEvictionCompleted(pod *corev1.Pod, reason string) events.Event {
	return surgeEviction(pod, corev1.EventTypeNormal, "completed", "Evicting pod released for surge eviction: "+reason)
}

// SurgeEvictionAborted is published when a surge is rolled back because its ReplicaSet did not become fully available in time.
func SurgeEvictionAborted(pod *corev1.Pod, reason string) events.Event {
	return surgeEviction(pod, corev1.EventTypeWarning, "aborted", "Restored pod to its ReplicaSet, it drains through the eviction API: "+reason)
}

// SurgeEvictionFallback is published when a pod eligible for surge eviction drains through the eviction API instead.
func SurgeEvictionFallback(pod *corev1.Pod, reason string) events.Event {
	return surgeEviction(pod, corev1.EventTypeNormal, "fallback", "Draining pod through the eviction API instead of surge eviction: "+reason)
}

func surgeEviction(pod *corev1.Pod, eventType, outcome, message string) events.Event {
	return events.Event{
		InvolvedObject: pod,
		Type:           eventType,
		Reason:         events.SurgeEviction,
		Message:        message,
		DedupeValues:   []string{pod.Name, outcome},
	}
}

func NodeFailedToDrain(node *corev1.Node, err error) events.Event {
	return events.Event{
		InvolvedObject: node,
		Type:           corev1.EventTypeWarning,
		Reason:         events.FailedDraining,
		Message:        fmt.Sprintf("Failed to drain node, %s", err),
		DedupeValues:   []string{node.Name},
	}
}

func NodeAwaitingVolumeDetachmentEvent(node *corev1.Node, volumeAttachments ...*storagev1.VolumeAttachment) events.Event {
	return events.Event{
		InvolvedObject: node,
		Type:           corev1.EventTypeNormal,
		Reason:         "AwaitingVolumeDetachment",
		Message: fmt.Sprintf(
			"Awaiting deletion of bound volumeattachments (%s)",
			pretty.Slice(lo.Map(volumeAttachments, func(va *storagev1.VolumeAttachment, _ int) string {
				return va.Name
			}), 5),
		),
		DedupeValues: []string{node.Name},
	}
}

func NodeTerminationGracePeriodExpiring(node *corev1.Node, terminationTime string) events.Event {
	return events.Event{
		InvolvedObject: node,
		Type:           corev1.EventTypeWarning,
		Reason:         events.TerminationGracePeriodExpiring,
		Message:        fmt.Sprintf("All pods will be deleted by %s", terminationTime),
		DedupeValues:   []string{node.Name},
	}
}

func NodeClaimTerminationGracePeriodExpiring(nodeClaim *v1.NodeClaim, terminationTime string) events.Event {
	return events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         events.TerminationGracePeriodExpiring,
		Message:        fmt.Sprintf("All pods will be deleted by %s", terminationTime),
		DedupeValues:   []string{nodeClaim.Name},
	}
}
