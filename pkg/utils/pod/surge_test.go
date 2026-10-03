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

package pod_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
)

const replicaSetUID = types.UID("rs-uid")

// surgePod is a running pod controlled by an apps/v1 ReplicaSet with a pod-template-hash label: the shape surge
// eviction acts on. mutate adjusts it per case.
func surgePod(mutate func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "web-abc",
			Namespace:   "default",
			Labels:      map[string]string{"app": "web", pod.PodTemplateHashLabelKey: "5d8f7c"},
			Annotations: map[string]string{},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "web-5d8f7c",
				UID:        replicaSetUID,
				Controller: new(true),
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

func surgeNodePool(annotation *string) *v1.NodePool {
	np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default", Annotations: map[string]string{}}}
	if annotation != nil {
		np.Annotations[v1.SurgeEvictionAnnotationKey] = *annotation
	}
	return np
}

func readyPod(controllerUID types.UID, readySince time.Time, mutate func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "replacement",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-5d8f7c", UID: controllerUID, Controller: new(true)}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readySince)}},
		},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

var _ = Describe("SurgeEviction", func() {
	DescribeTable("IsSurgeEvictionEnabled",
		func(podAnnotation, poolAnnotation *string, nilPool bool, expected bool) {
			p := surgePod(func(p *corev1.Pod) {
				if podAnnotation != nil {
					p.Annotations[v1.SurgeEvictionAnnotationKey] = *podAnnotation
				}
			})
			np := surgeNodePool(poolAnnotation)
			if nilPool {
				np = nil
			}
			Expect(pod.IsSurgeEvictionEnabled(p, np)).To(Equal(expected))
		},
		Entry("neither pod nor pool annotated", nil, nil, false, false),
		Entry("pool true", nil, new("true"), false, true),
		Entry("pool true, pod false", new("false"), new("true"), false, false),
		Entry("pool true, pod unrecognized value", new("yes"), new("true"), false, true),
		Entry("pod true, pool unannotated", new("true"), nil, false, true),
		Entry("pod true, pool false", new("true"), new("false"), false, true),
		Entry("pod true, no pool", new("true"), nil, true, true),
		Entry("pool unrecognized value", nil, new("True"), false, false),
		Entry("pool false", nil, new("false"), false, false),
		Entry("no pool", nil, nil, true, false),
	)

	DescribeTable("IsSurgeEvictable",
		func(mutate func(*corev1.Pod), expected bool) {
			Expect(pod.IsSurgeEvictable(surgePod(mutate), surgeNodePool(new("true")), fakeClock, nil)).To(Equal(expected))
		},
		Entry("eligible ReplicaSet pod", nil, true),
		Entry("opted out on the pod", func(p *corev1.Pod) { p.Annotations[v1.SurgeEvictionAnnotationKey] = "false" }, false),
		Entry("no pod-template-hash label", func(p *corev1.Pod) { delete(p.Labels, pod.PodTemplateHashLabelKey) }, false),
		Entry("empty pod-template-hash label", func(p *corev1.Pod) { p.Labels[pod.PodTemplateHashLabelKey] = "" }, false),
		Entry("no owner", func(p *corev1.Pod) { p.OwnerReferences = nil }, false),
		Entry("ReplicaSet owner that is not the controller", func(p *corev1.Pod) { p.OwnerReferences[0].Controller = nil }, false),
		Entry("ReplicaSet of another API group", func(p *corev1.Pod) { p.OwnerReferences[0].APIVersion = "extensions/v1beta1" }, false),
		Entry("StatefulSet controller", func(p *corev1.Pod) { p.OwnerReferences[0].Kind = "StatefulSet" }, false),
		Entry("aborted earlier", func(p *corev1.Pod) { p.Annotations[v1.SurgeEvictionAbortedAnnotationKey] = "2026-01-01T00:00:00Z" }, false),
		Entry("already isolated", func(p *corev1.Pod) { p.Annotations[v1.SurgeEvictionStartedAnnotationKey] = "2026-01-01T00:00:00Z" }, false),
		Entry("do-not-disrupt", func(p *corev1.Pod) { p.Annotations[v1.DoNotDisruptAnnotationKey] = "true" }, false),
		Entry("terminating", func(p *corev1.Pod) { p.DeletionTimestamp = &metav1.Time{Time: time.Now()} }, false),
		Entry("terminal", func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }, false),
		Entry("mirror pod", func(p *corev1.Pod) {
			p.OwnerReferences = append(p.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "Node", Name: "node"})
		}, false),
		Entry("tolerates the disrupted taint", func(p *corev1.Pod) {
			p.Spec.Tolerations = []corev1.Toleration{{Key: v1.DisruptedTaintKey, Operator: corev1.TolerationOpExists}}
		}, false),
	)

	It("should not be surge evictable when the NodePool does not enable it", func() {
		Expect(pod.IsSurgeEvictable(surgePod(nil), surgeNodePool(nil), fakeClock, nil)).To(BeFalse())
		Expect(pod.IsSurgeEvictable(surgePod(nil), nil, fakeClock, nil)).To(BeFalse())
	})

	It("should treat isolated pods as not reschedulable", func() {
		p := surgePod(nil)
		Expect(pod.IsReschedulable(p)).To(BeTrue())
		p.Annotations[v1.SurgeEvictionStartedAnnotationKey] = "2026-01-01T00:00:00Z"
		Expect(pod.IsSurgeIsolated(p)).To(BeTrue())
		Expect(pod.IsReschedulable(p)).To(BeFalse())
	})

	DescribeTable("IsAvailable",
		func(mutate func(*corev1.Pod), minReadySeconds int32, readyFor time.Duration, expected bool) {
			now := fakeClock.Now()
			Expect(pod.IsAvailable(readyPod(replicaSetUID, now.Add(-readyFor), mutate), minReadySeconds, now)).To(Equal(expected))
		},
		Entry("ready, no minReadySeconds", nil, int32(0), time.Duration(0), true),
		Entry("ready for longer than minReadySeconds", nil, int32(30), 31*time.Second, true),
		Entry("ready for exactly minReadySeconds", nil, int32(30), 30*time.Second, false),
		Entry("ready for less than minReadySeconds", nil, int32(30), 10*time.Second, false),
		Entry("not ready", func(p *corev1.Pod) { p.Status.Conditions[0].Status = corev1.ConditionFalse }, int32(0), time.Hour, false),
		Entry("no ready condition", func(p *corev1.Pod) { p.Status.Conditions = nil }, int32(0), time.Hour, false),
		Entry("terminating", func(p *corev1.Pod) { p.DeletionTimestamp = &metav1.Time{Time: time.Now()} }, int32(0), time.Hour, false),
		Entry("ready with no transition time and minReadySeconds", func(p *corev1.Pod) { p.Status.Conditions[0].LastTransitionTime = metav1.Time{} }, int32(5), time.Hour, false),
	)

	DescribeTable("AvailableControlledPods",
		func(pods []*corev1.Pod, minReadySeconds int32, expected int32) {
			Expect(pod.AvailableControlledPods(pods, replicaSetUID, minReadySeconds, fakeClock.Now())).To(Equal(expected))
		},
		Entry("no pods", []*corev1.Pod{}, int32(0), int32(0)),
		Entry("counts ready pods of the ReplicaSet", []*corev1.Pod{
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), nil),
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), nil),
		}, int32(0), int32(2)),
		Entry("ignores pods of another controller", []*corev1.Pod{
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), nil),
			readyPod("other-uid", time.Now().Add(-time.Minute), nil),
		}, int32(0), int32(1)),
		Entry("ignores released pods with no controller", []*corev1.Pod{
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), func(p *corev1.Pod) { p.OwnerReferences = nil }),
		}, int32(0), int32(0)),
		Entry("ignores pods whose owner reference is not the controller", []*corev1.Pod{
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), func(p *corev1.Pod) { p.OwnerReferences[0].Controller = new(false) }),
		}, int32(0), int32(0)),
		Entry("ignores terminating and not-ready pods", []*corev1.Pod{
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), func(p *corev1.Pod) { p.DeletionTimestamp = &metav1.Time{Time: time.Now()} }),
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), func(p *corev1.Pod) { p.Status.Conditions[0].Status = corev1.ConditionFalse }),
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), nil),
		}, int32(0), int32(1)),
		Entry("applies minReadySeconds", []*corev1.Pod{
			readyPod(replicaSetUID, time.Now().Add(-time.Minute), nil),
			readyPod(replicaSetUID, time.Now().Add(-time.Second), nil),
		}, int32(30), int32(1)),
	)
})
