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

package termination_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	podutil "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// envtest runs no ReplicaSet controller, so these tests play its part: they release an isolated pod (drop its
// controller reference) and create its replacement themselves.
var _ = Describe("SurgeEviction", func() {
	const hash = "5d8f7c9b4"
	var node *corev1.Node
	var otherNode *corev1.Node
	var nodeClaim *v1.NodeClaim
	var nodePool *v1.NodePool
	var deployment *appsv1.Deployment
	var rs *appsv1.ReplicaSet
	var appLabels map[string]string
	// drainStart is the time every test starts draining the node, and so the time its pods are isolated.
	var drainStart time.Time

	rsOwnerRef := func() metav1.OwnerReference {
		return metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: new(true), BlockOwnerDeletion: new(true)}
	}
	// rsPod is a running pod of the ReplicaSet, bound to the given node.
	rsPod := func(nodeName string, conditions ...corev1.PodCondition) *corev1.Pod {
		return test.Pod(test.PodOptions{
			NodeName: nodeName,
			ObjectMeta: metav1.ObjectMeta{
				Labels:          lo.Assign(appLabels, map[string]string{podutil.PodTemplateHashLabelKey: hash}),
				OwnerReferences: []metav1.OwnerReference{rsOwnerRef()},
			},
			Phase:      corev1.PodRunning,
			Conditions: conditions,
		})
	}
	ready := func() corev1.PodCondition {
		return corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(env.Clock.Now().Add(-time.Minute))}
	}
	notReady := func() corev1.PodCondition {
		return corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(env.Clock.Now())}
	}
	// release does what the ReplicaSet controller does to a pod whose labels stop matching its selector.
	release := func(pod *corev1.Pod) {
		GinkgoHelper()
		stored := pod.DeepCopy()
		pod.OwnerReferences = lo.Reject(pod.OwnerReferences, func(o metav1.OwnerReference, _ int) bool { return o.UID == rs.UID })
		Expect(env.Client.Patch(ctx, pod, client.MergeFrom(stored))).To(Succeed())
	}
	startDrain := func() {
		GinkgoHelper()
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		node = ExpectNodeExists(ctx, env.Client, node.Name)
		ExpectRequeued(ExpectObjectReconciled(ctx, env.Client, terminationController, node)) // Taint and DrainInitiation
	}
	reconcileDrain := func() {
		GinkgoHelper()
		node = ExpectNodeExists(ctx, env.Client, node.Name)
		ExpectRequeued(ExpectObjectReconciled(ctx, env.Client, terminationController, node))
	}
	expectIsolated := func(pod *corev1.Pod) *corev1.Pod {
		GinkgoHelper()
		pod = ExpectExists(ctx, env.Client, pod)
		Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(pod.Labels).ToNot(HaveKey(podutil.PodTemplateHashLabelKey))
		Expect(pod.Labels).To(HaveKeyWithValue("app", appLabels["app"]))
		Expect(pod.Annotations).To(HaveKeyWithValue(v1.SurgeEvictionReplicaSetAnnotationKey, rs.Name+"/"+string(rs.UID)))
		Expect(pod.Annotations).To(HaveKeyWithValue(v1.SurgeEvictionPodTemplateHashAnnotationKey, hash))
		Expect(pod.Annotations).To(HaveKeyWithValue(v1.SurgeEvictionStartedAnnotationKey, drainStart.UTC().Format(time.RFC3339)))
		Expect(pod.OwnerReferences).To(ContainElement(metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: new(false), BlockOwnerDeletion: new(false),
		}))
		Expect(queue.Has(pod)).To(BeFalse())
		return pod
	}
	expectUntouched := func(pod *corev1.Pod) *corev1.Pod {
		GinkgoHelper()
		pod = ExpectExists(ctx, env.Client, pod)
		Expect(lo.Keys(pod.Annotations)).ToNot(ContainElements(
			v1.SurgeEvictionStartedAnnotationKey, v1.SurgeEvictionReplicaSetAnnotationKey, v1.SurgeEvictionPodTemplateHashAnnotationKey,
		))
		return pod
	}

	BeforeEach(func() {
		env.Clock.SetTime(time.Now().Truncate(time.Second))
		drainStart = env.Clock.Now()
		cloudProvider.Reset()
		*queue = lo.FromPtr(terminator.NewQueue(env.Client, recorder))
		recorder.Reset()
		terminator.PodsSurgeEvictionsTotal.Reset()

		nodePool = test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{v1.SurgeEvictionAnnotationKey: "true"}}})
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{v1.TerminationFinalizer}}})
		node.Labels[v1.NodePoolLabelKey] = nodePool.Name
		cloudProvider.CreatedNodeClaims[node.Spec.ProviderID] = nodeClaim
		otherNode = test.Node()

		deployment = test.Deployment(test.DeploymentOptions{Replicas: 1})
		appLabels = map[string]string{"app": deployment.Name}
		ExpectApplied(ctx, env.Client, deployment)
		rs = test.ReplicaSet(test.ReplicaSetOptions{Selector: lo.Assign(appLabels, map[string]string{podutil.PodTemplateHashLabelKey: hash})})
		rs.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: new(true), BlockOwnerDeletion: new(true),
		}}
		rs.Spec.Replicas = new(int32(1))
		ExpectApplied(ctx, env.Client, rs)
	})

	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
		for _, obj := range []client.Object{rs, deployment} {
			Expect(client.IgnoreNotFound(env.Client.Delete(ctx, obj))).To(Succeed())
		}
	})

	It("should start the replacement before deleting the pod and then finish terminating the node", func() {
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, otherNode, pod)

		startDrain()
		pod = expectIsolated(pod)
		ExpectMetricCounterValue(terminator.PodsSurgeEvictionsTotal, 1, map[string]string{terminator.OutcomeLabel: terminator.SurgeEvictionOutcomeIsolated})

		// The ReplicaSet controller releases the pod and starts a replacement on another node.
		release(pod)
		replacement := rsPod(otherNode.Name, ready())
		ExpectApplied(ctx, env.Client, replacement)

		env.Clock.Step(2 * termination.MinDrainTime)
		reconcileDrain()
		EventuallyExpectTerminating(ctx, env.Client, pod)
		Expect(queue.Has(pod)).To(BeFalse())
		ExpectExists(ctx, env.Client, replacement)
		ExpectMetricCounterValue(terminator.PodsSurgeEvictionsTotal, 1, map[string]string{terminator.OutcomeLabel: terminator.SurgeEvictionOutcomeCompleted})

		// The kubelet finishes terminating the pod.
		ExpectDeleted(ctx, env.Client, pod)
		reconcileDrain()                                                                        // DrainValidation, VolumeDetachment, InstanceTerminationInitiation
		ExpectNotRequeued(ExpectObjectReconciled(ctx, env.Client, terminationController, node)) // InstanceTerminationValidation
		ExpectNotFound(ctx, env.Client, node)
	})
	It("should not delete the isolated pod while its replacement is not ready", func() {
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, otherNode, pod)
		startDrain()
		pod = expectIsolated(pod)

		release(pod)
		replacement := rsPod(otherNode.Name, notReady())
		ExpectApplied(ctx, env.Client, replacement)
		env.Clock.Step(2 * termination.MinDrainTime)
		for range 3 {
			reconcileDrain()
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		}
		pod = expectIsolated(pod)

		// The replacement becomes ready.
		replacement.Status.Conditions = []corev1.PodCondition{ready()}
		Expect(env.Client.Status().Update(ctx, replacement)).To(Succeed())
		reconcileDrain()
		EventuallyExpectTerminating(ctx, env.Client, pod)
	})
	It("should not delete the isolated pod until the ReplicaSet has released it", func() {
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, otherNode, pod)
		startDrain()
		pod = expectIsolated(pod)

		// A ready pod of the ReplicaSet already exists, but the ReplicaSet has not observed the release yet: the isolated
		// pod still counts toward it, so deleting the pod now could leave the ReplicaSet without a replacement.
		ExpectApplied(ctx, env.Client, rsPod(otherNode.Name, ready()))
		env.Clock.Step(2 * termination.MinDrainTime)
		reconcileDrain()
		reconcileDrain()
		pod = expectIsolated(pod)

		release(pod)
		reconcileDrain()
		EventuallyExpectTerminating(ctx, env.Client, pod)
	})
	It("should keep the node draining and leave the isolated pod alone across repeated reconciles", func() {
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, otherNode, pod)
		startDrain()
		pod = expectIsolated(pod)
		resourceVersion := pod.ResourceVersion

		for range 3 {
			reconcileDrain()
		}
		pod = expectIsolated(pod)
		Expect(pod.ResourceVersion).To(Equal(resourceVersion))
		ExpectMetricCounterValue(terminator.PodsSurgeEvictionsTotal, 1, map[string]string{terminator.OutcomeLabel: terminator.SurgeEvictionOutcomeIsolated})
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
	})
	It("should roll back after the timeout and then evict the pod through the eviction API", func() {
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, otherNode, pod)
		startDrain()
		pod = expectIsolated(pod)

		release(pod)
		ExpectApplied(ctx, env.Client, rsPod(otherNode.Name, notReady()))
		env.Clock.Step(9 * time.Minute)
		reconcileDrain()
		pod = expectIsolated(pod)

		env.Clock.Step(2 * time.Minute)
		reconcileDrain()
		pod = ExpectExists(ctx, env.Client, pod)
		Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(pod.Labels).To(HaveKeyWithValue(podutil.PodTemplateHashLabelKey, hash))
		Expect(pod.Annotations).To(HaveKeyWithValue(v1.SurgeEvictionAbortedAnnotationKey, env.Clock.Now().UTC().Format(time.RFC3339)))
		Expect(lo.Keys(pod.Annotations)).ToNot(ContainElements(
			v1.SurgeEvictionStartedAnnotationKey, v1.SurgeEvictionReplicaSetAnnotationKey, v1.SurgeEvictionPodTemplateHashAnnotationKey,
		))
		Expect(lo.Map(pod.OwnerReferences, func(o metav1.OwnerReference, _ int) string { return o.Kind })).ToNot(ContainElement("Deployment"))
		ExpectMetricCounterValue(terminator.PodsSurgeEvictionsTotal, 1, map[string]string{terminator.OutcomeLabel: terminator.SurgeEvictionOutcomeAborted})

		// The aborted pod drains through the eviction API like any other pod.
		reconcileDrain()
		Expect(queue.Has(pod)).To(BeTrue())
		ExpectObjectReconciled(ctx, env.Client, queue, pod)
		EventuallyExpectTerminating(ctx, env.Client, pod)
		pod = ExpectExists(ctx, env.Client, pod)
		Expect(pod.Labels).To(HaveKeyWithValue(podutil.PodTemplateHashLabelKey, hash))
	})
	It("should delete the isolated pod at once when the ReplicaSet is scaled to zero", func() {
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, otherNode, pod)
		startDrain()
		pod = expectIsolated(pod)

		rs.Spec.Replicas = new(int32(0))
		ExpectApplied(ctx, env.Client, rs)
		reconcileDrain()
		EventuallyExpectTerminating(ctx, env.Client, pod)
	})
	It("should delete the isolated pod at once when the ReplicaSet is deleted", func() {
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, otherNode, pod)
		startDrain()
		pod = expectIsolated(pod)

		Expect(env.Client.Delete(ctx, rs)).To(Succeed())
		reconcileDrain()
		EventuallyExpectTerminating(ctx, env.Client, pod)
	})
	It("should evict through the eviction API when the ReplicaSet is already scaled to zero", func() {
		rs.Spec.Replicas = new(int32(0))
		ExpectApplied(ctx, env.Client, rs)
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, pod)
		startDrain()
		pod = expectUntouched(pod)
		Expect(pod.Labels).To(HaveKeyWithValue(podutil.PodTemplateHashLabelKey, hash))
		Expect(queue.Has(pod)).To(BeTrue())
		ExpectMetricCounterValue(terminator.PodsSurgeEvictionsTotal, 1, map[string]string{terminator.OutcomeLabel: terminator.SurgeEvictionOutcomeFallback})
	})
	It("should evict through the eviction API when the ReplicaSet does not exist", func() {
		pod := rsPod(node.Name, ready())
		Expect(env.Client.Delete(ctx, rs)).To(Succeed())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, pod)
		startDrain()
		expectUntouched(pod)
		Expect(queue.Has(pod)).To(BeTrue())
	})
	It("should evict through the eviction API when the pod opts out", func() {
		pod := rsPod(node.Name, ready())
		pod.Annotations = map[string]string{v1.SurgeEvictionAnnotationKey: "false"}
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, pod)
		startDrain()
		pod = expectUntouched(pod)
		Expect(pod.Labels).To(HaveKeyWithValue(podutil.PodTemplateHashLabelKey, hash))
		Expect(queue.Has(pod)).To(BeTrue())
	})
	It("should evict pods that are not controlled by a ReplicaSet through the eviction API", func() {
		bare := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{podutil.PodTemplateHashLabelKey: hash}}})
		statefulSetPod := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{
			Labels:          map[string]string{podutil.PodTemplateHashLabelKey: hash},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: "db-uid", Controller: new(true)}},
		}})
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, bare, statefulSetPod)
		startDrain()
		for _, pod := range []*corev1.Pod{bare, statefulSetPod} {
			pod = expectUntouched(pod)
			Expect(pod.Labels).To(HaveKeyWithValue(podutil.PodTemplateHashLabelKey, hash))
			Expect(queue.Has(pod)).To(BeTrue())
		}
	})
	It("should evict through the eviction API when the NodePool does not enable surge eviction", func() {
		delete(nodePool.Annotations, v1.SurgeEvictionAnnotationKey)
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, pod)
		startDrain()
		pod = expectUntouched(pod)
		Expect(pod.Labels).To(HaveKeyWithValue(podutil.PodTemplateHashLabelKey, hash))
		Expect(pod.OwnerReferences).To(Equal([]metav1.OwnerReference{rsOwnerRef()}))
		Expect(queue.Has(pod)).To(BeTrue())
		Expect(recorder.Calls("SurgeEviction")).To(BeZero())
	})
	It("should surge evict a pod that opts in on a NodePool that does not enable it", func() {
		delete(nodePool.Annotations, v1.SurgeEvictionAnnotationKey)
		pod := rsPod(node.Name, ready())
		pod.Annotations = map[string]string{v1.SurgeEvictionAnnotationKey: "true"}
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, pod)
		startDrain()
		expectIsolated(pod)
	})
	It("should surge evict the pod even when a PodDisruptionBudget allows no disruptions", func() {
		budget := test.PodDisruptionBudget(test.PDBOptions{Labels: appLabels, MaxUnavailable: new(intstr.FromInt32(0))})
		pod := rsPod(node.Name, ready())
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, otherNode, pod, budget)
		startDrain()
		pod = expectIsolated(pod)

		release(pod)
		ExpectApplied(ctx, env.Client, rsPod(otherNode.Name, ready()))
		reconcileDrain()
		EventuallyExpectTerminating(ctx, env.Client, pod)
	})
	It("should still force-delete an isolated pod when the NodeClaim's terminationGracePeriod expires", func() {
		nodeClaim.Spec.TerminationGracePeriod = &metav1.Duration{Duration: 300 * time.Second}
		nodeClaim.Annotations = map[string]string{
			v1.NodeClaimTerminationTimestampAnnotationKey: env.Clock.Now().Add(nodeClaim.Spec.TerminationGracePeriod.Duration).Format(time.RFC3339),
		}
		pod := rsPod(node.Name, ready())
		pod.Spec.TerminationGracePeriodSeconds = new(int64(60))
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, pod)
		startDrain()
		pod = expectIsolated(pod)

		// The ReplicaSet never becomes available again, but the node's deadline leaves the pod only its own grace period.
		env.Clock.Step(250 * time.Second)
		reconcileDrain()
		EventuallyExpectTerminating(ctx, env.Client, pod)
	})
})
