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

package state_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Unlaunched NodeClaims", func() {
	unlaunched := func() *v1.NodeClaim {
		nodeClaim := test.NodeClaim()
		nodeClaim.Status.ProviderID = ""
		return nodeClaim
	}

	It("should record a NodeClaim the provisioner just created, before the informer sees it", func() {
		nodeClaim := unlaunched()
		cluster.UpdateNodeClaim(nodeClaim)
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: nodeClaim.Name}))
	})
	It("should not take the default Launched condition for a launch attempt", func() {
		nodeClaim := unlaunched()
		nodeClaim.StatusConditions().SetUnknown(v1.ConditionTypeLaunched)
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: nodeClaim.Name}))
	})
	It("should record the reason a launch attempt left on the Launched condition", func() {
		nodeClaim := unlaunched()
		nodeClaim.StatusConditions().SetUnknownWithReason(v1.ConditionTypeLaunched, "CapacityEvidencePending", "deferred")
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: nodeClaim.Name, LaunchAttemptReason: "CapacityEvidencePending"}))
	})
	It("should record that an unlaunched NodeClaim is deleting", func() {
		nodeClaim := unlaunched()
		nodeClaim.Finalizers = []string{v1.TerminationFinalizer}
		ExpectApplied(ctx, env.Client, nodeClaim)
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: nodeClaim.Name, Deleting: true}))
	})
	It("should record that a NodeClaim is a disruption replacement", func() {
		nodeClaim := unlaunched()
		nodeClaim.Annotations = map[string]string{v1.NodeClaimReplacementOriginAnnotationKey: "underutilized:spot"}
		cluster.UpdateNodeClaim(nodeClaim)
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: nodeClaim.Name, Replacement: true}))
	})
	It("should not mutate the conditions of the NodeClaim it reads", func() {
		nodeClaim := unlaunched()
		nodeClaim.Status.Conditions = nil
		cluster.UpdateNodeClaim(nodeClaim)
		Expect(nodeClaim.Status.Conditions).To(BeEmpty())
	})
	It("should record that the pods a NodeClaim was created for are recorded", func() {
		nodeClaim := unlaunched()
		cluster.UpdateNodeClaim(nodeClaim)
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: nodeClaim.Name}))

		pod := test.Pod()
		cluster.UpdatePodToNodeClaimMapping(map[string][]*corev1.Pod{nodeClaim.Name: {pod}})
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: nodeClaim.Name, PodsRecorded: true}))
		Expect(cluster.PodNodeClaimMapping(client.ObjectKeyFromObject(pod))).To(Equal(nodeClaim.Name))
	})
	It("should forget the record when the NodeClaim is deleted", func() {
		nodeClaim := unlaunched()
		cluster.UpdateNodeClaim(nodeClaim)
		cluster.UpdatePodToNodeClaimMapping(map[string][]*corev1.Pod{nodeClaim.Name: {test.Pod()}})
		cluster.DeleteNodeClaim(nodeClaim.Name)
		// A NodeClaim recreated under the same name starts without a record.
		cluster.UpdateNodeClaim(nodeClaim)
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: nodeClaim.Name}))
	})
	It("should drop a NodeClaim once it launches", func() {
		nodeClaim := unlaunched()
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.UnlaunchedNodeClaims()).To(HaveLen(1))

		nodeClaim.Status.ProviderID = test.RandomProviderID()
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.UnlaunchedNodeClaims()).To(BeEmpty())
	})
	It("should drop a NodeClaim deleted before it launched", func() {
		nodeClaim := unlaunched()
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectDeleted(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.UnlaunchedNodeClaims()).To(BeEmpty())
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should list exactly the NodeClaims that hold Synced false", func() {
		launched := test.NodeClaim()
		pending := unlaunched()
		ExpectApplied(ctx, env.Client, launched, pending)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(launched))
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(pending))
		Expect(cluster.Synced(ctx)).To(BeFalse())
		Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: pending.Name}))
	})
})
