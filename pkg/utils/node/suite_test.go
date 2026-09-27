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

package node_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var (
	ctx context.Context
	env *test.Environment
)

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "NodeUtils")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...), test.WithFieldIndexers(test.NodeClaimProviderIDFieldIndexer(ctx)))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

var _ = Describe("NodeUtils", func() {
	var testNode *corev1.Node
	var nodeClaim *v1.NodeClaim
	BeforeEach(func() {
		nodeClaim = test.NodeClaim()
	})
	It("should return nodeClaim for node which has the same provider ID", func() {
		testNode = test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, testNode, nodeClaim)

		nodeClaims, err := nodeutils.GetNodeClaims(ctx, env.Client, testNode)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodeClaims).To(HaveLen(1))
		for _, nc := range nodeClaims {
			Expect(nc.Status.ProviderID).To(BeEquivalentTo(testNode.Spec.ProviderID))
		}
	})
	It("should not return nodeClaim for node since the node supplied here has different provider ID", func() {
		testNode = test.Node(test.NodeOptions{
			ProviderID: "testID",
		})
		ExpectApplied(ctx, env.Client, testNode, nodeClaim)

		nodeClaims, err := nodeutils.GetNodeClaims(ctx, env.Client, testNode)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodeClaims).To(HaveLen(0))
	})
	It("should not return nodeClaim for node since the node supplied here has no provider ID", func() {
		testNode = test.Node(test.NodeOptions{
			ProviderID: "",
		})
		ExpectApplied(ctx, env.Client, testNode, nodeClaim)

		nodeClaims, err := nodeutils.GetNodeClaims(ctx, env.Client, testNode)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodeClaims).To(HaveLen(0))
	})
})

var _ = Describe("GetProvisionablePods", func() {
	const nominatedNode = "reserved-node"
	var pending *corev1.Pod
	var victim *corev1.Pod

	volcanoNominated := func() *corev1.Pod {
		p := test.UnschedulablePod()
		p.Spec.SchedulerName = podutils.VolcanoSchedulerName
		p.Status.NominatedNodeName = nominatedNode
		return p
	}
	provisionableNames := func() []string {
		pods, err := nodeutils.GetProvisionablePods(ctx, env.Client)
		Expect(err).NotTo(HaveOccurred())
		return lo.Map(pods, func(p *corev1.Pod, _ int) string { return p.Name })
	}

	BeforeEach(func() {
		victim = test.Pod(test.PodOptions{NodeName: nominatedNode})
	})

	It("should hold back a volcano pod nominated to a node whose victims are still terminating", func() {
		pending = volcanoNominated()
		ExpectApplied(ctx, env.Client, pending, victim)
		Expect(pending.Status.NominatedNodeName).To(Equal(nominatedNode))
		Expect(provisionableNames()).To(ConsistOf(pending.Name))

		ExpectDeletionTimestampSet(ctx, env.Client, victim)
		Expect(provisionableNames()).To(BeEmpty())
	})

	It("should release the volcano pod once nothing on the nominated node is terminating", func() {
		pending = volcanoNominated()
		bystander := test.Pod(test.PodOptions{NodeName: nominatedNode})
		ExpectApplied(ctx, env.Client, pending, bystander)
		Expect(provisionableNames()).To(ConsistOf(pending.Name))
	})

	It("should only hold back pods nominated to the draining node", func() {
		pending = volcanoNominated()
		elsewhere := volcanoNominated()
		elsewhere.Status.NominatedNodeName = "other-node"
		plain := test.UnschedulablePod()
		plain.Spec.SchedulerName = podutils.VolcanoSchedulerName
		ExpectApplied(ctx, env.Client, pending, elsewhere, plain, victim)
		ExpectDeletionTimestampSet(ctx, env.Client, victim)
		Expect(provisionableNames()).To(ConsistOf(elsewhere.Name, plain.Name))
	})

	It("should keep excluding kube-scheduler nominations regardless of the nominated node", func() {
		pending = test.UnschedulablePod()
		pending.Spec.SchedulerName = "default-scheduler"
		pending.Status.NominatedNodeName = nominatedNode
		ExpectApplied(ctx, env.Client, pending)
		Expect(provisionableNames()).To(BeEmpty())
	})
})
