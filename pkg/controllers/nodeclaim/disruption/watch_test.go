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

package disruption_test

import (
	"context"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	nodeclaimdisruption "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// driftCountingCloudProvider counts IsDrifted calls, one per drift evaluation of a launched NodeClaim whose
// instance type check is cached, so the count tracks how often the controller reconciles the NodeClaim.
type driftCountingCloudProvider struct {
	*fake.CloudProvider
	isDriftedCalls atomic.Int64
}

func (c *driftCountingCloudProvider) IsDrifted(ctx context.Context, nodeClaim *v1.NodeClaim) (cloudprovider.DriftReason, error) {
	c.isDriftedCalls.Add(1)
	return c.CloudProvider.IsDrifted(ctx, nodeClaim)
}

var _ = Describe("Watches", func() {
	It("should not reconcile a NodeClaim on pod events, only on changes to the NodeClaim", func() {
		countingCloudProvider := &driftCountingCloudProvider{CloudProvider: cp}
		mgr, err := controllerruntime.NewManager(env.Config, controllerruntime.Options{
			Scheme:                 scheme.Scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			Controller:             config.Controller{SkipNameValidation: lo.ToPtr(true)},
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(test.NodeProviderIDFieldIndexer(ctx)(mgr.GetCache())).To(Succeed())
		Expect(test.NodeClaimProviderIDFieldIndexer(ctx)(mgr.GetCache())).To(Succeed())
		controller := nodeclaimdisruption.NewController(env.Clock, mgr.GetClient(), countingCloudProvider)
		Expect(controller.Register(ctx, mgr)).To(Succeed())

		mgrCtx, cancel := context.WithCancel(ctx)
		DeferCleanup(cancel)
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()

		nodePool := test.NodePool()
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: it.Name,
					corev1.LabelTopologyZone:       "test-zone-1a",
					v1.CapacityTypeLabelKey:        v1.CapacityTypeSpot,
				},
			},
		})
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)

		// The NodeClaim's own create and status events reconcile it; wait for those to settle.
		Eventually(countingCloudProvider.isDriftedCalls.Load).WithTimeout(10 * time.Second).Should(BeNumerically(">", 0))
		var settled int64
		Eventually(func() bool {
			before := countingCloudProvider.isDriftedCalls.Load()
			time.Sleep(time.Second)
			settled = countingCloudProvider.isDriftedCalls.Load()
			return before == settled
		}).WithTimeout(15 * time.Second).Should(BeTrue())

		// Pods binding to, changing on, and leaving the node carry nothing the drift or consolidation reconcilers read:
		// consolidation reads the NodeClaim's lastPodEventTime, which the podevents controller writes.
		pod := test.Pod(test.PodOptions{NodeName: node.Name, Phase: corev1.PodRunning})
		ExpectApplied(ctx, env.Client, pod)
		for i := range 5 {
			stored := pod.DeepCopy()
			pod.Labels = lo.Assign(pod.Labels, map[string]string{"revision": string(rune('a' + i))})
			Expect(env.Client.Patch(ctx, pod, client.MergeFrom(stored))).To(Succeed())
		}
		ExpectDeleted(ctx, env.Client, pod)
		Consistently(countingCloudProvider.isDriftedCalls.Load).WithTimeout(3 * time.Second).Should(Equal(settled))

		// A change to the NodeClaim, such as the podevents controller stamping lastPodEventTime, still reconciles it.
		stored := nodeClaim.DeepCopy()
		nodeClaim.Status.LastPodEventTime = metav1.NewTime(time.Now())
		Expect(env.Client.Status().Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		Eventually(countingCloudProvider.isDriftedCalls.Load).WithTimeout(10 * time.Second).Should(BeNumerically(">", settled))
	})
})
