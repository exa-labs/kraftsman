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
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Disruption Sync Policy", func() {
	var nodePool *v1.NodePool
	var emptyNodeClaim *v1.NodeClaim
	var emptyNode *corev1.Node

	withPolicy := func(policy options.DisruptionSyncPolicy) {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{DisruptionSyncPolicy: lo.ToPtr(policy)}))
	}
	// applyUnlaunched creates a NodeClaim without a provider ID in the NodePool and informs cluster
	// state about it, the way the provisioner's create and the NodeClaim informer do.
	applyUnlaunched := func(np *v1.NodePool, mutate func(*v1.NodeClaim)) *v1.NodeClaim {
		nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: np.Name}}})
		nodeClaim.Status.ProviderID = ""
		if mutate != nil {
			mutate(nodeClaim)
		}
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))
		return nodeClaim
	}
	// markDeleting gives the NodeClaim a deletion timestamp that a finalizer holds open, as the
	// lifecycle controller does after an insufficient-capacity launch.
	markDeleting := func(nodeClaim *v1.NodeClaim) {
		live := ExpectExists(ctx, env.Client, nodeClaim)
		live.Finalizers = append(live.Finalizers, v1.TerminationFinalizer)
		ExpectApplied(ctx, env.Client, live)
		Expect(env.Client.Delete(ctx, live)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(live))
	}
	deferred := func(nc *v1.NodeClaim) {
		nc.StatusConditions().SetUnknownWithReason(v1.ConditionTypeLaunched, "CapacityEvidencePending", "the cloud provider deferred the launch")
	}
	failed := func(nc *v1.NodeClaim) {
		nc.StatusConditions().SetUnknownWithReason(v1.ConditionTypeLaunched, "LaunchFailed", "creating instance, throttled")
	}
	checks := func(reason, outcome string) map[string]string {
		return map[string]string{"reason": reason, "outcome": outcome}
	}

	BeforeEach(func() {
		disruption.UnlaunchedNodeClaimSyncChecksTotal.Reset()
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
		emptyNodeClaim, emptyNode = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveSpotInstance.Name,
					v1.CapacityTypeLabelKey:        leastExpensiveSpotOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       leastExpensiveSpotOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		emptyNodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	})

	// expectHydrated syncs cluster state once with every NodeClaim launched, as a running controller
	// has long since done. No policy relaxes the first sync.
	expectHydrated := func(nodeClaims []*v1.NodeClaim, nodes []*corev1.Node) {
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		Expect(cluster.Synced(ctx)).To(BeTrue())
	}

	Context("an empty node and one unlaunched NodeClaim", func() {
		DescribeTable("deletes the empty node only when the policy skips the NodeClaim's class",
			func(mutate func(*v1.NodeClaim), deleting bool, class string, policy options.DisruptionSyncPolicy, proceeds bool) {
				withPolicy(policy)
				ExpectApplied(ctx, env.Client, nodePool, emptyNodeClaim, emptyNode)
				expectHydrated([]*v1.NodeClaim{emptyNodeClaim}, []*corev1.Node{emptyNode})

				unlaunched := applyUnlaunched(nodePool, mutate)
				if deleting {
					markDeleting(unlaunched)
				}
				Expect(cluster.Synced(ctx)).To(BeFalse())

				ExpectSingletonReconciled(ctx, disruptionController)
				if proceeds {
					cmds := queue.GetCommands()
					Expect(cmds).To(HaveLen(1))
					Expect(cmds[0].Candidates[0].NodeClaim.Name).To(Equal(emptyNodeClaim.Name))
					Expect(cmds[0].Replacements).To(BeEmpty())
					ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks(class, "proceeded"))
				} else {
					Expect(queue.GetCommands()).To(BeEmpty())
					ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks(class, "waited"))
				}
			},
			Entry("deferred launch, Strict", deferred, false, "launch_deferred", options.DisruptionSyncPolicyStrict, false),
			Entry("deferred launch, IgnoreFailedOrDeferred", deferred, false, "launch_deferred", options.DisruptionSyncPolicyIgnoreFailedOrDeferred, true),
			Entry("deferred launch, IgnoreNonReplacements", deferred, false, "launch_deferred", options.DisruptionSyncPolicyIgnoreNonReplacements, true),
			Entry("failed launch, Strict", failed, false, "launch_failed", options.DisruptionSyncPolicyStrict, false),
			Entry("failed launch, IgnoreFailedOrDeferred", failed, false, "launch_failed", options.DisruptionSyncPolicyIgnoreFailedOrDeferred, true),
			Entry("deleting before launch, Strict", nil, true, "deleting", options.DisruptionSyncPolicyStrict, false),
			Entry("deleting before launch, IgnoreFailedOrDeferred", nil, true, "deleting", options.DisruptionSyncPolicyIgnoreFailedOrDeferred, true),
			Entry("in flight, Strict", nil, false, "in_flight", options.DisruptionSyncPolicyStrict, false),
			Entry("in flight, IgnoreFailedOrDeferred", nil, false, "in_flight", options.DisruptionSyncPolicyIgnoreFailedOrDeferred, false),
			Entry("in flight, IgnoreNonReplacements", nil, false, "in_flight", options.DisruptionSyncPolicyIgnoreNonReplacements, true),
		)
		It("labels a check with the class that needs the loosest policy", func() {
			withPolicy(options.DisruptionSyncPolicyIgnoreFailedOrDeferred)
			ExpectApplied(ctx, env.Client, nodePool, emptyNodeClaim, emptyNode)
			expectHydrated([]*v1.NodeClaim{emptyNodeClaim}, []*corev1.Node{emptyNode})
			applyUnlaunched(nodePool, deferred)
			applyUnlaunched(nodePool, nil)

			ExpectSingletonReconciled(ctx, disruptionController)
			Expect(queue.GetCommands()).To(BeEmpty())
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("in_flight", "waited"))
		})
		It("never relaxes the first sync against the API server", func() {
			withPolicy(options.DisruptionSyncPolicyIgnoreNonReplacements)
			ExpectApplied(ctx, env.Client, nodePool, emptyNodeClaim, emptyNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{emptyNode}, []*v1.NodeClaim{emptyNodeClaim})
			applyUnlaunched(nodePool, deferred)
			Expect(cluster.HasSynced()).To(BeFalse())

			ExpectSingletonReconciled(ctx, disruptionController)
			Expect(queue.GetCommands()).To(BeEmpty())
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("not_hydrated", "waited"))
		})
	})

	Context("the replacement of an in-flight command", func() {
		var candidateClaim *v1.NodeClaim
		var candidateNode *corev1.Node
		var cmd *disruption.Command

		BeforeEach(func() {
			withPolicy(options.DisruptionSyncPolicyIgnoreNonReplacements)
			candidateClaim, candidateNode = test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}},
			})
			ExpectApplied(ctx, env.Client, nodePool, emptyNodeClaim, emptyNode, candidateClaim, candidateNode)
			expectHydrated([]*v1.NodeClaim{emptyNodeClaim, candidateClaim}, []*corev1.Node{emptyNode, candidateNode})

			nct := pscheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			cmd = &disruption.Command{
				Method:            disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Candidates:        []*disruption.Candidate{{StateNode: ExpectStateNodeExistsForNodeClaim(cluster, candidateClaim), NodePool: nodePool}},
				Replacements:      []*disruption.Replacement{{NodeClaim: &pscheduling.NodeClaim{NodeClaimTemplate: *nct}}},
			}
			Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
			// The provisioner records the replacement in cluster state as it creates it.
			Expect(cluster.Synced(ctx)).To(BeFalse())
			Expect(queue.ReplacementNames().Has(cmd.Replacements[0].Name)).To(BeTrue())
			// The queue annotates the replacement before creating it, so cluster state knows it is one
			// from its first record, before the command enters the queue.
			Expect(cluster.UnlaunchedNodeClaims()).To(ConsistOf(state.UnlaunchedNodeClaim{Name: cmd.Replacements[0].Name, Replacement: true}))
		})

		It("waits for it under every policy while it launches", func() {
			ExpectSingletonReconciled(ctx, disruptionController)
			Expect(queue.GetCommands()).To(HaveLen(1))
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("replacement", "waited"))
		})
		It("waits for it while it is deleting, until the queue fails its command", func() {
			replacement := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: cmd.Replacements[0].Name}}
			markDeleting(replacement)

			// The candidate stays marked for deletion until the queue sees the replacement fail. A pass
			// that skipped the replacement now would launch capacity for the candidate's pods again.
			ExpectSingletonReconciled(ctx, disruptionController)
			Expect(queue.GetCommands()).To(HaveLen(1))
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, candidateClaim).MarkedForDeletion()).To(BeTrue())
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("replacement", "waited"))

			// The queue times the command out and unmarks the candidate. The leftover still carries its
			// replacement annotation, so the pass keeps waiting until it is gone.
			env.Clock.Step(30 * time.Minute)
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKeyFromObject(candidateClaim))
			Expect(queue.GetCommands()).To(BeEmpty())
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, candidateClaim).MarkedForDeletion()).To(BeFalse())
			ExpectSingletonReconciled(ctx, disruptionController)
			Expect(queue.GetCommands()).To(BeEmpty())
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 2, checks("replacement", "waited"))

			ExpectFinalizersRemoved(ctx, env.Client, replacement)
			ExpectNotFound(ctx, env.Client, replacement)
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(replacement))
			Expect(cluster.UnlaunchedNodeClaims()).To(BeEmpty())

			ExpectSingletonReconciled(ctx, disruptionController)
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Candidates[0].NodeClaim.Name).To(Equal(emptyNodeClaim.Name))
		})
	})

	Context("a replacement the queue has not registered yet", func() {
		It("waits for it by its annotation", func() {
			// The queue creates a command's replacements, and cluster state records them, before the
			// command enters the queue.
			withPolicy(options.DisruptionSyncPolicyIgnoreNonReplacements)
			ExpectApplied(ctx, env.Client, nodePool, emptyNodeClaim, emptyNode)
			expectHydrated([]*v1.NodeClaim{emptyNodeClaim}, []*corev1.Node{emptyNode})
			replacement := applyUnlaunched(nodePool, func(nc *v1.NodeClaim) {
				nc.Annotations = map[string]string{v1.NodeClaimReplacementOriginAnnotationKey: "underutilized:on-demand"}
			})
			Expect(queue.ReplacementNames().Has(replacement.Name)).To(BeFalse())

			ExpectSingletonReconciled(ctx, disruptionController)
			Expect(queue.GetCommands()).To(BeEmpty())
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("replacement", "waited"))
		})
	})

	Context("pending pods bound for an in-flight provisioning NodeClaim", func() {
		var rs *appsv1.ReplicaSet
		var driftedClaim *v1.NodeClaim
		var driftedNode *corev1.Node
		var pod *corev1.Pod
		var backlogNodePool *v1.NodePool
		var backlogPods []*corev1.Pod

		BeforeEach(func() {
			rs = test.ReplicaSet()
			ExpectApplied(ctx, env.Client, rs)
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
			pod = test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID,
						Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
					}}},
				NodeSelector: map[string]string{v1.NodePoolLabelKey: nodePool.Name},
			})
			driftedClaim, driftedNode = test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})
			driftedClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			// The backlog can only land in a NodePool the drifted node is not in, where the provisioner
			// has already created a NodeClaim for it that has not launched.
			backlogNodePool = test.NodePool()
			backlogPods = test.UnschedulablePods(test.PodOptions{
				NodeSelector: map[string]string{v1.NodePoolLabelKey: backlogNodePool.Name},
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("1")},
				},
			}, 3)
			ExpectApplied(ctx, env.Client, pod, driftedClaim, driftedNode, nodePool, backlogNodePool)
			for _, p := range backlogPods {
				ExpectApplied(ctx, env.Client, p)
			}
			ExpectManualBinding(ctx, env.Client, pod, driftedNode)
			expectHydrated([]*v1.NodeClaim{driftedClaim}, []*corev1.Node{driftedNode})
		})

		It("waits for it under Strict and IgnoreFailedOrDeferred", func() {
			applyUnlaunched(backlogNodePool, nil)
			for _, policy := range []options.DisruptionSyncPolicy{options.DisruptionSyncPolicyStrict, options.DisruptionSyncPolicyIgnoreFailedOrDeferred} {
				withPolicy(policy)
				ExpectSingletonReconciled(ctx, disruptionController)
				Expect(queue.GetCommands()).To(BeEmpty())
			}
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 2, checks("in_flight", "waited"))
		})
		It("drifts the node under IgnoreNonReplacements without launching capacity for the backlog again", func() {
			withPolicy(options.DisruptionSyncPolicyIgnoreNonReplacements)
			inFlight := applyUnlaunched(backlogNodePool, nil)

			ExpectSingletonReconciled(ctx, disruptionController)
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("in_flight", "proceeded"))
			// The simulation saw the backlog as pending, as it sees any pod without launched capacity, and
			// opened claims for it. Those host no disrupted pod, so the command does not own them.
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Replacements).To(HaveLen(1))
			backlogClaims := lo.Filter(ExpectNodeClaims(ctx, env.Client), func(nc *v1.NodeClaim, _ int) bool {
				return nc.Labels[v1.NodePoolLabelKey] == backlogNodePool.Name
			})
			Expect(lo.Map(backlogClaims, func(nc *v1.NodeClaim, _ int) string { return nc.Name })).To(ConsistOf(inFlight.Name))
			for _, p := range backlogPods {
				ExpectNotScheduled(ctx, env.Client, p)
			}
		})
	})

	Context("an in-flight NodeClaim with capacity a consolidation candidate could use", func() {
		var candidateClaim *v1.NodeClaim
		var candidateNode *corev1.Node

		BeforeEach(func() {
			withPolicy(options.DisruptionSyncPolicyIgnoreNonReplacements)
			candidateClaim, candidateNode = test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})
			candidateClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
			pod := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("1")},
			}})
			ExpectApplied(ctx, env.Client, nodePool, candidateClaim, candidateNode, pod)
			ExpectManualBinding(ctx, env.Client, pod, candidateNode)
			expectHydrated([]*v1.NodeClaim{candidateClaim}, []*corev1.Node{candidateNode})
		})

		It("does not count on its capacity before it launches", func() {
			applyUnlaunched(nodePool, func(nc *v1.NodeClaim) {
				nc.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")}
			})
			ExpectSingletonReconciled(ctx, disruptionController)
			ExpectMetricCounterValue(disruption.UnlaunchedNodeClaimSyncChecksTotal, 1, checks("in_flight", "proceeded"))
			// The candidate's pod cannot move onto a NodeClaim that is not in cluster state, so the
			// pass replaces the candidate rather than deleting it.
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		})
		It("does not count on its capacity once it launches and has yet to initialize", func() {
			inFlight := applyUnlaunched(nodePool, nil)
			// The NodeClaim launches between passes, or between this pass's sync check and its
			// simulation: either way it becomes a node in cluster state that is not initialized.
			inFlight.Status.ProviderID = test.RandomProviderID()
			inFlight.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")}
			inFlight.Status.Capacity = inFlight.Status.Allocatable
			inFlight.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
			ExpectApplied(ctx, env.Client, inFlight)
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(inFlight))
			Expect(cluster.UnlaunchedNodeClaims()).To(BeEmpty())
			Expect(cluster.Synced(ctx)).To(BeTrue())

			ExpectSingletonReconciled(ctx, disruptionController)
			// Strictly synced: the check is not counted. The simulation would place the candidate's pod
			// on the uninitialized node, which disruption refuses to rely on.
			Expect(queue.GetCommands()).To(BeEmpty())
			_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_unlaunched_nodeclaim_sync_checks_total", map[string]string{})
			Expect(found).To(BeFalse())
		})
	})
})
