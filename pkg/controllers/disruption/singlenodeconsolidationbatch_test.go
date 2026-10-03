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
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// scriptedValidator answers Validate from a fixed script, one entry per call, so a test can
// place a rejection at an exact position in a batch. Calls past the script are accepted. Like the
// real validator, it spends the validation period it is given, on the suite's fake clock.
type scriptedValidator struct {
	errs  []error
	calls int
	// periods records the validation period each call was given, which is how the tests assert
	// that only the first admission of a pass pays the settling wait.
	periods []time.Duration
}

func (v *scriptedValidator) Validate(_ context.Context, cmd disruption.Command, period time.Duration) (disruption.Command, error) {
	v.periods = append(v.periods, period)
	env.Clock.Step(period)
	i := v.calls
	v.calls++
	if i < len(v.errs) && v.errs[i] != nil {
		return disruption.Command{}, v.errs[i]
	}
	return cmd, nil
}

// blockingValidator queues a rival command for a proposal's own candidate just as that proposal
// is validated, which is how a losing race with the queue is produced deterministically.
type blockingValidator struct {
	method    disruption.Method
	blockCall int
	calls     int
}

func (v *blockingValidator) Validate(ctx context.Context, cmd disruption.Command, _ time.Duration) (disruption.Command, error) {
	defer func() { v.calls++ }()
	if v.calls == v.blockCall {
		rival := disruption.Command{Method: v.method, Candidates: cmd.Candidates}
		if err := queue.StartCommand(ctx, &rival); err != nil {
			return disruption.Command{}, err
		}
	}
	return cmd, nil
}

// slowValidator burns a fixed amount of the pass's admission budget per call by advancing the
// suite's fake clock, which is how a test reaches the reserve gate without any real waiting.
type slowValidator struct {
	cost  time.Duration
	calls int
}

func (v *slowValidator) Validate(_ context.Context, cmd disruption.Command, _ time.Duration) (disruption.Command, error) {
	v.calls++
	env.Clock.Step(v.cost)
	return cmd, nil
}

// steppingClock advances every time it is read, which walks a pass past its own timeout with
// proposals in hand without the test waiting for anything.
type steppingClock struct {
	*clocktesting.FakeClock
	step time.Duration
}

func (c *steppingClock) Now() time.Time {
	c.Step(c.step)
	return c.FakeClock.Now()
}

// rejectingSlowValidator is slowValidator that also rejects, so a test can show that a rejected
// attempt spends the budget the same way an admitted one does.
type rejectingSlowValidator struct {
	slowValidator
}

func (v *rejectingSlowValidator) Validate(ctx context.Context, cmd disruption.Command, period time.Duration) (disruption.Command, error) {
	_, _ = v.slowValidator.Validate(ctx, cmd, period)
	return disruption.Command{}, disruption.NewSchedulingValidationError(errors.New("stale plan"))
}

// immediateValidator runs the real consolidation validation without its settling wait, which the
// suite's fake clock never advances through.
type immediateValidator struct {
	inner disruption.Validator
}

func (v immediateValidator) Validate(ctx context.Context, cmd disruption.Command, _ time.Duration) (disruption.Command, error) {
	return v.inner.Validate(ctx, cmd, 0)
}

var _ = Describe("Batched Single-Node Consolidation", func() {
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var rs *appsv1.ReplicaSet
	var validator *scriptedValidator
	var singleNode *disruption.SingleNodeConsolidation
	labels := map[string]string{"app": "batched-consolidation"}

	// newSingleNodeConsolidation builds a single-node method backed by the given validator.
	newSingleNodeConsolidation := func(v disruption.Validator) *disruption.SingleNodeConsolidation {
		return disruption.NewSingleNodeConsolidation(
			disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(v),
		)
	}

	// podOn returns a ReplicaSet-owned pod requesting cpu, so its node is a real consolidation
	// candidate rather than an empty node.
	podOn := func(cpu string) *corev1.Pod {
		return test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         "apps/v1",
					Kind:               "ReplicaSet",
					Name:               rs.Name,
					UID:                rs.UID,
					Controller:         lo.ToPtr(true),
					BlockOwnerDeletion: lo.ToPtr(true),
				}},
			},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			},
		})
	}

	// applyNodes puts one pod of the given size on each node and syncs cluster state.
	applyNodes := func(count int, cpu string) {
		for i := range count {
			pod := podOn(cpu)
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i], pod)
			ExpectManualBinding(ctx, env.Client, pod, nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes[:count], nodeClaims[:count])
	}

	// admissionFailures reads the running total of proposals rejected at the given admission
	// stage, which the tests compare as a delta since the registry is process-wide.
	admissionFailures := func(stage, reason string) float64 {
		GinkgoHelper()
		metric, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_consolidation_admission_failures_total", map[string]string{
			"consolidation_type": disruption.SingleNodeConsolidationType,
			"stage":              stage,
			"reason":             reason,
		})
		if !found {
			return 0
		}
		return metric.GetCounter().GetValue()
	}

	// batchedPasses reads how many passes reported a batch size, and passOutcomes the running total
	// of passes that ended with the given outcome.
	batchedPasses := func() uint64 {
		GinkgoHelper()
		metric, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_consolidation_commands_admitted_per_pass", map[string]string{
			"consolidation_type": disruption.SingleNodeConsolidationType,
		})
		if !found {
			return 0
		}
		return metric.GetHistogram().GetSampleCount()
	}

	passOutcomes := func(outcome string) float64 {
		GinkgoHelper()
		metric, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_consolidation_pass_outcomes_total", map[string]string{
			"consolidation_type": disruption.SingleNodeConsolidationType,
			"outcome":            outcome,
		})
		if !found {
			return 0
		}
		return metric.GetCounter().GetValue()
	}

	candidatesFor := func(m *disruption.SingleNodeConsolidation) []*disruption.Candidate {
		GinkgoHelper()
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, m.ShouldDisrupt, m.Class(), queue)
		Expect(err).To(Succeed())
		return candidates
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{MaxConsolidationCommandsPerPass: lo.ToPtr(3)}))
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})
		nodeClaims, nodes = test.NodeClaimsAndNodes(5, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            leastExpensiveInstance.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for _, nc := range nodeClaims {
			nc.Labels[v1.NodePoolLabelKey] = nodePool.Name
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		for _, n := range nodes {
			n.Labels[v1.NodePoolLabelKey] = nodePool.Name
		}
		rs = test.ReplicaSet()
		ExpectApplied(ctx, env.Client, nodePool, rs)

		validator = &scriptedValidator{}
		singleNode = newSingleNodeConsolidation(validator)
	})

	AfterEach(func() {
		disruption.SingleNodeConsolidationTimeoutDuration = 3 * time.Minute
		ExpectCleanedUp(ctx, env.Client)
	})

	It("admits several non-overlapping commands from one pass", func() {
		applyNodes(4, "1")
		candidates := candidatesFor(singleNode)
		Expect(candidates).To(HaveLen(4))

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(3))

		// The pass queued its own commands, so the controller must not start them again.
		Expect(queue.GetCommands()).To(HaveLen(3))
		claimed := sets.New[string]()
		for _, cmd := range cmds {
			Expect(cmd.Admitted).To(BeTrue())
			Expect(cmd.ID).ToNot(BeZero())
			for _, c := range cmd.Candidates {
				Expect(claimed.Has(c.ProviderID())).To(BeFalse())
				claimed.Insert(c.ProviderID())
			}
		}
		// Only the first admission waits out the settling period; the rest inherit it.
		Expect(validator.periods).To(Equal([]time.Duration{15 * time.Second, 0, 0}))
	})

	It("counts walk time after a proposal was found toward its settling window", func() {
		applyNodes(4, "1")
		candidates := candidatesFor(singleNode)
		cmds := lo.Map(candidates[:3], func(c *disruption.Candidate, _ int) disruption.Command {
			return disruption.Command{Candidates: []*disruption.Candidate{c}}
		})
		now := env.Clock.Now()
		// Found 20s, 10s and 0s before the walk ended. The first is past its window and validates
		// at once; the second waits out the 5s it has left; the third has 10s left once the second's
		// wait has run.
		admitted, err := singleNode.AdmitHeldProposals(ctx, cmds, []time.Time{now.Add(-20 * time.Second), now.Add(-10 * time.Second), now})
		Expect(err).To(Succeed())
		Expect(admitted).To(HaveLen(3))
		Expect(validator.periods).To(Equal([]time.Duration{0, 5 * time.Second, 10 * time.Second}))
		// No proposal is validated sooner than the settling window after it was found.
		Expect(env.Clock.Now()).To(Equal(now.Add(15 * time.Second)))
	})

	It("does not wait when every proposal was found a full settling window before the walk ended", func() {
		applyNodes(4, "1")
		candidates := candidatesFor(singleNode)
		cmds := lo.Map(candidates[:2], func(c *disruption.Candidate, _ int) disruption.Command {
			return disruption.Command{Candidates: []*disruption.Candidate{c}}
		})
		now := env.Clock.Now()
		admitted, err := singleNode.AdmitHeldProposals(ctx, cmds, []time.Time{now.Add(-time.Minute), now.Add(-15 * time.Second)})
		Expect(err).To(Succeed())
		Expect(admitted).To(HaveLen(2))
		Expect(validator.periods).To(Equal([]time.Duration{0, 0}))
	})

	It("keeps the one-command-per-pass behavior when the cap is 1", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{MaxConsolidationCommandsPerPass: lo.ToPtr(1)}))
		applyNodes(4, "1")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		// The unbatched path leaves starting the command to the controller.
		Expect(cmds[0].Admitted).To(BeFalse())
		Expect(queue.GetCommands()).To(BeEmpty())
	})

	It("does not admit more commands than the NodePool's remaining budget", func() {
		applyNodes(4, "1")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 2}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(2))
		Expect(queue.GetCommands()).To(HaveLen(2))
	})

	It("admits the remaining commands when one proposal is rejected at admission", func() {
		applyNodes(4, "1")
		validator.errs = []error{disruption.NewSchedulingValidationError(errors.New("stale plan"))}
		rejected := admissionFailures(disruption.AdmissionStageValidation, "scheduling")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		// The rejected proposal no longer abandons the whole pass.
		Expect(cmds).To(HaveLen(2))
		Expect(queue.GetCommands()).To(HaveLen(2))
		Expect(admissionFailures(disruption.AdmissionStageValidation, "scheduling")).To(Equal(rejected + 1))
	})

	It("returns no commands when every proposal is rejected at admission", func() {
		applyNodes(4, "1")
		validator.errs = []error{
			disruption.NewSchedulingValidationError(errors.New("stale plan")),
			disruption.NewBudgetValidationError(errors.New("budget")),
			disruption.NewChurnValidationError(errors.New("churn")),
		}

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(BeEmpty())
		Expect(queue.GetCommands()).To(BeEmpty())
		// Candidates are still actionable, so the pass must not claim the fleet is consolidated.
		Expect(singleNode.IsConsolidated()).To(BeFalse())
	})

	It("surfaces a start failure without losing the commands already admitted", func() {
		applyNodes(4, "1")
		blocker := &blockingValidator{blockCall: 1}
		method := newSingleNodeConsolidation(blocker)
		blocker.method = method
		completed := passOutcomes("completed")
		noop := passOutcomes("no_op")

		cmds, err := method.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(method)...)
		Expect(err).To(HaveOccurred())
		// The command admitted before the failure keeps running, next to the rival command.
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Admitted).To(BeTrue())
		Expect(queue.GetCommands()).To(HaveLen(2))
		// A pass that queued a command acted, however the rest of admission went.
		Expect(passOutcomes("completed")).To(Equal(completed + 1))
		Expect(passOutcomes("no_op")).To(Equal(noop))
	})

	It("does not report a batch size for a pass that held one proposal", func() {
		applyNodes(4, "1")
		batched := batchedPasses()

		// A budget of one leaves the pass holding a single proposal, which is what the unbatched
		// controller would have done.
		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 1}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		// One proposal is what the unbatched controller does, so it must not count as a batch,
		// or the histogram's sample rate stops meaning "passes that batched".
		Expect(batchedPasses()).To(Equal(batched))
	})

	It("reports a batch size for a pass that held several proposals", func() {
		applyNodes(4, "1")
		batched := batchedPasses()

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(3))
		Expect(batchedPasses()).To(Equal(batched + 1))
	})

	It("stops admitting once validations outrun the admission budget", func() {
		applyNodes(4, "1")
		// Three held proposals budget 15s + 3x20s; a validation costing 30s leaves the third
		// proposal inside the reserve, where admission stops.
		slow := &slowValidator{cost: 30 * time.Second}
		method := newSingleNodeConsolidation(slow)
		skipped := admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")

		cmds, err := method.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(method)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(2))
		Expect(queue.GetCommands()).To(HaveLen(2))
		Expect(slow.calls).To(Equal(2))
		Expect(admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")).To(Equal(skipped + 1))
	})

	It("spends its admission budget on attempts, not on admissions", func() {
		applyNodes(4, "1")
		slow := &rejectingSlowValidator{slowValidator: slowValidator{cost: 30 * time.Second}}
		method := newSingleNodeConsolidation(slow)
		skipped := admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")

		cmds, err := method.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(method)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(BeEmpty())
		// A rejected attempt costs a re-simulation too, so it spends the budget rather than
		// letting every remaining proposal validate for free.
		Expect(slow.calls).To(Equal(2))
		Expect(admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")).To(Equal(skipped + 1))
	})

	It("admits every proposal it holds when the walk times out", func() {
		// The cap is above the candidate count, so the walk ends on the timeout rather than on a
		// full batch.
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{MaxConsolidationCommandsPerPass: lo.ToPtr(5)}))
		applyNodes(5, "1")
		// A clock that advances as the walk reads it runs the pass past its timeout with proposals
		// in hand; the walk breaks out, and admission's own budget covers all of them.
		disruption.SingleNodeConsolidationTimeoutDuration = 5 * time.Second
		method := disruption.NewSingleNodeConsolidation(
			disruption.MakeConsolidation(&steppingClock{FakeClock: env.Clock, step: time.Second}, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(validator),
		)
		skipped := admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")
		timedOut := passOutcomes("timed_out")

		cmds, err := method.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(method)...)
		Expect(err).To(Succeed())
		Expect(passOutcomes("timed_out")).To(Equal(timedOut + 1))
		// Every proposal the walk was holding is admitted, not just the first.
		Expect(len(cmds)).To(BeNumerically(">", 1))
		Expect(queue.GetCommands()).To(HaveLen(len(cmds)))
		// The elapsed pass timeout is not what gates admission, so nothing is skipped for it.
		Expect(admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")).To(Equal(skipped))
	})

	It("rejects a proposal whose headroom an earlier command in the same pass consumed", func() {
		// Two loaded nodes and one node with room for exactly one of their pods. Consolidating
		// either is valid on its own; consolidating both is not, and the second proposal is
		// validated only after the first command has marked its candidate for deletion.
		applyNodes(2, "20")
		spareClaim, spareNode := nodeClaims[2], nodes[2]
		spareClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		ExpectApplied(ctx, env.Client, spareClaim, spareNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{spareNode}, []*v1.NodeClaim{spareClaim})

		real := newSingleNodeConsolidation(immediateValidator{
			inner: disruption.NewSingleConsolidationValidator(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)),
		})
		candidates := lo.Filter(candidatesFor(real), func(c *disruption.Candidate, _ int) bool {
			return c.ProviderID() != spareClaim.Status.ProviderID
		})
		Expect(candidates).To(HaveLen(2))

		rejected := admissionFailures(disruption.AdmissionStageValidation, "scheduling")
		cmds, err := real.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		Expect(queue.GetCommands()).To(HaveLen(1))
		// The second proposal was held, then rejected against the state the first command created.
		Expect(admissionFailures(disruption.AdmissionStageValidation, "scheduling")).To(Equal(rejected + 1))
	})

	// replaceThenDelete builds a pass that holds a replace followed by a delete. The first candidate
	// is an expensive node whose pod fits nowhere but a replacement; the second is a small node whose
	// pod fits on a spare node. Between the two admissions the replace's NodeClaim exists but has not
	// launched, so cluster state models it with no allocatable capacity, and a delete validated in
	// that window would have to open a new NodeClaim for the replaced node's pod.
	replaceThenDelete := func() (*disruption.SingleNodeConsolidation, []*disruption.Candidate) {
		GinkgoHelper()
		expensiveClaim, expensiveNode := nodeClaims[0], nodes[0]
		for k, v := range map[string]string{
			corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
			v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
			corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
		} {
			expensiveClaim.Labels[k] = v
			expensiveNode.Labels[k] = v
		}
		smallClaim, smallNode := nodeClaims[1], nodes[1]
		smallClaim.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourcePods: resource.MustParse("100")}
		smallNode.Status.Allocatable = smallClaim.Status.Allocatable
		spareClaim, spareNode := nodeClaims[2], nodes[2]
		for i, cpu := range []string{"3", "1.5", "30"} {
			pod := podOn(cpu)
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i], pod)
			ExpectManualBinding(ctx, env.Client, pod, nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController,
			[]*corev1.Node{expensiveNode, smallNode, spareNode}, []*v1.NodeClaim{expensiveClaim, smallClaim, spareClaim})

		real := newSingleNodeConsolidation(immediateValidator{
			inner: disruption.NewSingleConsolidationValidator(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)),
		})
		candidates := lo.Filter(candidatesFor(real), func(c *disruption.Candidate, _ int) bool {
			return c.ProviderID() != spareClaim.Status.ProviderID
		})
		Expect(candidates).To(HaveLen(2))
		return real, candidates
	}

	// whileWaiting stands in for the NodeClaim lifecycle controller and the passage of time: whenever
	// the pass is blocked on the suite's fake clock it optionally launches every NodeClaim that has
	// not launched yet, then advances the clock. The returned function stops it.
	whileWaiting := func(launch bool) (stop func()) {
		done, stopped := make(chan struct{}), make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(stopped)
			for {
				select {
				case <-done:
					return
				case <-time.After(10 * time.Millisecond):
				}
				if !env.Clock.HasWaiters() {
					continue
				}
				for _, nc := range ExpectNodeClaims(ctx, env.Client) {
					if !launch || nc.Status.ProviderID != "" {
						continue
					}
					deployed, err := ExpectNodeClaimDeployedNoNode(ctx, env.Client, cloudProvider, nc)
					Expect(err).To(Succeed())
					cluster.UpdateNodeClaim(deployed)
				}
				env.Clock.Step(time.Second)
			}
		}()
		return func() { close(done); <-stopped }
	}

	It("validates a proposal held behind a replace once the replacement has launched", func() {
		real, candidates := replaceThenDelete()
		stop := whileWaiting(true)
		rejected := admissionFailures(disruption.AdmissionStageValidation, "scheduling")

		cmds, err := real.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		stop()
		Expect(err).To(Succeed())
		// The replace sorts first, so the delete is the proposal validated behind it.
		Expect(cmds).To(HaveLen(2))
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		Expect(cmds[1].Decision()).To(Equal(disruption.DeleteDecision))
		Expect(queue.GetCommands()).To(HaveLen(2))
		Expect(admissionFailures(disruption.AdmissionStageValidation, "scheduling")).To(Equal(rejected))
	})

	It("stops admitting when an earlier command's replacement does not launch within the admission budget", func() {
		real, candidates := replaceThenDelete()
		stop := whileWaiting(false)
		unlaunched := admissionFailures(disruption.AdmissionStageDeadline, "replacements_not_launched")
		rejected := admissionFailures(disruption.AdmissionStageValidation, "scheduling")

		cmds, err := real.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		stop()
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		// The delete is left for a later pass rather than judged against a NodeClaim with no capacity.
		Expect(admissionFailures(disruption.AdmissionStageDeadline, "replacements_not_launched")).To(Equal(unlaunched + 1))
		Expect(admissionFailures(disruption.AdmissionStageValidation, "scheduling")).To(Equal(rejected))
	})

	It("does not wait on NodeClaims the pass did not create", func() {
		applyNodes(4, "1")
		// A NodeClaim some other controller created and the cloud provider has not launched yet.
		unrelated := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
		unrelated.Status.ProviderID = ""
		cluster.UpdateNodeClaim(unrelated)
		Expect(cluster.Synced(ctx)).To(BeFalse())
		stop := whileWaiting(false)
		unlaunched := admissionFailures(disruption.AdmissionStageDeadline, "replacements_not_launched")

		// Every proposal is a delete, so the commands admitted ahead of each one launched nothing it
		// would be judged against.
		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		stop()
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(3))
		Expect(admissionFailures(disruption.AdmissionStageDeadline, "replacements_not_launched")).To(Equal(unlaunched))
		cluster.DeleteNodeClaim(unrelated.Name)
	})

	It("starts every command exactly once when the controller runs the pass", func() {
		applyNodes(4, "1")
		controller := disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(newSingleNodeConsolidation(&scriptedValidator{})))

		ExpectSingletonReconciled(ctx, controller)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(3))
		providerIDs := sets.New[string]()
		for _, cmd := range cmds {
			for _, c := range cmd.Candidates {
				Expect(providerIDs.Has(c.ProviderID())).To(BeFalse(), fmt.Sprintf("%s was claimed twice", c.ProviderID()))
				providerIDs.Insert(c.ProviderID())
			}
		}
	})
})
