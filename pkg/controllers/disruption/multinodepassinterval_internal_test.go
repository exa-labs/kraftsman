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

package disruption

import (
	"context"
	"errors"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

func multiNodeIntervalContext(interval time.Duration) context.Context {
	return options.ToContext(context.Background(), &options.Options{MultiNodeConsolidationInterval: interval})
}

func newIntervalMultiNode(clk *clocktesting.FakeClock) *MultiNodeConsolidation {
	cloudProvider := fake.NewCloudProvider()
	return NewMultiNodeConsolidation(MakeConsolidation(clk, state.NewCluster(clk, nil, cloudProvider), nil, nil, cloudProvider, noopRecorder{}, nil))
}

func TestMultiNodePassIntervalDisabledByDefault(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	m := newIntervalMultiNode(clk)
	ctx := multiNodeIntervalContext(0)

	m.recordPassOutcome(ctx, PassOutcomeNoOp, nil)
	if !m.DueForPass(ctx) {
		t.Fatal("with no interval configured, every loop iteration must run a pass")
	}
}

func TestMultiNodePassIntervalSpacesPassesAfterNoCommand(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	m := newIntervalMultiNode(clk)
	ctx := multiNodeIntervalContext(10 * time.Minute)

	if !m.DueForPass(ctx) {
		t.Fatal("the first pass must be due")
	}
	for _, outcome := range []string{PassOutcomeNoOp, PassOutcomeTimedOut} {
		m.recordPassOutcome(ctx, outcome, nil)
		if m.DueForPass(ctx) {
			t.Fatalf("a pass ending %s must push the next one out by the interval", outcome)
		}
		clk.Step(10*time.Minute - time.Second)
		if m.DueForPass(ctx) {
			t.Fatalf("a pass ending %s must not be followed by another inside the interval", outcome)
		}
		clk.Step(time.Second)
		if !m.DueForPass(ctx) {
			t.Fatalf("the next pass must be due once the interval after a pass ending %s has elapsed", outcome)
		}
	}

	// A pass that found a command changed the fleet, so the next one is due at once.
	m.recordPassOutcome(ctx, PassOutcomeNoOp, nil)
	m.recordPassOutcome(ctx, PassOutcomeCompleted, nil)
	if !m.DueForPass(ctx) {
		t.Fatal("a pass that produced a command must leave the next pass due immediately")
	}

	// A pass that failed is retried by the controller; the interval must not swallow the retry.
	m.recordPassOutcome(ctx, PassOutcomeNoOp, nil)
	m.recordPassOutcome(ctx, PassOutcomeNoOp, errors.New("transient"))
	if !m.DueForPass(ctx) {
		t.Fatal("a pass that returned an error must leave the next pass due immediately")
	}

	// A pass with no candidates never reaches ComputeCommands; the controller reports it instead.
	m.RecordEmptyPass(ctx)
	if m.DueForPass(ctx) {
		t.Fatal("a pass with no candidates must push the next one out by the interval")
	}
}

func TestMultiNodeComputeCommandsSchedulesNextPass(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	m := newIntervalMultiNode(clk)
	ctx := multiNodeIntervalContext(10 * time.Minute)

	// No candidates: the pass finds nothing, which must defer the next one.
	cmds, err := m.ComputeCommands(ctx, map[string]int{})
	if err != nil {
		t.Fatalf("computing commands: %v", err)
	}
	if len(cmds) != 0 {
		t.Fatalf("expected no commands, got %d", len(cmds))
	}
	if m.DueForPass(ctx) {
		t.Fatal("a pass that found nothing must defer the next one by the interval")
	}
}

// gatedMethod is a Method whose pass gate is closed. Its ComputeCommands records the call so the
// test can assert the controller never reached it.
type gatedMethod struct {
	computed bool
}

func (g *gatedMethod) DueForPass(context.Context) bool                { return false }
func (g *gatedMethod) RecordEmptyPass(context.Context)                {}
func (g *gatedMethod) ShouldDisrupt(context.Context, *Candidate) bool { return true }
func (g *gatedMethod) ComputeCommands(context.Context, map[string]int, ...*Candidate) ([]Command, error) {
	g.computed = true
	return nil, nil
}
func (g *gatedMethod) Reason() v1.DisruptionReason { return v1.DisruptionReasonUnderutilized }
func (g *gatedMethod) Class() string               { return GracefulDisruptionClass }
func (g *gatedMethod) ConsolidationType() string   { return MultiNodeConsolidationType }

func TestControllerSkipsMethodThatIsNotDue(t *testing.T) {
	// The controller has no cluster: a gated method must be skipped before candidates are built,
	// which would otherwise dereference it.
	c := &Controller{clock: clocktesting.NewFakeClock(time.Now())}
	method := &gatedMethod{}
	acted, err := c.disrupt(context.Background(), method)
	if err != nil || acted {
		t.Fatalf("expected a skipped method to report no action and no error, got %v, %v", acted, err)
	}
	if method.computed {
		t.Fatal("a method that is not due must not compute commands")
	}
}

func TestControllerRecordsEmptyMultiNodePass(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	cloudProvider := fake.NewCloudProvider()
	m := newIntervalMultiNode(clk)
	c := &Controller{
		clock:         clk,
		kubeClient:    fakecr.NewFakeClient(),
		cluster:       state.NewCluster(clk, nil, cloudProvider),
		cloudProvider: cloudProvider,
		recorder:      noopRecorder{},
	}
	ctx := multiNodeIntervalContext(10 * time.Minute)

	// The cluster has no nodes, so the pass ends before ComputeCommands. It still found nothing,
	// and the next loop iteration must not rebuild candidates for it.
	acted, err := c.disrupt(ctx, m)
	if err != nil || acted {
		t.Fatalf("expected an empty pass to report no action and no error, got %v, %v", acted, err)
	}
	if m.DueForPass(ctx) {
		t.Fatal("a pass with no candidates must push the next one out by the interval")
	}
}
