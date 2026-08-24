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

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func TestNegativeResultCacheLifecycle(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	cache := NewNegativeResultCache(clk)
	ttl := 5 * time.Minute

	if cache.ShouldSkip("unit", "provider-a", "fp-1") {
		t.Fatal("an empty cache must not skip")
	}

	cache.StoreNegative("provider-a", "fp-1", ttl)
	if !cache.ShouldSkip("unit", "provider-a", "fp-1") {
		t.Fatal("a stored verdict with an unchanged fingerprint must skip")
	}

	// A changed fingerprint misses and evicts, so the stale entry cannot hit again.
	if cache.ShouldSkip("unit", "provider-a", "fp-2") {
		t.Fatal("a changed fingerprint must not skip")
	}
	if cache.ShouldSkip("unit", "provider-a", "fp-1") {
		t.Fatal("a changed fingerprint must evict the stored verdict")
	}

	cache.StoreNegative("provider-a", "fp-1", ttl)
	clk.Step(ttl + time.Second)
	if cache.ShouldSkip("unit", "provider-a", "fp-1") {
		t.Fatal("an expired verdict must not skip")
	}

	cache.StoreNegative("provider-a", "fp-1", ttl)
	cache.Clear()
	if cache.ShouldSkip("unit", "provider-a", "fp-1") {
		t.Fatal("a cleared cache must not skip")
	}

	cache.StoreNegative("provider-a", "fp-1", 0)
	if cache.ShouldSkip("unit", "provider-a", "fp-1") {
		t.Fatal("a non-positive TTL must not store a verdict")
	}
}

func TestNegativeResultCacheDropExpired(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	cache := NewNegativeResultCache(clk)

	cache.StoreNegative("provider-gone", "fp-1", time.Minute)
	clk.Step(2 * time.Minute)
	cache.StoreNegative("provider-live", "fp-2", time.Minute)

	cache.DropExpired()
	if len(cache.entries) != 1 {
		t.Fatalf("expected the expired entry to be swept, got %d entries", len(cache.entries))
	}
	if !cache.ShouldSkip("unit", "provider-live", "fp-2") {
		t.Fatal("a live verdict must survive the sweep")
	}
}

func TestNoOpDurability(t *testing.T) {
	ctx, durability := withNoOpDurability(context.Background())
	if !durability.Conclusive() {
		t.Fatal("an unmarked evaluation must be conclusive")
	}

	// The mark must be visible through derived contexts, since candidate evaluation runs under
	// its own timeout context.
	derived, cancel := context.WithCancel(ctx)
	defer cancel()
	markNoOpInconclusive(derived)
	if durability.Conclusive() {
		t.Fatal("a marked evaluation must not be conclusive")
	}

	// A mark on a context without a tracked evaluation must be a no-op.
	markNoOpInconclusive(context.Background())
}

// fakeRevisionProvider wraps the fake cloud provider with a fixed instance type revision.
type fakeRevisionProvider struct {
	*fake.CloudProvider
	revision uint64
	err      error
	calls    int
}

func (f *fakeRevisionProvider) InstanceTypeRevision(_ context.Context, _ *v1.NodePool) (uint64, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	return f.revision, nil
}

func fingerprintCandidate(nodeRV, claimRV string, poolGeneration int64, podUIDs ...string) *Candidate {
	pods := make([]*corev1.Pod, 0, len(podUIDs))
	for _, uid := range podUIDs {
		pods = append(pods, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), ResourceVersion: "rv-" + uid}})
	}
	return &Candidate{
		StateNode: &state.StateNode{
			Node:      &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", ResourceVersion: nodeRV}},
			NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-a", ResourceVersion: claimRV}},
		},
		NodePool:          &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool-a", UID: "pool-a-uid", Generation: poolGeneration, ResourceVersion: "pool-rv"}},
		reschedulablePods: pods,
	}
}

func TestNegativeCacheFingerprintCoversEveryInput(t *testing.T) {
	ctx := context.Background()
	provider := &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 7}
	base := fingerprintCandidate("n1", "c1", 1, "uid-b", "uid-a")

	baseFingerprint := newNegativeCacheFingerprints(fakecr.NewFakeClient(), provider).fingerprint(ctx, base)
	if baseFingerprint == "" {
		t.Fatal("a fully versioned candidate must fingerprint")
	}
	// Pod order must not matter: the same set is the same candidate.
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), provider).fingerprint(ctx, fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")); got != baseFingerprint {
		t.Fatal("pod order changed the fingerprint")
	}
	// A NodePool status patch bumps resourceVersion without changing the spec; that is the
	// ordinary churn of nodes joining and leaving and must not invalidate the fingerprint.
	statusPatched := fingerprintCandidate("n1", "c1", 1, "uid-b", "uid-a")
	statusPatched.NodePool.ResourceVersion = "pool-rv-bumped"
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), provider).fingerprint(ctx, statusPatched); got != baseFingerprint {
		t.Fatal("a NodePool resourceVersion bump without a spec change changed the fingerprint")
	}

	for name, changed := range map[string]*Candidate{
		"node resourceVersion":      fingerprintCandidate("n2", "c1", 1, "uid-a", "uid-b"),
		"nodeclaim resourceVersion": fingerprintCandidate("n1", "c2", 1, "uid-a", "uid-b"),
		"nodepool generation":       fingerprintCandidate("n1", "c1", 2, "uid-a", "uid-b"),
		"pod set":                   fingerprintCandidate("n1", "c1", 1, "uid-a"),
		// A NodePool deleted and recreated under the same name resets its generation and may reuse
		// an instance type revision, so only the UID distinguishes it from the pool the verdict
		// was computed against.
		"nodepool UID": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.NodePool.UID = "pool-a-recreated-uid"
			return c
		}(),
		"pod resourceVersion": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.reschedulablePods[0].ResourceVersion = "rv-updated"
			return c
		}(),
	} {
		if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), provider).fingerprint(ctx, changed); got == baseFingerprint {
			t.Fatalf("changing the %s did not change the fingerprint", name)
		}
	}
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 8}).fingerprint(ctx, base); got == baseFingerprint {
		t.Fatal("changing the instance type revision did not change the fingerprint")
	}

	// The simulation searches every ready NodePool for a replacement, so a change to any other
	// pool — not just the candidate's — must change the fingerprint.
	otherPool := managedNodePool(1, true)
	withOther := newNegativeCacheFingerprints(fakecr.NewFakeClient(otherPool), provider).fingerprint(ctx, base)
	if withOther == baseFingerprint {
		t.Fatal("adding another ready NodePool did not change the fingerprint")
	}
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(managedNodePool(2, true)), provider).fingerprint(ctx, base); got == withOther {
		t.Fatal("editing another ready NodePool did not change the fingerprint")
	}
	// A fleet pool recreated under the same name with the same generation and revision must still
	// change the fingerprint: its UID is the only thing that distinguishes the new object.
	recreated := managedNodePool(1, true)
	recreated.UID = "pool-other-recreated-uid"
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(recreated), provider).fingerprint(ctx, base); got == withOther {
		t.Fatal("recreating a fleet NodePool with a new UID did not change the fingerprint")
	}
	// Readiness gates membership in the fleet component, so a pool flipping unready must change
	// the fingerprint: the simulation the verdict came from could have used that pool.
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(managedNodePool(1, false)), provider).fingerprint(ctx, base); got != baseFingerprint {
		t.Fatal("an unready NodePool entered the fleet component")
	}
}

func managedNodePool(generation int64, ready bool) *v1.NodePool {
	const name = "pool-other"
	nodePool := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), Generation: generation},
		Spec: v1.NodePoolSpec{
			Template: v1.NodeClaimTemplate{
				Spec: v1.NodeClaimTemplateSpec{
					NodeClassRef: &v1.NodeClassReference{Group: "karpenter.test.sh", Kind: "TestNodeClass", Name: "default"},
				},
			},
		},
	}
	if ready {
		nodePool.StatusConditions().SetTrue(status.ConditionReady)
	} else {
		nodePool.StatusConditions().SetFalse(status.ConditionReady, "NotReady", "test")
	}
	return nodePool
}

func TestNegativeCacheFingerprintFailsClosed(t *testing.T) {
	ctx := context.Background()
	base := fingerprintCandidate("n1", "c1", 1, "uid-a")

	// A provider that cannot version its offerings makes the candidate unfingerprintable.
	// The fake provider implements the interface with a 0 (unstable) revision by default,
	// which must fail closed the same way as not implementing it at all.
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), fake.NewCloudProvider()).fingerprint(ctx, base); got != "" {
		t.Fatal("a provider without instance type revisions must not fingerprint")
	}
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), err: errors.New("unavailable")}).fingerprint(ctx, base); got != "" {
		t.Fatal("a failing revision lookup must not fingerprint")
	}
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 0}).fingerprint(ctx, base); got != "" {
		t.Fatal("a zero revision must not fingerprint")
	}

	incomplete := fingerprintCandidate("n1", "c1", 1, "uid-a")
	incomplete.NodePool = nil
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 7}).fingerprint(ctx, incomplete); got != "" {
		t.Fatal("a candidate without a NodePool must not fingerprint")
	}

	unversionedPod := fingerprintCandidate("n1", "c1", 1, "uid-a")
	unversionedPod.reschedulablePods[0].ResourceVersion = ""
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 7}).fingerprint(ctx, unversionedPod); got != "" {
		t.Fatal("a candidate with an unversioned pod must not fingerprint")
	}
}

func TestNegativeCacheFingerprintMemoizesRevisionPerPool(t *testing.T) {
	ctx := context.Background()
	provider := &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 7}
	fingerprints := newNegativeCacheFingerprints(fakecr.NewFakeClient(), provider)

	fingerprints.fingerprint(ctx, fingerprintCandidate("n1", "c1", 1, "uid-a"))
	fingerprints.fingerprint(ctx, fingerprintCandidate("n2", "c2", 1, "uid-b"))
	if provider.calls != 1 {
		t.Fatalf("expected one revision lookup per NodePool per pass, got %d", provider.calls)
	}
}
