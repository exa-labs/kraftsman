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
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
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

// fakeRevisionProvider wraps the fake cloud provider with a fixed instance type revision and
// counts revision and instance type lookups.
type fakeRevisionProvider struct {
	*fake.CloudProvider
	revision uint64
	err      error
	calls    int
	getCalls int
}

func (f *fakeRevisionProvider) GetInstanceTypes(ctx context.Context, nodePool *v1.NodePool) ([]*cloudprovider.InstanceType, error) {
	f.getCalls++
	return f.CloudProvider.GetInstanceTypes(ctx, nodePool)
}

func (f *fakeRevisionProvider) InstanceTypeRevision(_ context.Context, _ *v1.NodePool) (uint64, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	return f.revision, nil
}

// fingerprintCandidate builds a candidate whose Node and NodeClaim carry the given values as a
// label, so tests can change their content; both objects also carry a resourceVersion, which the
// fingerprint must ignore.
func fingerprintCandidate(nodeContent, claimContent string, poolGeneration int64, podUIDs ...string) *Candidate {
	pods := make([]*corev1.Pod, 0, len(podUIDs))
	for _, uid := range podUIDs {
		pods = append(pods, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), ResourceVersion: "rv-" + uid}})
	}
	return &Candidate{
		StateNode: &state.StateNode{
			Node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", ResourceVersion: "node-rv",
				Labels: map[string]string{"fingerprint-test": nodeContent}}},
			NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-a", ResourceVersion: "claim-rv",
				Labels: map[string]string{"fingerprint-test": claimContent}}},
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
		"node labels":         fingerprintCandidate("n2", "c1", 1, "uid-a", "uid-b"),
		"nodeclaim labels":    fingerprintCandidate("n1", "c2", 1, "uid-a", "uid-b"),
		"nodepool generation": fingerprintCandidate("n1", "c1", 2, "uid-a", "uid-b"),
		"pod set":             fingerprintCandidate("n1", "c1", 1, "uid-a"),
		"node taints": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.Node.Spec.Taints = []corev1.Taint{{Key: "example.com/taint", Effect: corev1.TaintEffectNoSchedule}}
			return c
		}(),
		"node unschedulable": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.Node.Spec.Unschedulable = true
			return c
		}(),
		"node allocatable": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.Node.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}
			return c
		}(),
		"node annotations": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.Node.Annotations = map[string]string{"example.com/annotation": "set"}
			return c
		}(),
		"node condition status": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.Node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
			return c
		}(),
		"nodeclaim taints": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.NodeClaim.Spec.Taints = []corev1.Taint{{Key: "example.com/taint", Effect: corev1.TaintEffectNoSchedule}}
			return c
		}(),
		"nodeclaim allocatable": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.NodeClaim.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}
			return c
		}(),
		"nodeclaim condition status": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.NodeClaim.StatusConditions().SetFalse(v1.ConditionTypeConsolidatable, "NotConsolidatable", "test")
			return c
		}(),
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
		// The spot-to-spot stability annotations change the verdict without bumping generation.
		"spot-to-spot min node age annotation": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.NodePool.Annotations = map[string]string{v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey: "30m"}
			return c
		}(),
		"spot-to-spot min savings annotation": func() *Candidate {
			c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
			c.NodePool.Annotations = map[string]string{v1.NodePoolSpotToSpotMinSavingsAnnotationKey: "0.05"}
			return c
		}(),
	} {
		if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), provider).fingerprint(ctx, changed); got == baseFingerprint {
			t.Fatalf("changing the %s did not change the fingerprint", name)
		}
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

// TestNegativeCacheFingerprintIgnoresHeartbeats pins that the writes a healthy node receives
// continuously - kubelet status heartbeats, pod-event timestamps on its NodeClaim, condition
// transition times - leave the fingerprint alone. They move the objects' resourceVersions every
// few minutes, which would otherwise expire most verdicts between two passes.
func TestNegativeCacheFingerprintIgnoresHeartbeats(t *testing.T) {
	ctx := context.Background()
	provider := &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 7}
	withConditions := func(heartbeat time.Time) *Candidate {
		c := fingerprintCandidate("n1", "c1", 1, "uid-a", "uid-b")
		c.Node.Status.Conditions = []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastHeartbeatTime: metav1.NewTime(heartbeat)},
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: metav1.NewTime(heartbeat)},
		}
		c.NodeClaim.Status.LastPodEventTime = metav1.NewTime(heartbeat)
		c.NodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		return c
	}
	before := withConditions(time.Unix(1000, 0))
	after := withConditions(time.Unix(2000, 0))
	after.Node.ResourceVersion = "node-rv-heartbeat"
	after.NodeClaim.ResourceVersion = "claim-rv-heartbeat"
	// Condition order is not meaningful either.
	after.Node.Status.Conditions[0], after.Node.Status.Conditions[1] = after.Node.Status.Conditions[1], after.Node.Status.Conditions[0]

	beforeFingerprint := newNegativeCacheFingerprints(fakecr.NewFakeClient(), provider).fingerprint(ctx, before)
	if beforeFingerprint == "" {
		t.Fatal("a fully versioned candidate must fingerprint")
	}
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), provider).fingerprint(ctx, after); got != beforeFingerprint {
		t.Fatal("a heartbeat-only update changed the fingerprint")
	}
}

// TestNegativeCacheFingerprintFollowsInstanceTypeContent pins that the instance type component
// follows content, not the provider's revision.
func TestNegativeCacheFingerprintFollowsInstanceTypeContent(t *testing.T) {
	ctx := context.Background()
	base := fingerprintCandidate("n1", "c1", 1, "uid-a")
	baseFingerprint := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 7}).fingerprint(ctx, base)
	if baseFingerprint == "" {
		t.Fatal("a fully versioned candidate must fingerprint")
	}
	// A provider bumps its revision whenever anything it returns can differ, offerings included;
	// a bump over unchanged instance types must not invalidate verdicts.
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: fake.NewCloudProvider(), revision: 8}).fingerprint(ctx, base); got != baseFingerprint {
		t.Fatal("a revision bump over unchanged instance types changed the fingerprint")
	}
	changedTypes := fake.NewCloudProvider()
	changedTypes.InstanceTypes = []*cloudprovider.InstanceType{fake.NewInstanceType("only-instance-type")}
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: changedTypes, revision: 7}).fingerprint(ctx, base); got == baseFingerprint {
		t.Fatal("changing the NodePool's instance types did not change the fingerprint")
	}
}

// TestInstanceTypesContentHash pins what the instance type component of a fingerprint covers:
// names, capacity, and requirements, but not offerings or the requirement keys offerings carry,
// whose changes are price and availability moves the entry TTL bounds.
func TestInstanceTypesContentHash(t *testing.T) {
	offering := func(zone, capacityType string, price float64, available bool) cloudprovider.Offering {
		return cloudprovider.Offering{
			Available: available,
			Price:     price,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				corev1.LabelTopologyZone: zone,
				v1.CapacityTypeLabelKey:  capacityType,
			}),
		}
	}
	instanceTypes := func(price float64, available bool, zones []string, arch string, cpu string) []*cloudprovider.InstanceType {
		offerings := lo.Map(zones, func(zone string, _ int) cloudprovider.Offering { return offering(zone, "spot", price, available) })
		return []*cloudprovider.InstanceType{
			fake.NewInstanceType("type-b"),
			fake.NewInstanceType("type-a",
				fake.WithOfferings(offerings...),
				fake.WithArchitecture(arch),
				fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}),
			),
		}
	}
	base := mustContentHash(t, instanceTypes(1.0, true, []string{"zone-1", "zone-2"}, "amd64", "4"))

	for name, unchanged := range map[string][]*cloudprovider.InstanceType{
		"an offering price move":               instanceTypes(0.5, true, []string{"zone-1", "zone-2"}, "amd64", "4"),
		"an offering becoming unavailable":     instanceTypes(1.0, false, []string{"zone-1", "zone-2"}, "amd64", "4"),
		"a zone's offering disappearing":       instanceTypes(1.0, true, []string{"zone-1"}, "amd64", "4"),
		"the list arriving in another order":   reversed(instanceTypes(1.0, true, []string{"zone-1", "zone-2"}, "amd64", "4")),
		"the same content built a second time": instanceTypes(1.0, true, []string{"zone-1", "zone-2"}, "amd64", "4"),
	} {
		if got := mustContentHash(t, unchanged); got != base {
			t.Fatalf("%s changed the instance type content hash", name)
		}
	}
	for name, changed := range map[string][]*cloudprovider.InstanceType{
		"a requirement offerings do not carry": instanceTypes(1.0, true, []string{"zone-1", "zone-2"}, "arm64", "4"),
		"capacity":                             instanceTypes(1.0, true, []string{"zone-1", "zone-2"}, "amd64", "8"),
		"the set of instance types":            instanceTypes(1.0, true, []string{"zone-1", "zone-2"}, "amd64", "4")[1:],
	} {
		if got := mustContentHash(t, changed); got == base {
			t.Fatalf("changing %s did not change the instance type content hash", name)
		}
	}
}

func mustContentHash(t *testing.T, instanceTypes []*cloudprovider.InstanceType) uint64 {
	t.Helper()
	hash, ok := instanceTypesContentHash(instanceTypes)
	if !ok {
		t.Fatal("instance type content could not be hashed")
	}
	return hash
}

// TestInstanceTypesContentHashCoversOverridesAndDevices pins that offering resource overrides,
// keyed to the offering they apply to, and DRA device metadata are part of the content.
func TestInstanceTypesContentHashCoversOverridesAndDevices(t *testing.T) {
	zoneOffering := func(zone string, available bool, gpus string) cloudprovider.Offering {
		o := cloudprovider.Offering{
			Available: available,
			Price:     1,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				corev1.LabelTopologyZone: zone,
				v1.CapacityTypeLabelKey:  "on-demand",
			}),
		}
		if gpus != "" {
			o.CapacityOverride = corev1.ResourceList{"example.com/gpu": resource.MustParse(gpus)}
		}
		return o
	}
	withOfferings := func(offerings ...cloudprovider.Offering) []*cloudprovider.InstanceType {
		return []*cloudprovider.InstanceType{fake.NewInstanceType("type-a", fake.WithOfferings(offerings...))}
	}
	base := mustContentHash(t, withOfferings(zoneOffering("zone-1", true, "1"), zoneOffering("zone-2", true, "")))

	if got := mustContentHash(t, withOfferings(zoneOffering("zone-1", false, "1"), zoneOffering("zone-2", true, ""))); got != base {
		t.Fatal("an overridden offering becoming unavailable changed the hash")
	}
	for name, changed := range map[string][]*cloudprovider.InstanceType{
		"an offering's capacity override":       withOfferings(zoneOffering("zone-1", true, "2"), zoneOffering("zone-2", true, "")),
		"which offering an override applies to": withOfferings(zoneOffering("zone-1", true, ""), zoneOffering("zone-2", true, "1")),
		"the instance type's DRA device metadata": func() []*cloudprovider.InstanceType {
			instanceTypes := withOfferings(zoneOffering("zone-1", true, "1"), zoneOffering("zone-2", true, ""))
			template := fake.ResourceSliceTemplate("gpu.example.com", "pool", fake.Devices("gpu-0")...)
			instanceTypes[0].DynamicResources.ResourceSliceTemplates = []*cloudprovider.ResourceSliceTemplate{&template}
			return instanceTypes
		}(),
	} {
		if got := mustContentHash(t, changed); got == base {
			t.Fatalf("changing %s did not change the hash", name)
		}
	}

	// Device metadata is hashed by content: the same templates rebuilt are the same, and a
	// different driver name, held in an interned handle, is different.
	withDriver := func(driver string) []*cloudprovider.InstanceType {
		instanceTypes := withOfferings(zoneOffering("zone-1", true, "1"), zoneOffering("zone-2", true, ""))
		template := fake.ResourceSliceTemplate(driver, "pool", fake.Devices("gpu-0")...)
		instanceTypes[0].DynamicResources.ResourceSliceTemplates = []*cloudprovider.ResourceSliceTemplate{&template}
		return instanceTypes
	}
	withGPU := mustContentHash(t, withDriver("gpu.example.com"))
	if rebuilt := mustContentHash(t, withDriver("gpu.example.com")); rebuilt != withGPU {
		t.Fatal("rebuilding identical device metadata changed the hash")
	}
	if mustContentHash(t, withDriver("other.example.com")) == withGPU {
		t.Fatal("changing a device driver did not change the hash")
	}
}

// TestInstanceTypesContentHashOrderWithSharedNames pins order-insensitivity when several instance
// types share a name, as a provider with one backend per region returns them, in whatever order
// its backends answered.
func TestInstanceTypesContentHashOrderWithSharedNames(t *testing.T) {
	inZone := func(zone, cpu string) *cloudprovider.InstanceType {
		return fake.NewInstanceType("shared-name",
			fake.WithOfferings(cloudprovider.Offering{
				Available:    true,
				Price:        1,
				Requirements: scheduling.NewLabelRequirements(map[string]string{corev1.LabelTopologyZone: zone, v1.CapacityTypeLabelKey: "spot"}),
			}),
			fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}),
		)
	}
	x, y := inZone("zone-1", "4"), inZone("zone-2", "8")
	if mustContentHash(t, []*cloudprovider.InstanceType{x, y}) != mustContentHash(t, []*cloudprovider.InstanceType{y, x}) {
		t.Fatal("the order of instance types sharing a name changed the hash")
	}
	if mustContentHash(t, []*cloudprovider.InstanceType{x, x}) == mustContentHash(t, []*cloudprovider.InstanceType{x}) {
		t.Fatal("a repeated instance type canceled out of the hash")
	}
}

func reversed(instanceTypes []*cloudprovider.InstanceType) []*cloudprovider.InstanceType {
	slices.Reverse(instanceTypes)
	return instanceTypes
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
	unlistable := fake.NewCloudProvider()
	unlistable.ErrorsForNodePool[base.NodePool.Name] = errors.New("unavailable")
	if got := newNegativeCacheFingerprints(fakecr.NewFakeClient(), &fakeRevisionProvider{CloudProvider: unlistable, revision: 7}).fingerprint(ctx, base); got != "" {
		t.Fatal("a NodePool whose instance types cannot be listed must not fingerprint")
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
	if provider.calls != 1 || provider.getCalls != 1 {
		t.Fatalf("expected one revision and one instance type lookup per NodePool per pass, got %d and %d", provider.calls, provider.getCalls)
	}
}

// BenchmarkInstanceTypesContentHash measures the per-NodePool, per-pass cost of the instance type
// component of the fingerprint for a list the size a large cloud provider returns.
func BenchmarkInstanceTypesContentHash(b *testing.B) {
	instanceTypes := make([]*cloudprovider.InstanceType, 0, 700)
	for i := range 700 {
		instanceTypes = append(instanceTypes, fake.NewInstanceType(fmt.Sprintf("instance-type-%d", i)))
	}
	b.ReportAllocs()
	for b.Loop() {
		instanceTypesContentHash(instanceTypes)
	}
}
