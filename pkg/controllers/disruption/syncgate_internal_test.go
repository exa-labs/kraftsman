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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	clock "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

func TestClassifyUnlaunched(t *testing.T) {
	replacements := sets.New("replacement")
	for _, tc := range []struct {
		name      string
		nodeClaim state.UnlaunchedNodeClaim
		want      string
	}{
		{"no launch attempt yet", state.UnlaunchedNodeClaim{Name: "a", PodsRecorded: true}, unlaunchedInFlight},
		{"deferred by the cloud provider", state.UnlaunchedNodeClaim{Name: "a", PodsRecorded: true, LaunchAttemptReason: "CapacityEvidencePending"}, unlaunchedLaunchDeferred},
		{"failed launch", state.UnlaunchedNodeClaim{Name: "a", PodsRecorded: true, LaunchAttemptReason: launchFailedReason}, unlaunchedLaunchFailed},
		{"pods not recorded", state.UnlaunchedNodeClaim{Name: "a"}, unlaunchedUnrecorded},
		{"deferred, pods not recorded", state.UnlaunchedNodeClaim{Name: "a", LaunchAttemptReason: "CapacityEvidencePending"}, unlaunchedUnrecorded},
		{"deleting after a failed launch", state.UnlaunchedNodeClaim{Name: "a", Deleting: true, LaunchAttemptReason: launchFailedReason}, unlaunchedDeleting},
		{"replacement", state.UnlaunchedNodeClaim{Name: "replacement", PodsRecorded: true}, unlaunchedReplacement},
		{"annotated replacement the queue has not registered yet", state.UnlaunchedNodeClaim{Name: "a", Replacement: true}, unlaunchedReplacement},
		{"deleting annotated replacement", state.UnlaunchedNodeClaim{Name: "a", Replacement: true, Deleting: true}, unlaunchedReplacement},
		{"deferred replacement", state.UnlaunchedNodeClaim{Name: "replacement", LaunchAttemptReason: "CapacityEvidencePending"}, unlaunchedReplacement},
		{"deleting replacement", state.UnlaunchedNodeClaim{Name: "replacement", Deleting: true}, unlaunchedReplacement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyUnlaunched(tc.nodeClaim, replacements); got != tc.want {
				t.Fatalf("classifyUnlaunched() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestJudgeUnlaunched(t *testing.T) {
	inFlight := state.UnlaunchedNodeClaim{Name: "in-flight", PodsRecorded: true}
	deferred := state.UnlaunchedNodeClaim{Name: "deferred", PodsRecorded: true, LaunchAttemptReason: "CapacityEvidencePending"}
	deleting := state.UnlaunchedNodeClaim{Name: "deleting", Deleting: true}
	unrecorded := state.UnlaunchedNodeClaim{Name: "unrecorded"}
	replacement := state.UnlaunchedNodeClaim{Name: "replacement", PodsRecorded: true}
	replacements := sets.New("replacement")

	for _, tc := range []struct {
		name       string
		policy     options.DisruptionSyncPolicy
		unlaunched []state.UnlaunchedNodeClaim
		proceed    bool
		reason     string
		inFlight   []string
	}{
		{"strict waits on a deleting NodeClaim", options.DisruptionSyncPolicyStrict, []state.UnlaunchedNodeClaim{deleting}, false, unlaunchedDeleting, nil},
		{"failed-or-deferred skips deleting and deferred, leaving out the deferred one's pods", options.DisruptionSyncPolicyIgnoreFailedOrDeferred, []state.UnlaunchedNodeClaim{deleting, deferred}, true, unlaunchedLaunchDeferred, []string{"deferred"}},
		{"failed-or-deferred waits on an in-flight NodeClaim", options.DisruptionSyncPolicyIgnoreFailedOrDeferred, []state.UnlaunchedNodeClaim{deferred, inFlight}, false, unlaunchedInFlight, nil},
		{"non-replacements skips an in-flight NodeClaim", options.DisruptionSyncPolicyIgnoreNonReplacements, []state.UnlaunchedNodeClaim{deferred, inFlight, deleting}, true, unlaunchedInFlight, []string{"deferred", "in-flight"}},
		{"non-replacements waits on a NodeClaim whose pods are not recorded", options.DisruptionSyncPolicyIgnoreNonReplacements, []state.UnlaunchedNodeClaim{inFlight, unrecorded}, false, unlaunchedUnrecorded, nil},
		{"non-replacements waits on a replacement", options.DisruptionSyncPolicyIgnoreNonReplacements, []state.UnlaunchedNodeClaim{inFlight, replacement}, false, unlaunchedReplacement, nil},
		{"non-replacements skips a deleting NodeClaim without leaving out its pods", options.DisruptionSyncPolicyIgnoreNonReplacements, []state.UnlaunchedNodeClaim{deleting}, true, unlaunchedDeleting, nil},
		{"an empty policy is strict", "", []state.UnlaunchedNodeClaim{deleting}, false, unlaunchedDeleting, nil},
		{"no unlaunched NodeClaims", options.DisruptionSyncPolicyStrict, nil, true, "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := judgeUnlaunched(tc.policy, tc.unlaunched, replacements)
			if got.Proceed != tc.proceed || got.Reason != tc.reason || !got.CapacityInFlight.Equal(sets.New(tc.inFlight...)) {
				t.Fatalf("judgeUnlaunched() = {%v %q %v}, want {%v %q %v}", got.Proceed, got.Reason, sets.List(got.CapacityInFlight), tc.proceed, tc.reason, tc.inFlight)
			}
		})
	}
}

// TestWithoutCapacityInFlight checks that a pass leaves out only the pods recorded against a skipped
// NodeClaim that is still launching, and simulates them again once it launches or dies.
func TestWithoutCapacityInFlight(t *testing.T) {
	newPod := func(name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	}
	unlaunched := func(name string) *v1.NodeClaim {
		return &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	setup := func() (*state.Cluster, context.Context, []*corev1.Pod) {
		cluster := state.NewCluster(clock.NewFakeClock(time.Now()), nil, nil)
		forSkipped, forOther, unrecorded := newPod("for-skipped"), newPod("for-other"), newPod("unrecorded")
		cluster.UpdateNodeClaim(unlaunched("skipped"))
		cluster.UpdateNodeClaim(unlaunched("other"))
		cluster.UpdatePodToNodeClaimMapping(map[string][]*corev1.Pod{"skipped": {forSkipped}, "other": {forOther}})
		ctx := withCapacityInFlight(context.Background(), syncVerdict{Proceed: true, CapacityInFlight: sets.New("skipped")})
		return cluster, ctx, []*corev1.Pod{forSkipped, forOther, unrecorded}
	}
	names := func(pods []*corev1.Pod) []string {
		var out []string
		for _, p := range pods {
			out = append(out, p.Name)
		}
		return out
	}
	expect := func(t *testing.T, cluster *state.Cluster, ctx context.Context, pods []*corev1.Pod, wantExcluded ...string) {
		t.Helper()
		kept, excluded := withoutCapacityInFlight(ctx, cluster, pods)
		if !sets.New(names(excluded)...).Equal(sets.New(wantExcluded...)) || len(kept)+len(excluded) != len(pods) {
			t.Fatalf("excluded %v, kept %v; want excluded %v", names(excluded), names(kept), wantExcluded)
		}
	}

	t.Run("leaves out only the pods recorded against the skipped NodeClaim", func(t *testing.T) {
		cluster, ctx, pods := setup()
		expect(t, cluster, ctx, pods, "for-skipped")
	})
	t.Run("leaves out nothing for a pass that skipped nothing", func(t *testing.T) {
		cluster, _, pods := setup()
		expect(t, cluster, context.Background(), pods)
	})
	t.Run("simulates the pods again once the NodeClaim is deleted", func(t *testing.T) {
		cluster, ctx, pods := setup()
		cluster.DeleteNodeClaim("skipped")
		expect(t, cluster, ctx, pods)
	})
	t.Run("simulates the pods again once the NodeClaim is deleting", func(t *testing.T) {
		cluster, ctx, pods := setup()
		deleting := unlaunched("skipped")
		deleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		cluster.UpdateNodeClaim(deleting)
		expect(t, cluster, ctx, pods)
	})
	t.Run("simulates the pods again once the NodeClaim launches", func(t *testing.T) {
		cluster, ctx, pods := setup()
		launched := unlaunched("skipped")
		launched.Status.ProviderID = "fake:///launched"
		cluster.UpdateNodeClaim(launched)
		expect(t, cluster, ctx, pods)
	})
	t.Run("follows a pod recorded against a different NodeClaim since", func(t *testing.T) {
		cluster, ctx, pods := setup()
		cluster.UpdatePodToNodeClaimMapping(map[string][]*corev1.Pod{"other": {pods[0]}})
		expect(t, cluster, ctx, pods)
	})
}
