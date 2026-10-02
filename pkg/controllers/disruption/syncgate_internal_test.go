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
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"

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
		{"no launch attempt yet", state.UnlaunchedNodeClaim{Name: "a"}, unlaunchedInFlight},
		{"deferred by the cloud provider", state.UnlaunchedNodeClaim{Name: "a", LaunchAttemptReason: "CapacityEvidencePending"}, unlaunchedLaunchDeferred},
		{"failed launch", state.UnlaunchedNodeClaim{Name: "a", LaunchAttemptReason: launchFailedReason}, unlaunchedLaunchFailed},
		{"deleting after a failed launch", state.UnlaunchedNodeClaim{Name: "a", Deleting: true, LaunchAttemptReason: launchFailedReason}, unlaunchedDeleting},
		{"replacement", state.UnlaunchedNodeClaim{Name: "replacement"}, unlaunchedReplacement},
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
	inFlight := state.UnlaunchedNodeClaim{Name: "in-flight"}
	deferred := state.UnlaunchedNodeClaim{Name: "deferred", LaunchAttemptReason: "CapacityEvidencePending"}
	deleting := state.UnlaunchedNodeClaim{Name: "deleting", Deleting: true}
	replacement := state.UnlaunchedNodeClaim{Name: "replacement"}
	replacements := sets.New("replacement")

	for _, tc := range []struct {
		name       string
		policy     options.DisruptionSyncPolicy
		unlaunched []state.UnlaunchedNodeClaim
		want       syncVerdict
	}{
		{"strict waits on a deleting NodeClaim", options.DisruptionSyncPolicyStrict, []state.UnlaunchedNodeClaim{deleting}, syncVerdict{false, unlaunchedDeleting}},
		{"failed-or-deferred skips deleting and deferred", options.DisruptionSyncPolicyIgnoreFailedOrDeferred, []state.UnlaunchedNodeClaim{deleting, deferred}, syncVerdict{true, unlaunchedLaunchDeferred}},
		{"failed-or-deferred waits on an in-flight NodeClaim", options.DisruptionSyncPolicyIgnoreFailedOrDeferred, []state.UnlaunchedNodeClaim{deferred, inFlight}, syncVerdict{false, unlaunchedInFlight}},
		{"non-replacements skips an in-flight NodeClaim", options.DisruptionSyncPolicyIgnoreNonReplacements, []state.UnlaunchedNodeClaim{deferred, inFlight, deleting}, syncVerdict{true, unlaunchedInFlight}},
		{"non-replacements waits on a replacement", options.DisruptionSyncPolicyIgnoreNonReplacements, []state.UnlaunchedNodeClaim{inFlight, replacement}, syncVerdict{false, unlaunchedReplacement}},
		{"an empty policy is strict", "", []state.UnlaunchedNodeClaim{deleting}, syncVerdict{false, unlaunchedDeleting}},
		{"no unlaunched NodeClaims", options.DisruptionSyncPolicyStrict, nil, syncVerdict{true, ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := judgeUnlaunched(tc.policy, tc.unlaunched, replacements); got != tc.want {
				t.Fatalf("judgeUnlaunched() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
