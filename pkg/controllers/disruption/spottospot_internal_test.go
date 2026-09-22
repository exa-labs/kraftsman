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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func annotatedNodePool(annotations map[string]string) *v1.NodePool {
	return &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool", Annotations: annotations}}
}

func TestSpotToSpotMinNodeAgeForNodePool(t *testing.T) {
	const def = 5 * time.Minute
	for _, tc := range []struct {
		name    string
		value   *string
		want    time.Duration
		wantErr string
	}{
		{name: "missing annotation selects the default", want: def},
		{name: "empty annotation selects the default", value: new(""), want: def},
		{name: "a duration overrides the default", value: new("30m"), want: 30 * time.Minute},
		{name: "zero turns the floor off", value: new("0"), want: 0},
		{name: "an unparseable duration selects the default", value: new("soon"), want: def, wantErr: `parsing karpenter.sh/spot-to-spot-min-node-age annotation value "soon"`},
		{name: "a negative duration selects the default", value: new("-1m"), want: def, wantErr: "expected a non-negative duration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var annotations map[string]string
			if tc.value != nil {
				annotations = map[string]string{v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey: *tc.value}
			}
			got, err := SpotToSpotMinNodeAgeForNodePool(annotatedNodePool(annotations), def)
			if got != tc.want {
				t.Fatalf("SpotToSpotMinNodeAgeForNodePool() = %s, want %s", got, tc.want)
			}
			checkSettingError(t, err, tc.wantErr, "using the default spot-to-spot min node age 5m0s")
		})
	}
}

func TestSpotToSpotMinSavingsForNodePool(t *testing.T) {
	const def = 0.2
	for _, tc := range []struct {
		name    string
		value   *string
		want    float64
		wantErr string
	}{
		{name: "missing annotation selects the default", want: def},
		{name: "empty annotation selects the default", value: new(""), want: def},
		{name: "a fraction overrides the default", value: new("0.05"), want: 0.05},
		{name: "zero turns the pool floor off", value: new("0"), want: 0},
		{name: "an unparseable value selects the default", value: new("five percent"), want: def, wantErr: `parsing karpenter.sh/spot-to-spot-min-savings annotation value "five percent"`},
		{name: "a negative fraction selects the default", value: new("-0.1"), want: def, wantErr: "expected a fraction in [0, 1)"},
		{name: "one selects the default", value: new("1"), want: def, wantErr: "expected a fraction in [0, 1)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var annotations map[string]string
			if tc.value != nil {
				annotations = map[string]string{v1.NodePoolSpotToSpotMinSavingsAnnotationKey: *tc.value}
			}
			got, err := SpotToSpotMinSavingsForNodePool(annotatedNodePool(annotations), def)
			if got != tc.want {
				t.Fatalf("SpotToSpotMinSavingsForNodePool() = %g, want %g", got, tc.want)
			}
			checkSettingError(t, err, tc.wantErr, "using the default spot-to-spot min savings 0.2")
		})
	}
}

func checkSettingError(t *testing.T, err error, wantErr, wantDefault string) {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("expected an error containing %q", wantErr)
	}
	if !strings.Contains(err.Error(), wantErr) || !strings.Contains(err.Error(), wantDefault) {
		t.Fatalf("error %q must name the default %q and the failure %q", err, wantDefault, wantErr)
	}
}

// Invalid annotations fall back to the controller-wide options and are reported once each on the NodePool.
func TestSpotToSpotSettingsForReportsInvalidAnnotations(t *testing.T) {
	ctx := options.ToContext(context.Background(), &options.Options{SpotToSpotMinNodeAge: time.Hour, SpotToSpotMinSavings: 0.1})
	recorder := test.NewEventRecorder()
	np := annotatedNodePool(map[string]string{
		v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey: "-1m",
		v1.NodePoolSpotToSpotMinSavingsAnnotationKey: "1",
	})

	got := spotToSpotSettingsFor(ctx, recorder, np)
	if got.minNodeAge != time.Hour || got.minSavings != 0.1 {
		t.Fatalf("spotToSpotSettingsFor() = %+v, want the controller-wide options", got)
	}
	if calls := recorder.Calls(events.InvalidSpotToSpotSetting); calls != 2 {
		t.Fatalf("published %d %s events, want 2", calls, events.InvalidSpotToSpotSetting)
	}
	recorder.ForEachEvent(func(e events.Event) {
		if e.InvolvedObject != np {
			t.Fatalf("event %q involves %T, want the NodePool", e.Message, e.InvolvedObject)
		}
	})
}

// Valid annotations override the controller-wide options without any event.
func TestSpotToSpotSettingsForPrefersAnnotations(t *testing.T) {
	ctx := options.ToContext(context.Background(), &options.Options{SpotToSpotMinNodeAge: time.Hour, SpotToSpotMinSavings: 0.1})
	recorder := test.NewEventRecorder()
	np := annotatedNodePool(map[string]string{
		v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey: "30m",
		v1.NodePoolSpotToSpotMinSavingsAnnotationKey: "0.05",
	})

	got := spotToSpotSettingsFor(ctx, recorder, np)
	if got.minNodeAge != 30*time.Minute || got.minSavings != 0.05 {
		t.Fatalf("spotToSpotSettingsFor() = %+v, want 30m and 0.05", got)
	}
	if calls := recorder.Calls(events.InvalidSpotToSpotSetting); calls != 0 {
		t.Fatalf("published %d %s events, want none", calls, events.InvalidSpotToSpotSetting)
	}
}
