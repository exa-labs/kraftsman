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
	"fmt"
	"math"
	"strconv"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// The spot-to-spot stability settings. Each has a controller-wide default from the operator options
// (SPOT_TO_SPOT_MIN_NODE_AGE, SPOT_TO_SPOT_MIN_SAVINGS) that a NodePool overrides through an annotation,
// so a pool whose pods are slow to boot can hold its spot nodes longer than a pool whose pods are free
// to restart. Zero disables a setting, at either level.

// SpotToSpotMinNodeAgeForNodePool reads np's v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey. A missing or
// empty annotation selects def; a value that is not a non-negative Go duration also selects def and is
// reported through the returned error.
func SpotToSpotMinNodeAgeForNodePool(np *v1.NodePool, def time.Duration) (time.Duration, error) {
	value := np.Annotations[v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey]
	if value == "" {
		return def, nil
	}
	age, err := time.ParseDuration(value)
	if err != nil {
		return def, fmt.Errorf("using the default spot-to-spot min node age %s, parsing %s annotation value %q, %w", def, v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey, value, err)
	}
	if age < 0 {
		return def, fmt.Errorf("using the default spot-to-spot min node age %s, invalid %s annotation value %q, expected a non-negative duration", def, v1.NodePoolSpotToSpotMinNodeAgeAnnotationKey, value)
	}
	return age, nil
}

// SpotToSpotMinSavingsForNodePool reads np's v1.NodePoolSpotToSpotMinSavingsAnnotationKey. A missing or
// empty annotation selects def; a value that is not a fraction in [0, 1) also selects def and is reported
// through the returned error.
func SpotToSpotMinSavingsForNodePool(np *v1.NodePool, def float64) (float64, error) {
	value := np.Annotations[v1.NodePoolSpotToSpotMinSavingsAnnotationKey]
	if value == "" {
		return def, nil
	}
	savings, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return def, fmt.Errorf("using the default spot-to-spot min savings %g, parsing %s annotation value %q, %w", def, v1.NodePoolSpotToSpotMinSavingsAnnotationKey, value, err)
	}
	if math.IsNaN(savings) || savings < 0 || savings >= 1 {
		return def, fmt.Errorf("using the default spot-to-spot min savings %g, invalid %s annotation value %q, expected a fraction in [0, 1)", def, v1.NodePoolSpotToSpotMinSavingsAnnotationKey, value)
	}
	return savings, nil
}

// spotToSpotStabilityFloor decides whether the candidates may be replaced with spot at all, before any
// replacement is priced: the feature gate must be on and each candidate must be at least as old as its
// NodePool's spot-to-spot age floor. It returns the strictest of the pools' savings floors, or the skip
// reason to attribute the candidate to when it is held back. An age hold is a no-op that expires on its
// own, so it is marked inconclusive and never stored in the negative result cache. Spot prices move
// continuously, so the cheapest node at launch is undercut within minutes; the age floor is what keeps
// a price move from draining the same pods again before they have done any work. It is measured from
// the NodeClaim's creation, unlike consolidateAfter, which restarts on every pod event.
func (c *consolidation) spotToSpotStabilityFloor(ctx context.Context, candidates []*Candidate, publishEvents bool) (float64, string) {
	unconsolidatable := func(message string) {
		if len(candidates) == 1 && publishEvents {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, message)...)
		}
	}
	if !options.FromContext(ctx).FeatureGates.SpotToSpotConsolidation {
		unconsolidatable("SpotToSpotConsolidation is disabled, can't replace a spot node with a spot node")
		return 0, CandidateSkipSpotToSpotDisabled
	}
	var minSavings float64
	for _, cn := range candidates {
		settings := spotToSpotSettingsFor(ctx, c.recorder, cn.NodePool)
		if settings.minNodeAge > 0 {
			if age := c.clock.Since(cn.NodeClaim.CreationTimestamp.Time); age < settings.minNodeAge {
				unconsolidatable(fmt.Sprintf("SpotToSpotConsolidation requires the node to be at least %s old before replacing it with spot, it is %s old", settings.minNodeAge, age.Truncate(time.Second)))
				markNoOpInconclusive(ctx)
				return 0, CandidateSkipSpotToSpotMinNodeAge
			}
		}
		minSavings = max(minSavings, settings.minSavings)
	}
	return minSavings, ""
}

// spotToSpotSettings is the age floor and savings floor that govern one spot-to-spot decision.
type spotToSpotSettings struct {
	minNodeAge time.Duration
	minSavings float64
}

// spotToSpotSettingsFor resolves the settings for np from its annotations over the controller defaults in
// ctx. An invalid annotation keeps the default so the decision is still made, and is surfaced as a
// NodePool event and a log line on every evaluation until it is fixed.
func spotToSpotSettingsFor(ctx context.Context, recorder events.Recorder, np *v1.NodePool) spotToSpotSettings {
	opts := options.FromContext(ctx)
	minNodeAge, err := SpotToSpotMinNodeAgeForNodePool(np, opts.SpotToSpotMinNodeAge)
	if err != nil {
		recorder.Publish(disruptionevents.InvalidSpotToSpotSetting(np, err))
		log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Error(err, "invalid spot-to-spot min node age annotation")
	}
	minSavings, err := SpotToSpotMinSavingsForNodePool(np, opts.SpotToSpotMinSavings)
	if err != nil {
		recorder.Publish(disruptionevents.InvalidSpotToSpotSetting(np, err))
		log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Error(err, "invalid spot-to-spot min savings annotation")
	}
	return spotToSpotSettings{minNodeAge: minNodeAge, minSavings: minSavings}
}
