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

// Spot-first capacity arbitration hooks shared between the core and cloud
// providers that decide spot versus on-demand at launch time: a context flag
// asking a provider to probe a capacity market past its own insufficient
// capacity cooldowns, a deferral error that lets a provider hold a launch open
// without the claim being deleted or exponentially backed off, and the drift
// reason a provider reports for on-demand capacity it launched as a temporary
// fallback.

package cloudprovider

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
)

// LaunchDeferredError is returned by Create when the provider has decided not
// to launch yet rather than failed to launch: it wants more evidence before
// committing the claim to a capacity type. The lifecycle controller records
// ConditionReason/ConditionMessage on the Launched condition and requeues the
// claim after RetryAfter without counting a failure, so the time a claim spends
// deferred is bounded by wall clock instead of an exponential backoff whose
// last gap could push it past the launch timeout.
type LaunchDeferredError struct {
	error
	ConditionReason  string
	ConditionMessage string
	RetryAfter       time.Duration
}

// NewLaunchDeferredError wraps err as a deferral to be retried after retryAfter.
func NewLaunchDeferredError(err error, reason, message string, retryAfter time.Duration) *LaunchDeferredError {
	return &LaunchDeferredError{error: err, ConditionReason: reason, ConditionMessage: message, RetryAfter: retryAfter}
}

func (e *LaunchDeferredError) Error() string { return e.error.Error() }

func (e *LaunchDeferredError) Unwrap() error { return e.error }

// AsLaunchDeferredError returns the LaunchDeferredError in err's chain, if any.
func AsLaunchDeferredError(err error) (*LaunchDeferredError, bool) {
	var deferred *LaunchDeferredError
	if errors.As(err, &deferred) {
		return deferred, true
	}
	return nil, false
}

// DriftReasonOnDemandLeaseExpired is reported by cloud providers that launch
// on-demand capacity under a time-bounded lease when spot is not obtainable.
// It means the lease expired and spot is obtainable again, so the node should
// be replaced with a cheaper capacity type. The drift disruption method
// schedules these ahead of template drift: the replacement is a price
// correction, not a spec rollout, and waiting behind a fleet-wide template
// change would leave the expensive capacity running for hours.
const DriftReasonOnDemandLeaseExpired DriftReason = "OnDemandLeaseExpired"

type ignoreUnavailableKey struct{}

// WithUnavailableOfferingsIgnored asks the CloudProvider to disregard its own
// transient unavailability caches (insufficient-capacity cooldowns) for the
// given capacity types on calls made with the returned context. Offerings that
// are unavailable for structural reasons (no price, not offered in the zone,
// zonal shift, incompatible node class) are still reported unavailable. Used
// to probe a capacity market directly against the cloud API instead of
// trusting stale cache evidence.
func WithUnavailableOfferingsIgnored(ctx context.Context, capacityTypes ...string) context.Context {
	if len(capacityTypes) == 0 {
		return ctx
	}
	existing := IgnoredUnavailableCapacityTypes(ctx)
	return context.WithValue(ctx, ignoreUnavailableKey{}, existing.Union(sets.New(capacityTypes...)))
}

// IgnoredUnavailableCapacityTypes returns the capacity types for which the
// caller asked unavailability caches to be ignored. The result is never nil.
func IgnoredUnavailableCapacityTypes(ctx context.Context) sets.Set[string] {
	if ctx == nil {
		return sets.New[string]()
	}
	if v, ok := ctx.Value(ignoreUnavailableKey{}).(sets.Set[string]); ok {
		return v
	}
	return sets.New[string]()
}
