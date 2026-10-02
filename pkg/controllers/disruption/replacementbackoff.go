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

// Replacement failure back-off: after a consolidation command fails because a replacement never
// launched or never initialized - typically an insufficient-capacity launch whose NodeClaim is
// deleted - its candidates are held off for a while instead of being proposed again on the next
// pass. Every retry costs a discovery slot, a NodeClaim create and delete, and a taint and untaint
// of the candidate, and the market that refused the launch rarely recovers within a pass or two.

package disruption

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// maxReplacementBackoffMultiple caps the doubling: a candidate is never held off for more than this
// many times the configured base.
const maxReplacementBackoffMultiple = 16

// replacementBackoffEntry is one candidate's hold.
type replacementBackoffEntry struct {
	// failures counts consecutive failed commands for the candidate with the same fingerprint.
	failures int
	// lastFailure is when the most recent failure was recorded.
	lastFailure time.Time
	// until is when the hold ends.
	until time.Time
	// fingerprint is what the candidate looked like when it failed; a different candidate is a
	// different replacement problem and is released.
	fingerprint string
}

// ReplacementBackoff holds consolidation candidates off after a command that would have replaced
// them failed because a replacement did not launch or initialize. The hold starts at the configured
// base (CONSOLIDATION_REPLACEMENT_FAILURE_BACKOFF, 0 disables it), doubles on each consecutive
// failure up to maxReplacementBackoffMultiple times the base, and is released early when the
// candidate's pods or NodePool change. It is safe for concurrent use: the queue records failures
// from its reconcilers while a pass reads holds.
type ReplacementBackoff struct {
	mu      sync.Mutex
	clock   clock.Clock
	entries map[string]*replacementBackoffEntry
}

// NewReplacementBackoff returns an empty back-off.
func NewReplacementBackoff(clk clock.Clock) *ReplacementBackoff {
	return &ReplacementBackoff{clock: clk, entries: map[string]*replacementBackoffEntry{}}
}

// replacementBackoffDuration is the hold after the given number of consecutive failures.
func replacementBackoffDuration(base time.Duration, failures int) time.Duration {
	multiple := 1 << min(failures-1, 30)
	return base * time.Duration(min(multiple, maxReplacementBackoffMultiple))
}

// replacementBackoffFingerprint identifies the replacement problem a candidate posed: its node, its
// NodePool's spec revision, and the pods that needed a new home.
func replacementBackoffFingerprint(c *Candidate) string {
	uids := lo.Map(c.reschedulablePods, func(p *corev1.Pod, _ int) string { return string(p.UID) })
	slices.Sort(uids)
	var b strings.Builder
	b.WriteString(c.ProviderID())
	if c.NodePool != nil {
		b.WriteString("|" + string(c.NodePool.UID) + "/" + strconv.FormatInt(c.NodePool.Generation, 10))
	}
	b.WriteString("|" + strings.Join(uids, ","))
	return b.String()
}

// RecordFailure starts or extends the hold of every candidate of a command whose replacements did
// not launch or initialize. It is a no-op while the back-off is disabled.
func (b *ReplacementBackoff) RecordFailure(ctx context.Context, candidates []*Candidate) {
	base := options.FromContext(ctx).ConsolidationReplacementFailureBackoff
	if base <= 0 {
		return
	}
	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(now, base)
	for _, c := range candidates {
		fingerprint := replacementBackoffFingerprint(c)
		entry, ok := b.entries[c.ProviderID()]
		if !ok || entry.fingerprint != fingerprint {
			entry = &replacementBackoffEntry{fingerprint: fingerprint}
			b.entries[c.ProviderID()] = entry
		}
		entry.failures++
		entry.lastFailure = now
		entry.until = now.Add(replacementBackoffDuration(base, entry.failures))
		ObserveReplacementFailureBackoff(c.NodePool.Name, entry.failures)
	}
}

// Holds reports whether a candidate is still held off. A candidate whose fingerprint changed since
// its failure is released, and its entry dropped.
func (b *ReplacementBackoff) Holds(ctx context.Context, c *Candidate) bool {
	if options.FromContext(ctx).ConsolidationReplacementFailureBackoff <= 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.entries[c.ProviderID()]
	if !ok {
		return false
	}
	if entry.fingerprint != replacementBackoffFingerprint(c) {
		delete(b.entries, c.ProviderID())
		return false
	}
	return b.clock.Now().Before(entry.until)
}

// Forget drops the holds of candidates whose command succeeded.
func (b *ReplacementBackoff) Forget(candidates []*Candidate) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range candidates {
		delete(b.entries, c.ProviderID())
	}
}

// pruneLocked drops entries that have been quiet for twice the longest hold, so candidates that
// left the fleet do not accumulate and a long-quiet candidate starts its doubling over.
func (b *ReplacementBackoff) pruneLocked(now time.Time, base time.Duration) {
	horizon := 2 * base * maxReplacementBackoffMultiple
	for providerID, entry := range b.entries {
		if now.Sub(entry.lastFailure) > horizon {
			delete(b.entries, providerID)
		}
	}
}
