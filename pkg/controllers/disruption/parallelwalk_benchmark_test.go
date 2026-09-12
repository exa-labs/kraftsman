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
	"testing"
	"time"
)

// BenchmarkCandidateWalk measures walk throughput with a fixed per-candidate simulation cost,
// which is where parallel discovery pays: with N workers the walk should complete close to N
// times faster than the serial walk for the same candidate list.
func BenchmarkCandidateWalk(b *testing.B) {
	const simCost = 200 * time.Microsecond
	candidates := walkCandidates(256)
	simulate := func(_ context.Context, _ *Candidate) (Command, error) {
		time.Sleep(simCost)
		return Command{}, nil
	}
	b.Run("serial", func(b *testing.B) {
		for b.Loop() {
			for _, c := range candidates {
				_, _ = simulate(context.Background(), c)
			}
		}
	})
	for _, workers := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			for b.Loop() {
				gate := newWalkGate(map[string]int{"np": len(candidates)}, passAll)
				w := startCandidateWalker(context.Background(), candidates, workers, gate, simulate)
				for i := range candidates {
					_ = w.result(i)
				}
				w.stop()
			}
		})
	}
}
