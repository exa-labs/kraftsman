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

// EvaluatedCycleSize exposes the coverage-cycle cursor's size to the external test package.
func (s *SingleNodeConsolidation) EvaluatedCycleSize() int {
	return s.evaluatedThisCycle.Len()
}

// SeedEvaluatedCycle marks provider IDs as already reached by the coverage cycle, letting the
// external test package stage a partially-covered cycle.
func (s *SingleNodeConsolidation) SeedEvaluatedCycle(providerIDs ...string) {
	s.evaluatedThisCycle.Insert(providerIDs...)
}
