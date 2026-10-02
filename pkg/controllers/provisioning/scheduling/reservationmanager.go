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

package scheduling

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

type ReservationManager struct {
	reservations map[string]sets.Set[string] // hostname -> set[reservation id]
	capacity     map[string]int              // reservation id -> count
}

// newReservationManagerTimed wraps construction with a construction phase duration observation,
// sourcing the reserved-offering capacity map through the pass-scoped cache when one is available.
func newReservationManagerTimed(ctx context.Context, nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) *ReservationManager {
	start := time.Now()
	defer func() {
		ConstructionPhaseDurationSeconds.Observe(time.Since(start).Seconds(), constructionPhaseLabels(ctx, phaseReservationManager))
	}()
	return &ReservationManager{
		reservations: map[string]sets.Set[string]{},
		capacity:     reservationCapacityWithCache(ctx, nodePools, instanceTypes),
	}
}

func NewReservationManager(instanceTypes map[string][]*cloudprovider.InstanceType) *ReservationManager {
	return &ReservationManager{
		reservations: map[string]sets.Set[string]{},
		capacity:     buildReservationCapacity(instanceTypes),
	}
}

// buildReservationCapacity scans every offering of every instance type for reserved capacity,
// tracking the smallest observed capacity per reservation ID.
func buildReservationCapacity(instanceTypes map[string][]*cloudprovider.InstanceType) map[string]int {
	capacity := map[string]int{}
	for _, its := range instanceTypes {
		for _, it := range its {
			for _, o := range it.Offerings {
				// Direct requirement access avoids the per-offering allocations in Requirements.Get
				// (which constructs a new Requirement for absent keys) and Requirement.Any (which
				// copies the value set) on this hot path. Reserved offerings always carry a
				// single-valued In requirement for the capacity type.
				ct, ok := o.Requirements[v1.CapacityTypeLabelKey]
				if !ok || ct.Operator() != corev1.NodeSelectorOpIn || !ct.Has(v1.CapacityTypeReserved) {
					continue
				}
				// If we have multiple offerings with the same reservation ID, track the one with the least capacity. This could be
				// the result of multiple nodepools referencing the same capacity reservation, and there being an update to the
				// capacity between calls to GetInstanceTypes.
				if current, ok := capacity[o.ReservationID()]; !ok || current > o.ReservationCapacity {
					capacity[o.ReservationID()] = o.ReservationCapacity
				}
			}
		}
	}
	return capacity
}

// Should always be idempotent
func (rm *ReservationManager) CanReserve(hostname string, offering *cloudprovider.Offering) bool {
	reservations, ok := rm.reservations[hostname]
	if ok && reservations.Has(offering.ReservationID()) {
		return true
	}
	capacity, ok := rm.capacity[offering.ReservationID()]
	if !ok {
		// Note: this panic should never occur, and would indicate a serious bug in the scheduling code.
		panic(fmt.Sprintf("attempted to reserve non-existent offering with reservation id %q", offering.ReservationID()))
	}
	if capacity == 0 {
		return false
	}
	return true
}

// Should always be idempotent
func (rm *ReservationManager) Reserve(hostname string, offerings ...*cloudprovider.Offering) {
	for _, of := range offerings {
		reservations, ok := rm.reservations[hostname]
		if ok && reservations.Has(of.ReservationID()) {
			continue
		}
		rm.capacity[of.ReservationID()] -= 1
		if rm.capacity[of.ReservationID()] < 0 {
			panic(fmt.Sprintf("attempted to over-reserve an offering with reservation id %q", of.ReservationID()))
		}
		if !ok {
			rm.reservations[hostname] = sets.New[string]()
		}
		rm.reservations[hostname].Insert(of.ReservationID())
	}
}

func (rm *ReservationManager) Release(hostname string, offerings ...*cloudprovider.Offering) {
	for _, o := range offerings {
		if reservations, ok := rm.reservations[hostname]; ok && reservations.Has(o.ReservationID()) {
			reservations.Delete(o.ReservationID())
			rm.capacity[o.ReservationID()] += 1
		}
	}
}

func (rm *ReservationManager) HasReservation(hostname string, offering *cloudprovider.Offering) bool {
	reservation, ok := rm.reservations[hostname]
	if ok && reservation.Has(offering.ReservationID()) {
		return true
	}
	return false
}

func (rm *ReservationManager) RemainingCapacity(offering *cloudprovider.Offering) int {
	return rm.capacity[offering.ReservationID()]
}
