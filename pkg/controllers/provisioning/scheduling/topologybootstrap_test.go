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

// Tests for how a self-selecting pod affinity picks the domain it bootstraps in: the cheapest domain a new NodeClaim
// could launch in, with existing and in-flight nodes, anti-affinity and topology spread left as they were.

package scheduling

import (
	"context"
	"slices"
	"testing"
	"unique"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling/dynamicresources"
)

var bootstrapZones = []string{"zone-a", "zone-b", "zone-c", "zone-d"}

// gangPod is a pod of a gang that selects itself through its app label.
func gangPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "default",
		Name:      "gang-0",
		UID:       types.UID("gang-0"),
		Labels:    map[string]string{"app": "gang"},
	}}
}

// newZoneTopologyGroup builds a topology group of the given type on the zone key that selects gangPod and knows every
// bootstrap zone.
func newZoneTopologyGroup(topologyType TopologyType, pod *corev1.Pod) *TopologyGroup {
	domains := NewTopologyDomainGroup()
	for _, zone := range bootstrapZones {
		domains.Insert(zone, "default", nil, scheduling.NewRequirements())
	}
	return NewTopologyGroup(topologyType, corev1.LabelTopologyZone, pod, sets.New(pod.Namespace),
		&metav1.LabelSelector{MatchLabels: map[string]string{"app": "gang"}}, 1, nil, nil, nil, domains)
}

// zoneTopology wraps a single topology group owned by pod.
func zoneTopology(tg *TopologyGroup, pod *corev1.Pod) *Topology {
	tg.AddOwner(pod.UID)
	return &Topology{topologyGroups: map[uint64]*TopologyGroup{tg.Hash(): tg}}
}

func staticPrices(prices map[string]float64) DomainPriceFunc {
	return func(string) map[string]float64 { return prices }
}

// forbiddenPrices fails the test if the topology consults prices at all.
func forbiddenPrices(t *testing.T) DomainPriceFunc {
	return func(string) map[string]float64 {
		t.Helper()
		t.Fatalf("prices must not be consulted")
		return nil
	}
}

// zoneRequirements returns node requirements restricting the zone with the given operator and values.
func zoneRequirements(op corev1.NodeSelectorOperator, zones ...string) scheduling.Requirements {
	return scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, op, zones...))
}

// bootstrappedZones returns the zone values AddRequirements settles on for a node with nodeRequirements.
func bootstrappedZones(t *testing.T, topology *Topology, pod *corev1.Pod, nodeRequirements scheduling.Requirements, prices DomainPriceFunc) []string {
	t.Helper()
	requirements, err := topology.AddRequirements(pod, nil, scheduling.NewRequirements(), nodeRequirements, NewBootstrapPreference(prices))
	if err != nil {
		t.Fatalf("AddRequirements: %v", err)
	}
	zones := requirements.Get(corev1.LabelTopologyZone).Values()
	slices.Sort(zones)
	return zones
}

func TestAffinityBootstrapPicksCheapestDomain(t *testing.T) {
	pod := gangPod()
	topology := zoneTopology(newZoneTopologyGroup(TopologyTypePodAffinity, pod), pod)
	prices := staticPrices(map[string]float64{"zone-a": 3, "zone-b": 1, "zone-c": 2, "zone-d": 1.5})
	for range 20 {
		if got := bootstrappedZones(t, topology, pod, zoneRequirements(corev1.NodeSelectorOpExists), prices); !slices.Equal(got, []string{"zone-b"}) {
			t.Fatalf("expected the cheapest zone, got %v", got)
		}
	}
}

func TestAffinityBootstrapHonoursNodeDomains(t *testing.T) {
	pod := gangPod()
	topology := zoneTopology(newZoneTopologyGroup(TopologyTypePodAffinity, pod), pod)
	prices := staticPrices(map[string]float64{"zone-a": 3, "zone-b": 1, "zone-c": 2, "zone-d": 1.5})
	for _, tc := range []struct {
		name     string
		node     scheduling.Requirements
		expected []string
	}{
		{"In", zoneRequirements(corev1.NodeSelectorOpIn, "zone-a", "zone-c"), []string{"zone-c"}},
		{"NotIn", zoneRequirements(corev1.NodeSelectorOpNotIn, "zone-b"), []string{"zone-d"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := bootstrappedZones(t, topology, pod, tc.node, prices); !slices.Equal(got, tc.expected) {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestAffinityBootstrapHonoursPodDomains(t *testing.T) {
	pod := gangPod()
	topology := zoneTopology(newZoneTopologyGroup(TopologyTypePodAffinity, pod), pod)
	prices := staticPrices(map[string]float64{"zone-a": 3, "zone-b": 1, "zone-c": 2, "zone-d": 1.5})
	podRequirements := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "zone-a", "zone-c"))
	requirements, err := topology.AddRequirements(pod, nil, podRequirements, zoneRequirements(corev1.NodeSelectorOpExists), NewBootstrapPreference(prices))
	if err != nil {
		t.Fatalf("AddRequirements: %v", err)
	}
	if got := requirements.Get(corev1.LabelTopologyZone).Values(); !slices.Equal(got, []string{"zone-c"}) {
		t.Fatalf("expected the cheapest zone the pod allows, got %v", got)
	}
}

func TestAffinityBootstrapPrefersPricedDomains(t *testing.T) {
	pod := gangPod()
	topology := zoneTopology(newZoneTopologyGroup(TopologyTypePodAffinity, pod), pod)
	for range 20 {
		if got := bootstrappedZones(t, topology, pod, zoneRequirements(corev1.NodeSelectorOpExists), staticPrices(map[string]float64{"zone-c": 9})); !slices.Equal(got, []string{"zone-c"}) {
			t.Fatalf("expected the only priced zone, got %v", got)
		}
	}
}

// Without prices, or with equal prices, the bootstrap is deterministic for a group.
func TestAffinityBootstrapIsDeterministicWithoutPrices(t *testing.T) {
	pod := gangPod()
	topology := zoneTopology(newZoneTopologyGroup(TopologyTypePodAffinity, pod), pod)
	for _, prices := range []DomainPriceFunc{staticPrices(nil), staticPrices(map[string]float64{"zone-a": 1, "zone-b": 1, "zone-c": 1, "zone-d": 1})} {
		first := bootstrappedZones(t, topology, pod, zoneRequirements(corev1.NodeSelectorOpExists), prices)
		if len(first) != 1 {
			t.Fatalf("expected one zone, got %v", first)
		}
		for range 20 {
			if got := bootstrappedZones(t, topology, pod, zoneRequirements(corev1.NodeSelectorOpExists), prices); !slices.Equal(got, first) {
				t.Fatalf("expected %v every time, got %v", first, got)
			}
		}
	}
}

func TestOrderBootstrapCandidates(t *testing.T) {
	prices := map[string]float64{"zone-a": 2, "zone-b": 1, "zone-c": 2, "zone-d": 2}
	domains := []string{"zone-e", "zone-d", "zone-c", "zone-b", "zone-a"}
	orderBootstrapCandidates(domains, prices, 42)
	if domains[0] != "zone-b" || domains[4] != "zone-e" {
		t.Fatalf("expected the cheapest zone first and the unpriced zone last, got %v", domains)
	}
	again := []string{"zone-a", "zone-b", "zone-c", "zone-d", "zone-e"}
	orderBootstrapCandidates(again, prices, 42)
	if !slices.Equal(domains, again) {
		t.Fatalf("expected the same order regardless of input order, got %v and %v", domains, again)
	}

	// Groups with different seeds spread across equally priced domains.
	firsts := sets.New[string]()
	for seed := range uint64(16) {
		uniform := []string{"zone-a", "zone-b", "zone-c", "zone-d"}
		orderBootstrapCandidates(uniform, map[string]float64{"zone-a": 1, "zone-b": 1, "zone-c": 1, "zone-d": 1}, seed)
		firsts.Insert(uniform[0])
	}
	if firsts.Len() < 3 {
		t.Fatalf("expected equally priced zones to spread across groups, got %v", sets.List(firsts))
	}
}

// Excluding a bootstrapped domain moves the next evaluation to the next-cheapest one, until none are left.
func TestBootstrapPreferenceExcludesChosenDomains(t *testing.T) {
	pod := gangPod()
	topology := zoneTopology(newZoneTopologyGroup(TopologyTypePodAffinity, pod), pod)
	preference := NewBootstrapPreference(staticPrices(map[string]float64{"zone-a": 3, "zone-b": 1, "zone-c": 2, "zone-d": 1.5}))
	for _, expected := range []string{"zone-b", "zone-d", "zone-c", "zone-a"} {
		requirements, err := topology.AddRequirements(pod, nil, scheduling.NewRequirements(), zoneRequirements(corev1.NodeSelectorOpExists), preference)
		if err != nil {
			t.Fatalf("AddRequirements: %v", err)
		}
		if got := requirements.Get(corev1.LabelTopologyZone).Values(); !slices.Equal(got, []string{expected}) {
			t.Fatalf("expected %s, got %v", expected, got)
		}
		if !preference.Chosen() {
			t.Fatalf("expected the choice of %s to be recorded", expected)
		}
		preference.ExcludeChosen()
	}
	if _, err := topology.AddRequirements(pod, nil, scheduling.NewRequirements(), zoneRequirements(corev1.NodeSelectorOpExists), preference); err == nil {
		t.Fatalf("expected no domain once every candidate is excluded")
	}
	if preference.Chosen() {
		t.Fatalf("expected no choice once every candidate is excluded")
	}
}

// A node already pinned to a domain (an existing node, or an in-flight NodeClaim) bootstraps in its own domain even when
// another is cheaper, and prices are not consulted for it.
func TestAffinityBootstrapKeepsPinnedNodeDomain(t *testing.T) {
	pod := gangPod()
	topology := zoneTopology(newZoneTopologyGroup(TopologyTypePodAffinity, pod), pod)
	if got := bootstrappedZones(t, topology, pod, zoneRequirements(corev1.NodeSelectorOpIn, "zone-a"), forbiddenPrices(t)); !slices.Equal(got, []string{"zone-a"}) {
		t.Fatalf("expected the node's own zone, got %v", got)
	}
}

// Once a matching pod runs in a domain, the affinity follows it rather than the prices.
func TestAffinityFollowsScheduledPodsOverPrices(t *testing.T) {
	pod := gangPod()
	tg := newZoneTopologyGroup(TopologyTypePodAffinity, pod)
	tg.Record("zone-a")
	topology := zoneTopology(tg, pod)
	if got := bootstrappedZones(t, topology, pod, zoneRequirements(corev1.NodeSelectorOpExists), forbiddenPrices(t)); !slices.Equal(got, []string{"zone-a"}) {
		t.Fatalf("expected the zone the gang already runs in, got %v", got)
	}
}

func TestAntiAffinityAndSpreadIgnorePrices(t *testing.T) {
	pod := gangPod()
	for _, topologyType := range []TopologyType{TopologyTypePodAntiAffinity, TopologyTypeSpread} {
		t.Run(topologyType.String(), func(t *testing.T) {
			tg := newZoneTopologyGroup(topologyType, pod)
			tg.Record("zone-b")
			nodeDomains := scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpExists)
			podDomains := scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpExists)
			withoutPrices, validWithout := tg.Get(pod, podDomains, nodeDomains, nil)
			withPrices, validWith := tg.Get(pod, podDomains, nodeDomains, NewBootstrapPreference(forbiddenPrices(t)))
			if topologyType == TopologyTypePodAntiAffinity {
				// Anti-affinity returns every empty domain, so the result is deterministic and comparable.
				if !withoutPrices.Has("zone-a") || withoutPrices.Has("zone-b") || withoutPrices.Len() != 3 {
					t.Fatalf("expected every zone but zone-b, got %v", withoutPrices.Values())
				}
				if !slices.Equal(sets.List(sets.New(withPrices.Values()...)), sets.List(sets.New(withoutPrices.Values()...))) {
					t.Fatalf("prices changed anti-affinity: %v vs %v", withPrices.Values(), withoutPrices.Values())
				}
			}
			if !validWith.Equal(validWithout) {
				t.Fatalf("prices changed valid domains: %v vs %v", sets.List(validWith), sets.List(validWithout))
			}
		})
	}
}

func TestCheapestPriceByDomain(t *testing.T) {
	small := fake.NewInstanceType("small",
		fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}),
		fake.WithOfferings(
			offeringInZone(v1.CapacityTypeOnDemand, "zone-a", 0.1, true),
			offeringInZone(v1.CapacityTypeOnDemand, "zone-b", 0.1, true),
		))
	large := fake.NewInstanceType("large",
		fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16")}),
		fake.WithOfferings(
			offeringInZone(v1.CapacityTypeOnDemand, "zone-a", 3, true),
			offeringInZone(v1.CapacityTypeSpot, "zone-a", 0.5, false), // unavailable
			offeringInZone(v1.CapacityTypeSpot, "zone-b", 1.2, true),
			offeringInZone(v1.CapacityTypeOnDemand, "zone-b", 2, true),
			offeringInZone(v1.CapacityTypeOnDemand, "zone-c", 2.5, true),
			offeringInZone(v1.CapacityTypeSpot, "zone-d", 0.2, false), // unavailable, and the zone's only offering
		))
	larger := fake.NewInstanceType("larger",
		fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32")}),
		fake.WithOfferings(offeringInZone(v1.CapacityTypeOnDemand, "zone-c", 2.2, true)))
	instanceTypes := []*cloudprovider.InstanceType{small, large, larger}
	requests := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}
	always := func(*cloudprovider.Offering) bool { return true }
	noDaemons := []DaemonOverheadGroup{{InstanceTypes: instanceTypes, HostPortUsage: scheduling.NewHostPortUsage()}}

	hostPortPod := gangPod()
	hostPortPod.Spec.Containers = []corev1.Container{{Ports: []corev1.ContainerPort{{HostPort: 8080, ContainerPort: 8080, Protocol: corev1.ProtocolTCP}}}}
	daemonPort := scheduling.NewHostPortUsage()
	daemonPort.Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "daemon"}}, scheduling.GetHostPorts(hostPortPod))

	for _, tc := range []struct {
		name         string
		groups       []DaemonOverheadGroup
		pod          *corev1.Pod
		requirements scheduling.Requirements
		key          string
		usable       func(*cloudprovider.Offering) bool
		expected     map[string]float64
	}{
		{
			name: "daemon overhead",
			groups: []DaemonOverheadGroup{
				{InstanceTypes: []*cloudprovider.InstanceType{small, large}, DaemonOverhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10")}, HostPortUsage: scheduling.NewHostPortUsage()},
				{InstanceTypes: []*cloudprovider.InstanceType{larger}, HostPortUsage: scheduling.NewHostPortUsage()},
			},
			requirements: scheduling.NewRequirements(),
			key:          corev1.LabelTopologyZone,
			usable:       always,
			expected:     map[string]float64{"zone-c": 2.2},
		},
		{
			name: "daemon host port conflicts",
			groups: []DaemonOverheadGroup{
				{InstanceTypes: []*cloudprovider.InstanceType{small, large}, HostPortUsage: daemonPort},
				{InstanceTypes: []*cloudprovider.InstanceType{larger}, HostPortUsage: scheduling.NewHostPortUsage()},
			},
			pod:          hostPortPod,
			requirements: scheduling.NewRequirements(),
			key:          corev1.LabelTopologyZone,
			usable:       always,
			expected:     map[string]float64{"zone-c": 2.2},
		},
		{
			name:         "cheapest available offering that fits, per zone",
			requirements: scheduling.NewRequirements(),
			key:          corev1.LabelTopologyZone,
			usable:       always,
			expected:     map[string]float64{"zone-a": 3, "zone-b": 1.2, "zone-c": 2.2},
		},
		{
			name:         "capacity type restriction",
			requirements: scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand)),
			key:          corev1.LabelTopologyZone,
			usable:       always,
			expected:     map[string]float64{"zone-a": 3, "zone-b": 2, "zone-c": 2.2},
		},
		{
			name:         "zone restriction",
			requirements: zoneRequirements(corev1.NodeSelectorOpIn, "zone-a", "zone-c"),
			key:          corev1.LabelTopologyZone,
			usable:       always,
			expected:     map[string]float64{"zone-a": 3, "zone-c": 2.2},
		},
		{
			name:         "instance type restriction",
			requirements: scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, "larger")),
			key:          corev1.LabelTopologyZone,
			usable:       always,
			expected:     map[string]float64{"zone-c": 2.2},
		},
		{
			name:         "unusable offerings",
			requirements: scheduling.NewRequirements(),
			key:          corev1.LabelTopologyZone,
			usable:       func(o *cloudprovider.Offering) bool { return o.CapacityType() != v1.CapacityTypeSpot },
			expected:     map[string]float64{"zone-a": 3, "zone-b": 2, "zone-c": 2.2},
		},
		{
			name:         "other offering keys",
			requirements: scheduling.NewRequirements(),
			key:          v1.CapacityTypeLabelKey,
			usable:       always,
			expected:     map[string]float64{v1.CapacityTypeOnDemand: 2, v1.CapacityTypeSpot: 1.2},
		},
		{
			name:         "keys offerings do not carry",
			requirements: scheduling.NewRequirements(),
			key:          corev1.LabelHostname,
			usable:       always,
			expected:     map[string]float64{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups := tc.groups
			if groups == nil {
				groups = noDaemons
			}
			pod := tc.pod
			if pod == nil {
				pod = gangPod()
			}
			got := cheapestPriceByDomain(instanceTypes, groups, pod, tc.requirements, requests, tc.key, tc.usable)
			if len(got) != len(tc.expected) {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
			for domain, price := range tc.expected {
				if got[domain] != price {
					t.Fatalf("expected %v, got %v", tc.expected, got)
				}
			}
		})
	}
}

// A NodeClaim prices domains from its own instance type options, so an affinity it bootstraps lands in the zone its
// cheapest available offering is in, and skips a reserved offering it can no longer reserve.
func TestNodeClaimBootstrapsAffinityInCheapestZone(t *testing.T) {
	reserved := cloudprovider.Offering{
		Available:           true,
		Price:               0.01,
		ReservationCapacity: 0,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:          v1.CapacityTypeReserved,
			corev1.LabelTopologyZone:         "zone-d",
			cloudprovider.ReservationIDLabel: "reservation-1",
		}),
	}
	it := fake.NewInstanceType("gpu", fake.WithOfferings(
		offeringInZone(v1.CapacityTypeOnDemand, "zone-a", 3, true),
		offeringInZone(v1.CapacityTypeOnDemand, "zone-b", 1, true),
		offeringInZone(v1.CapacityTypeSpot, "zone-c", 0.5, false),
		reserved,
	))
	instanceTypes := []*cloudprovider.InstanceType{it}
	nodeClaim := &NodeClaim{
		NodeClaimTemplate:    NodeClaimTemplate{InstanceTypeOptions: instanceTypes},
		daemonOverheadGroups: []DaemonOverheadGroup{{InstanceTypes: instanceTypes, HostPortUsage: scheduling.NewHostPortUsage()}},
		reservationManager:   NewReservationManager(map[string][]*cloudprovider.InstanceType{"default": instanceTypes}),
		hostname:             "hostname-placeholder-0001",
	}
	pod := gangPod()
	topology := zoneTopology(newZoneTopologyGroup(TopologyTypePodAffinity, pod), pod)
	ctx := karpopts.ToContext(context.Background(), &karpopts.Options{FeatureGates: karpopts.FeatureGates{ReservedCapacity: true}})
	prices := nodeClaim.domainPrices(ctx, pod, scheduling.NewRequirements(), corev1.ResourceList{}, instanceTypes)
	if got := bootstrappedZones(t, topology, pod, zoneRequirements(corev1.NodeSelectorOpExists), prices); !slices.Equal(got, []string{"zone-b"}) {
		t.Fatalf("expected zone-b, got %v", got)
	}
}

// Only instance types whose device allocation succeeded are priced.
func TestAllocatableInstanceTypes(t *testing.T) {
	a, b := fake.NewInstanceType("a"), fake.NewInstanceType("b")
	instanceTypes := []*cloudprovider.InstanceType{a, b}
	if got := allocatableInstanceTypes(instanceTypes, nil); !slices.Equal(got, instanceTypes) {
		t.Fatalf("expected every instance type without an allocation, got %v", instanceTypeNames(got))
	}
	result := &dynamicresources.AllocationResult{InstanceTypes: []dynamicresources.InstanceTypeID{unique.Make("b")}}
	if got := allocatableInstanceTypes(instanceTypes, result); !slices.Equal(got, []*cloudprovider.InstanceType{b}) {
		t.Fatalf("expected only b, got %v", instanceTypeNames(got))
	}
}

// A bootstrapping affinity chooses among the domains the pod's other topologies allow, so a cheaper domain that its
// anti-affinity rules out does not strand it.
func TestAffinityBootstrapRespectsOtherTopologies(t *testing.T) {
	pod := gangPod()
	affinity := newZoneTopologyGroup(TopologyTypePodAffinity, pod)
	antiAffinity := newZoneTopologyGroup(TopologyTypePodAntiAffinity, pod)
	antiAffinity.selector = labels.SelectorFromSet(labels.Set{"app": "other"})
	antiAffinity.rawSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}}
	antiAffinity.Record("zone-b")
	affinity.AddOwner(pod.UID)
	antiAffinity.AddOwner(pod.UID)
	topology := &Topology{topologyGroups: map[uint64]*TopologyGroup{affinity.Hash(): affinity, antiAffinity.Hash(): antiAffinity}}
	prices := staticPrices(map[string]float64{"zone-a": 3, "zone-b": 1, "zone-c": 2, "zone-d": 1.5})
	for range 20 {
		if got := bootstrappedZones(t, topology, pod, zoneRequirements(corev1.NodeSelectorOpExists), prices); !slices.Equal(got, []string{"zone-d"}) {
			t.Fatalf("expected the cheapest zone the anti-affinity allows, got %v", got)
		}
	}
}
