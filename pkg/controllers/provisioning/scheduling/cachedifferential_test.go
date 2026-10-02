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

// Differential test for the scheduler construction caches.
//
// Every memoization a consolidation pass installs (daemon overhead and existing-node ingredients,
// cross-pass daemon overhead groups, node requirements, domain groups, NodeClaim templates,
// reservation capacity, topology pod and node reads, topology pod counts, inverse anti-affinity)
// claims to be invisible: a scheduler built with them must be indistinguishable from one built
// without them. This file checks that claim end to end over seeded random clusters. For each
// candidate simulation of a pass it builds one scheduler under the pass's caches and one with no
// caches at all, from identical inputs, and compares a canonical rendering of everything
// construction produces: templates and their instance types, daemon overhead groups, existing
// nodes (taints, requirements, remaining resources), NodePool limits, reservation capacity, and the
// topology (domain universe, every topology group's counts, owners and empty domains, and the
// inverse anti-affinity groups). The cached scheduler is then solved so any write into shared
// cached state shows up as a mismatch in a later candidate. Between passes the world mutates the
// way a cluster does (node labels and taints, pods, NodePool edits and re-creation, instance type
// and offering changes, DaemonSet changes) while the cross-pass daemon overhead group store lives
// on, and a validation-style re-simulation inside a pass refreshes exactly the caches the
// disruption validator refreshes.
//
// Seeds: KRAFTSMAN_CACHE_DIFF_SEED pins the base seed; KRAFTSMAN_CACHE_DIFF_WORLDS sets how many
// worlds run (default 25).
package scheduling

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

const diffFamilyLabel = "example.com/family"

var diffZones = []string{"test-zone-1", "test-zone-2", "test-zone-3"}

// diffOffering and diffInstanceType are the provider-side spec the world mutates. Instance type
// objects are rebuilt from them for every pass, as a provider refresh would.
type diffOffering struct {
	zone, capacityType string
	price              float64
	available          bool
	reservationID      string
	reservationCap     int
}

type diffInstanceType struct {
	name      string
	cpu, mem  int64 // cores, GiB
	pods      int64
	family    string
	offerings []diffOffering
}

type diffWorld struct {
	rng           *rand.Rand
	catalog       []*diffInstanceType
	nodePools     []*v1.NodePool
	poolCatalog   map[string][]string // NodePool name -> instance type names the provider returns for it
	nodes         []*corev1.Node
	pods          []*corev1.Pod // bound (including DaemonSet-owned and terminal) and pending
	daemonSetPods []*corev1.Pod
	version       int
}

func (w *diffWorld) nextRV() string {
	w.version++
	return strconv.Itoa(w.version)
}

func newDiffWorld(rng *rand.Rand) *diffWorld {
	w := &diffWorld{rng: rng, poolCatalog: map[string][]string{}}
	for i := range 4 + rng.Intn(5) {
		w.catalog = append(w.catalog, w.randomInstanceType(fmt.Sprintf("it-%d", i)))
	}
	for i := range 1 + rng.Intn(3) {
		w.nodePools = append(w.nodePools, w.randomNodePool(fmt.Sprintf("pool-%d", i), types.UID(fmt.Sprintf("pool-%d-uid-1", i))))
	}
	for i := range 3 + rng.Intn(6) {
		w.nodes = append(w.nodes, w.randomNode(fmt.Sprintf("node-%d", i)))
	}
	for i := range rng.Intn(4) {
		w.daemonSetPods = append(w.daemonSetPods, w.randomDaemonSetPod(i))
	}
	for _, node := range w.nodes {
		w.bindDaemonPods(node)
		for range rng.Intn(5) {
			w.pods = append(w.pods, w.randomWorkloadPod(node.Name))
		}
	}
	for range rng.Intn(5) {
		w.pods = append(w.pods, w.randomWorkloadPod(""))
	}
	// a pod leaked onto a node that no longer exists, and a terminal pod
	if rng.Intn(3) == 0 {
		w.pods = append(w.pods, w.randomWorkloadPod("node-gone"))
	}
	if rng.Intn(3) == 0 && len(w.nodes) > 0 {
		p := w.randomWorkloadPod(w.nodes[0].Name)
		p.Status.Phase = corev1.PodSucceeded
		w.pods = append(w.pods, p)
	}
	return w
}

func (w *diffWorld) pick(values ...string) string { return values[w.rng.Intn(len(values))] }

func (w *diffWorld) randomInstanceType(name string) *diffInstanceType {
	cpu := []int64{1, 2, 4, 8, 16}[w.rng.Intn(5)]
	it := &diffInstanceType{name: name, cpu: cpu, mem: cpu * []int64{2, 4, 8}[w.rng.Intn(3)], pods: 10 + w.rng.Int63n(100), family: w.pick("a", "b")}
	it.offerings = w.randomOfferings(it)
	return it
}

func (w *diffWorld) randomOfferings(it *diffInstanceType) []diffOffering {
	var offerings []diffOffering
	base := float64(it.cpu)*0.04 + float64(it.mem)*0.005
	for _, zone := range diffZones {
		if w.rng.Intn(4) == 0 {
			continue
		}
		offerings = append(offerings,
			diffOffering{zone: zone, capacityType: v1.CapacityTypeOnDemand, price: base, available: w.rng.Intn(8) != 0},
			diffOffering{zone: zone, capacityType: v1.CapacityTypeSpot, price: base * (0.3 + 0.6*w.rng.Float64()), available: w.rng.Intn(5) != 0},
		)
		if w.rng.Intn(6) == 0 {
			offerings = append(offerings, diffOffering{zone: zone, capacityType: v1.CapacityTypeReserved, price: base * 0.1, available: true,
				reservationID: fmt.Sprintf("r-%s-%s", it.name, zone), reservationCap: w.rng.Intn(3)})
		}
	}
	if len(offerings) == 0 {
		offerings = append(offerings, diffOffering{zone: diffZones[0], capacityType: v1.CapacityTypeOnDemand, price: base, available: true})
	}
	return offerings
}

func (w *diffWorld) randomNodePool(name string, uid types.UID) *v1.NodePool {
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid, Generation: 1}})
	np.Spec.Weight = lo.ToPtr(w.rng.Int31n(50))
	w.randomizeNodePoolSpec(np)
	// The provider returns a pool-specific subset of the catalog.
	var names []string
	for _, it := range w.catalog {
		if w.rng.Intn(4) != 0 {
			names = append(names, it.name)
		}
	}
	if len(names) == 0 {
		names = []string{w.catalog[0].name}
	}
	w.poolCatalog[name] = names
	return np
}

func (w *diffWorld) randomizeNodePoolSpec(np *v1.NodePool) {
	var reqs []v1.NodeSelectorRequirementWithMinValues
	if w.rng.Intn(2) == 0 {
		zones := lo.Samples(diffZones, 1+w.rng.Intn(len(diffZones)))
		reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: zones})
	}
	if w.rng.Intn(3) == 0 {
		reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: diffFamilyLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{w.pick("a", "b")}})
	}
	switch w.rng.Intn(3) {
	case 0:
		reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand}})
	case 1:
		reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand, v1.CapacityTypeReserved}})
	}
	np.Spec.Template.Spec.Requirements = reqs
	np.Spec.Template.Spec.Taints = nil
	if w.rng.Intn(3) == 0 {
		np.Spec.Template.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: np.Name, Effect: corev1.TaintEffectNoSchedule}}
	}
	np.Spec.Template.Labels = map[string]string{"example.com/tier": w.pick("x", "y")}
	np.Spec.Limits = nil
	if w.rng.Intn(4) == 0 {
		np.Spec.Limits = v1.Limits{corev1.ResourceCPU: resource.MustParse(strconv.Itoa(8 + w.rng.Intn(40)))}
	}
}

func (w *diffWorld) catalogEntry(name string) *diffInstanceType {
	return lo.FindOrElse(w.catalog, nil, func(it *diffInstanceType) bool { return it.name == name })
}

func (w *diffWorld) randomNode(name string) *corev1.Node {
	np := w.nodePools[w.rng.Intn(len(w.nodePools))]
	it := w.catalogEntry(w.pick(w.poolCatalog[np.Name]...))
	labels := map[string]string{
		v1.NodePoolLabelKey:            np.Name,
		v1.NodeInitializedLabelKey:     "true",
		v1.NodeRegisteredLabelKey:      "true",
		corev1.LabelInstanceTypeStable: it.name,
		corev1.LabelTopologyZone:       w.pick(diffZones...),
		corev1.LabelHostname:           name,
		v1.CapacityTypeLabelKey:        w.pick(v1.CapacityTypeSpot, v1.CapacityTypeOnDemand),
		diffFamilyLabel:                it.family,
		"example.com/tier":             np.Spec.Template.Labels["example.com/tier"],
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), ResourceVersion: w.nextRV(), Labels: labels},
		Spec:       corev1.NodeSpec{ProviderID: "fake:///" + name, Taints: append([]corev1.Taint(nil), np.Spec.Template.Spec.Taints...)},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewQuantity(it.cpu, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(it.mem<<30, resource.BinarySI),
				corev1.ResourcePods:   *resource.NewQuantity(it.pods, resource.DecimalSI),
			},
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewQuantity(it.cpu, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(it.mem<<30, resource.BinarySI),
				corev1.ResourcePods:   *resource.NewQuantity(it.pods, resource.DecimalSI),
			},
		},
	}
}

// randomDaemonSetPod returns the pod the scheduler is handed for DaemonSet ds-<i>: a running pod of
// it, as getDaemonSetPods returns once the DaemonSet has one.
func (w *diffWorld) randomDaemonSetPod(i int) *corev1.Pod {
	name := fmt.Sprintf("ds-%d", i)
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-standin", Namespace: "kube-system",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: name, UID: types.UID(name + "-uid"), Controller: lo.ToPtr(true)}}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%dm", 50+50*w.rng.Intn(6))),
			corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", 64*(1+w.rng.Intn(4)))),
		}}}}},
	}
	w.randomizeDaemonSetPod(p)
	return p
}

func (w *diffWorld) randomizeDaemonSetPod(p *corev1.Pod) {
	p.Spec.NodeSelector = nil
	p.Spec.Tolerations = nil
	p.Spec.Containers[0].Ports = nil
	switch w.rng.Intn(4) {
	case 0:
		p.Spec.NodeSelector = map[string]string{diffFamilyLabel: w.pick("a", "b")}
	case 1:
		p.Spec.NodeSelector = map[string]string{corev1.LabelTopologyZone: w.pick(diffZones...)}
	}
	if w.rng.Intn(2) == 0 {
		p.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	}
	if w.rng.Intn(4) == 0 {
		p.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 9100, ContainerPort: 9100, Protocol: corev1.ProtocolTCP}}
	}
	p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse(fmt.Sprintf("%dm", 50+50*w.rng.Intn(6)))
}

// bindDaemonPods binds a running copy of each compatible DaemonSet pod to the node, so the node's
// tracked DaemonSet requests are non-trivial.
func (w *diffWorld) bindDaemonPods(node *corev1.Node) {
	for _, ds := range w.daemonSetPods {
		if scheduling.Taints(node.Spec.Taints).ToleratesPod(ds) != nil ||
			scheduling.NewLabelRequirements(node.Labels).Compatible(scheduling.NewStrictPodRequirements(ds)) != nil || w.rng.Intn(4) == 0 {
			continue
		}
		p := ds.DeepCopy()
		p.Name = ds.OwnerReferences[0].Name + "-" + node.Name
		p.UID = types.UID(p.Name + "-uid")
		p.ResourceVersion = w.nextRV()
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: ds.OwnerReferences[0].Name, UID: ds.OwnerReferences[0].UID, Controller: lo.ToPtr(true)}}
		p.Spec.NodeName = node.Name
		p.Status.Phase = corev1.PodRunning
		w.pods = append(w.pods, p)
	}
}

var diffPodSeq int

//nolint:gocyclo
func (w *diffWorld) randomWorkloadPod(nodeName string) *corev1.Pod {
	diffPodSeq++
	name := fmt.Sprintf("pod-%d", diffPodSeq)
	app := w.pick("a", "b", "c")
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: w.pick("default", "other"), UID: types.UID(name + "-uid"), ResourceVersion: w.nextRV(),
			Labels: map[string]string{"app": app, "rev": w.pick("1", "2")}},
		Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%dm", 100*(1+w.rng.Intn(15)))),
			corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", 128*(1+w.rng.Intn(16)))),
		}}}}},
	}
	if nodeName != "" {
		p.Status.Phase = corev1.PodRunning
	} else {
		p.Status.Phase = corev1.PodPending
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	}
	selector := func() *metav1.LabelSelector {
		switch w.rng.Intn(5) {
		case 0:
			return &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{w.pick("a", "b", "c")}}}}
		case 1:
			return &metav1.LabelSelector{}
		default:
			return &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}}
		}
	}
	if w.rng.Intn(3) == 0 {
		p.Spec.NodeSelector = map[string]string{diffFamilyLabel: w.pick("a", "b")}
	}
	if w.rng.Intn(3) == 0 {
		p.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
	}
	if w.rng.Intn(2) == 0 {
		tsc := corev1.TopologySpreadConstraint{
			MaxSkew: 1 + w.rng.Int31n(2), TopologyKey: w.pick(corev1.LabelTopologyZone, corev1.LabelHostname),
			WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: selector(),
		}
		if w.rng.Intn(3) == 0 {
			tsc.NodeTaintsPolicy = lo.ToPtr(corev1.NodeInclusionPolicyHonor)
		}
		if w.rng.Intn(3) == 0 {
			tsc.NodeAffinityPolicy = lo.ToPtr(corev1.NodeInclusionPolicyIgnore)
		}
		if w.rng.Intn(3) == 0 {
			tsc.MatchLabelKeys = []string{"rev"}
		}
		p.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{tsc}
	}
	if w.rng.Intn(3) == 0 {
		term := corev1.PodAffinityTerm{TopologyKey: w.pick(corev1.LabelHostname, corev1.LabelTopologyZone), LabelSelector: selector()}
		if w.rng.Intn(3) == 0 {
			term.Namespaces = []string{"default", "other"}
		}
		p.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{term}}}
	}
	return p
}

// mutate applies one random cluster change between passes, bumping the versions the real API
// server and provider would bump.
//
//nolint:gocyclo
func (w *diffWorld) mutate() string {
	switch w.rng.Intn(11) {
	case 0:
		n := w.nodes[w.rng.Intn(len(w.nodes))]
		n.Labels[corev1.LabelTopologyZone] = w.pick(diffZones...)
		n.Labels[diffFamilyLabel] = w.pick("a", "b")
		n.ResourceVersion = w.nextRV()
		return "node labels " + n.Name
	case 1:
		n := w.nodes[w.rng.Intn(len(w.nodes))]
		if len(n.Spec.Taints) == 0 {
			n.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "x", Effect: corev1.TaintEffectNoSchedule}}
		} else {
			n.Spec.Taints = nil
		}
		n.ResourceVersion = w.nextRV()
		return "node taints " + n.Name
	case 2:
		n := w.nodes[w.rng.Intn(len(w.nodes))]
		w.pods = append(w.pods, w.randomWorkloadPod(n.Name))
		return "pod bound to " + n.Name
	case 3:
		bound := lo.Filter(w.pods, func(p *corev1.Pod, _ int) bool { return p.Spec.NodeName != "" })
		if len(bound) == 0 {
			return "noop"
		}
		victim := bound[w.rng.Intn(len(bound))]
		w.pods = lo.Without(w.pods, victim)
		return "pod deleted " + victim.Name
	case 4:
		w.pods = append(w.pods, w.randomWorkloadPod(""))
		return "pending pod added"
	case 5:
		np := w.nodePools[w.rng.Intn(len(w.nodePools))]
		w.randomizeNodePoolSpec(np)
		np.Generation++
		return "nodepool edited " + np.Name
	case 6:
		i := w.rng.Intn(len(w.nodePools))
		old := w.nodePools[i]
		recreated := w.randomNodePool(old.Name, types.UID(fmt.Sprintf("%s-uid-%d", old.Name, w.version)))
		w.nextRV()
		w.nodePools[i] = recreated
		return "nodepool recreated " + old.Name
	case 7:
		it := w.catalog[w.rng.Intn(len(w.catalog))]
		it.family = w.pick("a", "b")
		it.pods = 10 + w.rng.Int63n(100)
		return "instance type changed " + it.name
	case 8:
		it := w.catalog[w.rng.Intn(len(w.catalog))]
		it.offerings = w.randomOfferings(it)
		return "offerings changed " + it.name
	case 9:
		if len(w.daemonSetPods) == 0 {
			w.daemonSetPods = append(w.daemonSetPods, w.randomDaemonSetPod(0))
			return "daemonset added"
		}
		if w.rng.Intn(2) == 0 {
			return w.swapDaemonSetStandIn()
		}
		w.randomizeDaemonSetPod(w.daemonSetPods[w.rng.Intn(len(w.daemonSetPods))])
		return "daemonset changed"
	default:
		live := lo.Filter(w.pods, func(p *corev1.Pod, _ int) bool { return p.Spec.NodeName != "" && len(p.OwnerReferences) == 0 })
		if len(live) == 0 {
			return "noop"
		}
		p := live[w.rng.Intn(len(live))]
		p.Labels["app"] = w.pick("a", "b", "c")
		p.ResourceVersion = w.nextRV()
		return "pod relabeled " + p.Name
	}
}

// swapDaemonSetStandIn replaces a DaemonSet's stand-in pod with another running pod of the same
// DaemonSet, as happens whenever a newer node joins: a different name and UID, another node binding
// with the DaemonSet controller's node-pinning matchFields term, and a per-pod token volume.
func (w *diffWorld) swapDaemonSetStandIn() string {
	i := w.rng.Intn(len(w.daemonSetPods))
	p := w.daemonSetPods[i].DeepCopy()
	w.version++
	node := w.nodes[w.rng.Intn(len(w.nodes))].Name
	p.Name = fmt.Sprintf("%s-%d", p.OwnerReferences[0].Name, w.version)
	p.UID = types.UID(p.Name + "-uid")
	p.ResourceVersion = strconv.Itoa(w.version)
	p.Spec.NodeName = node
	token := fmt.Sprintf("kube-api-access-%d", w.version)
	p.Spec.Volumes = []corev1.Volume{{Name: token, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}}}}
	p.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: token, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount"}}
	p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node}}}}},
	}}}
	w.daemonSetPods[i] = p
	return "daemonset stand-in swapped " + p.Name
}

// diffPassInputs is one pass's view of the world: the objects the API server serves, cluster
// state built from them, and the provider's instance types and revisions.
type diffPassInputs struct {
	kubeClient    client.Client
	cluster       *state.Cluster
	instanceTypes map[string][]*cloudprovider.InstanceType
	revisions     map[string]uint64
}

//nolint:gocyclo
func (w *diffWorld) passInputs(t *testing.T) diffPassInputs {
	t.Helper()
	ctx := context.Background()
	var objs []client.Object
	for _, n := range w.nodes {
		objs = append(objs, n.DeepCopy())
	}
	for _, p := range w.pods {
		objs = append(objs, p.DeepCopy())
	}
	for _, ns := range []string{"default", "other", "kube-system"} {
		objs = append(objs, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: map[string]string{"name": ns}}})
	}
	kubeClient := fakecr.NewClientBuilder().WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
		Build()
	cluster := state.NewCluster(clock.NewFakeClock(time.Now()), kubeClient, fake.NewCloudProvider())
	for _, n := range w.nodes {
		if err := cluster.UpdateNode(ctx, n.DeepCopy()); err != nil {
			t.Fatalf("updating node: %v", err)
		}
	}
	for _, p := range w.pods {
		if p.Spec.NodeName == "" || p.Spec.NodeName == "node-gone" {
			continue
		}
		if err := cluster.UpdatePod(ctx, p.DeepCopy()); err != nil {
			t.Fatalf("updating pod: %v", err)
		}
	}
	in := diffPassInputs{kubeClient: kubeClient, cluster: cluster, instanceTypes: map[string][]*cloudprovider.InstanceType{}, revisions: map[string]uint64{}}
	for _, np := range w.nodePools {
		var its []*cloudprovider.InstanceType
		h := fnv.New64a()
		for _, name := range w.poolCatalog[np.Name] {
			spec := w.catalogEntry(name)
			its = append(its, spec.build())
			fmt.Fprintf(h, "%+v;", *spec)
		}
		in.instanceTypes[np.Name] = its
		// An honest revision: it changes whenever the content returned for the NodePool changes.
		in.revisions[np.Name] = h.Sum64() | 1
	}
	return in
}

func (spec *diffInstanceType) build() *cloudprovider.InstanceType {
	offerings := lo.Map(spec.offerings, func(o diffOffering, _ int) *cloudprovider.Offering {
		labels := map[string]string{v1.CapacityTypeLabelKey: o.capacityType, corev1.LabelTopologyZone: o.zone}
		if o.reservationID != "" {
			labels[cloudprovider.ReservationIDLabel] = o.reservationID
		}
		return &cloudprovider.Offering{Requirements: scheduling.NewLabelRequirements(labels), Price: o.price, Available: o.available, ReservationCapacity: o.reservationCap}
	})
	return fake.NewInstanceType(spec.name,
		fake.WithResources(corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewQuantity(spec.cpu, resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(spec.mem<<30, resource.BinarySI),
			corev1.ResourcePods:   *resource.NewQuantity(spec.pods, resource.DecimalSI),
		}),
		fake.WithOfferings(lo.FromSlicePtr(offerings)...),
		fake.WithRequirements(scheduling.NewRequirement(diffFamilyLabel, corev1.NodeSelectorOpIn, spec.family)),
	)
}

// diffPassCaches is the set of caches one consolidation pass installs, mirroring
// SingleNodeConsolidation.ComputeCommands.
type diffPassCaches struct {
	daemonOverhead *DaemonOverheadCache
	domainGroups   *DomainGroupCache
	nodeReqs       *NodeRequirementsCache
	reservations   *ReservationCapacityCache
	templates      *NodeClaimTemplateCache
	topologyReads  *TopologyPassCache
	inverse        *InverseAffinityCache
}

func newDiffPassCaches(store *DaemonOverheadGroupStore) *diffPassCaches {
	return &diffPassCaches{
		daemonOverhead: NewDaemonOverheadCacheWithGroupStore(store),
		domainGroups:   NewDomainGroupCache(),
		nodeReqs:       NewNodeRequirementsCache(),
		reservations:   NewReservationCapacityCache(),
		templates:      NewNodeClaimTemplateCache(),
		topologyReads:  NewTopologyPassCache(),
		inverse:        NewInverseAffinityCache(),
	}
}

// refreshForValidation mirrors what the disruption validator does before its re-simulation:
// fresh topology reads and inverse anti-affinity, and the state-derived existing-node entries
// dropped. Everything else carries over from the pass.
func (c *diffPassCaches) refreshForValidation() {
	c.topologyReads = NewTopologyPassCache()
	c.inverse = NewInverseAffinityCache()
	c.daemonOverhead.DropStateDerived()
}

func (c *diffPassCaches) context(ctx context.Context) context.Context {
	ctx = WithDaemonOverheadCache(ctx, c.daemonOverhead)
	ctx = WithDomainGroupCache(ctx, c.domainGroups)
	ctx = WithNodeRequirementsCache(ctx, c.nodeReqs)
	ctx = WithReservationCapacityCache(ctx, c.reservations)
	ctx = WithNodeClaimTemplateCache(ctx, c.templates)
	ctx = WithTopologyPassCache(ctx, c.topologyReads)
	return WithInverseAffinityCache(ctx, c.inverse)
}

// diffCandidate is one simulation: the nodes it removes and an optional split price limit.
type diffCandidate struct {
	nodes      []string
	priceLimit float64
}

// buildScheduler constructs a scheduler for the candidate exactly as a consolidation simulation
// would: the candidate's nodes left out of the state nodes, and its reschedulable pods plus the
// pending pods excluded from topology counts and handed to the scheduler.
func (w *diffWorld) buildScheduler(t *testing.T, ctx context.Context, in diffPassInputs, candidate diffCandidate) (*Scheduler, []*corev1.Pod) {
	t.Helper()
	removed := sets.New(candidate.nodes...)
	stateNodes := lo.Filter(in.cluster.SimulationCopyNodes(), func(n *state.StateNode, _ int) bool { return !removed.Has(n.Name()) })
	sort.Slice(stateNodes, func(i, j int) bool { return stateNodes[i].Name() < stateNodes[j].Name() })
	var pods []*corev1.Pod
	for _, p := range w.pods {
		if p.Status.Phase == corev1.PodSucceeded || len(p.OwnerReferences) > 0 {
			continue
		}
		if p.Spec.NodeName == "" || removed.Has(p.Spec.NodeName) {
			pods = append(pods, p)
		}
	}
	nodePools := lo.Map(w.nodePools, func(np *v1.NodePool, _ int) *v1.NodePool { return np.DeepCopy() })
	ctx = WithInstanceTypeRevisions(ctx, in.revisions)
	topology, err := NewTopology(ctx, in.kubeClient, in.cluster, stateNodes, nodePools, in.instanceTypes, pods)
	if err != nil {
		t.Fatalf("building topology: %v", err)
	}
	s := NewScheduler(ctx, in.kubeClient, nodePools, in.cluster, stateNodes, topology, in.instanceTypes, w.daemonSetPods,
		events.NewRecorder(&record.FakeRecorder{}), clock.NewFakeClock(time.Now()), nil, nil,
		IsConsolidationSimulation, NewNodeClaimPriceLimit(candidate.priceLimit))
	return s, pods
}

func renderRequirements(reqs scheduling.Requirements) string {
	parts := lo.Map(reqs.Values(), func(r *scheduling.Requirement, _ int) string {
		values := r.Values()
		sort.Strings(values)
		s := fmt.Sprintf("%s %s %v", r.Key, r.Operator(), values)
		if r.MinValues != nil {
			s += fmt.Sprintf(" min=%d", *r.MinValues)
		}
		return s
	})
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

func renderResources(rl corev1.ResourceList) string {
	keys := lo.Keys(rl)
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return strings.Join(lo.Map(keys, func(k corev1.ResourceName, _ int) string {
		q := rl[k]
		return fmt.Sprintf("%s=%d", k, q.MilliValue())
	}), ",")
}

// renderHostPorts renders the ports a HostPortUsage reserves. The reserving pods' keys are left out:
// they only exempt a pod from conflicting with its own ports, and a daemon pod is never the pod
// being scheduled, so two usages reserving the same ports under different daemon pod names are
// equivalent.
func renderHostPorts(u *scheduling.HostPortUsage) string {
	var ports []string
	// HostPortUsage exposes no accessor for what it reserves; read the map through reflection.
	for it := reflect.ValueOf(u).Elem().FieldByName("reserved").MapRange(); it.Next(); {
		ports = append(ports, fmt.Sprint(it.Value()))
	}
	sort.Strings(ports)
	return fmt.Sprint(ports)
}

func renderTopologyGroup(tg *TopologyGroup) string {
	owners := lo.Map(lo.Keys(tg.owners), func(u types.UID, _ int) string { return string(u) })
	sort.Strings(owners)
	domains := lo.Map(lo.Entries(tg.domains), func(e lo.Entry[string, int32], _ int) string { return fmt.Sprintf("%s:%d", e.Key, e.Value) })
	sort.Strings(domains)
	selector := "<nil>"
	if tg.selector != nil {
		selector = tg.selector.String()
	}
	return fmt.Sprintf("%s %s skew=%d minDomains=%v ns=%v sel=%q filter=%x owners=%v domains=%v empty=%v",
		tg.Type, tg.Key, tg.maxSkew, lo.FromPtrOr(tg.minDomains, -1), sets.List(tg.namespaces), selector, hashNodeFilter(tg.nodeFilter),
		owners, domains, sets.List(tg.emptyDomains))
}

// snapshotScheduler renders everything construction produced, in a canonical order, so two
// schedulers built from identical inputs render identically.
func snapshotScheduler(s *Scheduler) []string {
	var out []string
	for _, nct := range s.nodeClaimTemplates {
		its := lo.Map(nct.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
		sort.Strings(its)
		out = append(out, fmt.Sprintf("template %s weight=%d policy=%v its=%v reqs={%s} taints=%v labels=%v annotations=%v",
			nct.NodePoolName, nct.NodePoolWeight, nct.PackingPolicy, its, renderRequirements(nct.Requirements), nct.Spec.Taints, nct.Labels, nct.Annotations))
		groups := lo.Map(s.daemonOverheadGroups[nct], func(g DaemonOverheadGroup, _ int) string {
			names := lo.Map(g.InstanceTypes, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
			sort.Strings(names)
			return fmt.Sprintf("%v overhead=%s ports=%s", names, renderResources(g.DaemonOverhead), renderHostPorts(g.HostPortUsage))
		})
		sort.Strings(groups)
		for _, g := range groups {
			out = append(out, "  daemon group "+g)
		}
	}
	for _, n := range s.existingNodes {
		itName := "<nil>"
		if n.instanceType != nil {
			itName = n.instanceType.Name
		}
		out = append(out, fmt.Sprintf("existing %s it=%s taints=%v reqs={%s} remaining=%s consolidateAfter=%t",
			n.Name(), itName, n.cachedTaints, renderRequirements(n.requirements), renderResources(n.remainingResources), n.isUnderConsolidateAfter))
	}
	pools := lo.Keys(s.remainingResources)
	sort.Strings(pools)
	for _, pool := range pools {
		out = append(out, fmt.Sprintf("limits %s %s", pool, renderResources(s.remainingResources[pool])))
	}
	reservations := lo.Map(lo.Entries(s.reservationManager.capacity), func(e lo.Entry[string, int], _ int) string { return fmt.Sprintf("%s:%d", e.Key, e.Value) })
	sort.Strings(reservations)
	out = append(out, fmt.Sprintf("reservations %v", reservations))
	topologyKeys := lo.Keys(s.topology.domainGroups)
	sort.Strings(topologyKeys)
	for _, key := range topologyKeys {
		group := s.topology.domainGroups[key]
		domains := lo.Keys(group)
		sort.Strings(domains)
		for _, domain := range domains {
			sources := lo.Map(lo.Entries(group[domain]), func(e lo.Entry[string, TopologyDomainSource], _ int) string {
				return fmt.Sprintf("%s taints=%v reqs={%s}", e.Key, e.Value.Taints, renderRequirements(e.Value.Requirements))
			})
			sort.Strings(sources)
			out = append(out, fmt.Sprintf("domain %s=%s %v", key, domain, sources))
		}
	}
	var groups []string
	for _, tg := range s.topology.topologyGroups {
		groups = append(groups, "topology "+renderTopologyGroup(tg))
	}
	for _, tg := range s.topology.inverseTopologyGroups {
		groups = append(groups, "inverse "+renderTopologyGroup(tg))
	}
	sort.Strings(groups)
	return append(out, groups...)
}

func diffSnapshots(cached, uncached []string) string {
	if strings.Join(cached, "\n") == strings.Join(uncached, "\n") {
		return ""
	}
	c, u := sets.New(cached...), sets.New(uncached...)
	return fmt.Sprintf("only cached:\n  %s\nonly uncached:\n  %s", strings.Join(sets.List(c.Difference(u)), "\n  "), strings.Join(sets.List(u.Difference(c)), "\n  "))
}

func diffEnv(name string, fallback int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(name), 10, 64); err == nil {
		return v
	}
	return fallback
}

// TestSchedulerConstructionCachesMatchUncached is the differential check described in the file
// header.
func TestSchedulerConstructionCachesMatchUncached(t *testing.T) {
	baseSeed := diffEnv("KRAFTSMAN_CACHE_DIFF_SEED", time.Now().UnixNano())
	worlds := diffEnv("KRAFTSMAN_CACHE_DIFF_WORLDS", 25)
	t.Logf("base seed %d (rerun with KRAFTSMAN_CACHE_DIFF_SEED=%d)", baseSeed, baseSeed)
	failures := 0
	for world := range worlds {
		seed := baseSeed + world
		if msg := runCacheDifferentialWorld(t, seed); msg != "" {
			failures++
			t.Errorf("seed %d: %s", seed, msg)
			if failures >= 3 {
				return
			}
		}
	}
}

// runCacheDifferentialWorld runs several passes over one seeded world and returns a description
// of the first mismatch, or "" when cached and uncached construction agree everywhere.
func runCacheDifferentialWorld(t *testing.T, seed int64) string {
	t.Helper()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec
	w := newDiffWorld(rng)
	store := NewDaemonOverheadGroupStore()
	cachedOpts := test.Options(test.OptionsFields{TopologyCountCacheMode: lo.ToPtr(karpopts.TopologyCountCacheModeOn)})
	uncachedOpts := test.Options(test.OptionsFields{TopologyCountCacheMode: lo.ToPtr(karpopts.TopologyCountCacheModeOff)})
	var history []string
	for pass := range 4 {
		if pass > 0 {
			for range 1 + rng.Intn(2) {
				history = append(history, fmt.Sprintf("pass %d: %s", pass, w.mutate()))
			}
		}
		in := w.passInputs(t)
		caches := newDiffPassCaches(store)
		for step := range 4 {
			if step == 3 {
				// validation-style re-simulation after churn inside the pass
				history = append(history, fmt.Sprintf("pass %d validation after: %s", pass, w.mutate()))
				in = w.passInputs(t)
				caches.refreshForValidation()
			}
			candidate := diffCandidate{nodes: []string{w.nodes[rng.Intn(len(w.nodes))].Name}}
			if rng.Intn(4) == 0 {
				candidate.nodes = append(candidate.nodes, w.nodes[rng.Intn(len(w.nodes))].Name)
			}
			if rng.Intn(3) == 0 {
				candidate.priceLimit = 0.05 + rng.Float64()*0.5
			}
			cachedCtx := caches.context(karpopts.ToContext(context.Background(), cachedOpts))
			uncachedCtx := karpopts.ToContext(context.Background(), uncachedOpts)
			cached, pods := w.buildScheduler(t, cachedCtx, in, candidate)
			uncached, _ := w.buildScheduler(t, uncachedCtx, in, candidate)
			if diff := diffSnapshots(snapshotScheduler(cached), snapshotScheduler(uncached)); diff != "" {
				return fmt.Sprintf("pass %d step %d candidate %+v differs\nhistory:\n  %s\n%s", pass, step, candidate, strings.Join(history, "\n  "), diff)
			}
			// Solving writes into everything the scheduler owns; a write into shared cached
			// state surfaces as a mismatch in a later candidate. The decisions themselves must
			// match too, up to the ties topology breaks by map iteration order.
			cachedResults, err := cached.Solve(cachedCtx, copyPods(pods))
			if err != nil {
				return fmt.Sprintf("solve: %v", err)
			}
			if msg := w.compareDecisions(t, cachedCtx, uncachedCtx, in, candidate, renderResults(cachedResults), uncached, pods); msg != "" {
				return fmt.Sprintf("pass %d step %d candidate %+v: %s\nhistory:\n  %s", pass, step, candidate, msg, strings.Join(history, "\n  "))
			}
		}
	}
	return ""
}

func copyPods(pods []*corev1.Pod) []*corev1.Pod {
	return lo.Map(pods, func(p *corev1.Pod, _ int) *corev1.Pod { return p.DeepCopy() })
}

// renderResults renders a Solve's decisions: where each pod went (an existing node, or a new
// NodeClaim described by its NodePool, its pods and its instance type options) and which pods failed,
// and whether each failure is pass-invariant.
func renderResults(r Results) string {
	var out []string
	for _, n := range r.ExistingNodes {
		for _, p := range n.Pods {
			out = append(out, fmt.Sprintf("%s -> existing %s", p.Name, n.Name()))
		}
	}
	for _, nc := range r.NewNodeClaims {
		pods := lo.Map(nc.Pods, func(p *corev1.Pod, _ int) string { return p.Name })
		sort.Strings(pods)
		its := lo.Map(nc.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
		sort.Strings(its)
		out = append(out, fmt.Sprintf("new %s pods=%v its=%v", nc.NodePoolName, pods, its))
	}
	for p, err := range r.PodErrors {
		out = append(out, fmt.Sprintf("%s -> error invariant=%t", p.Name, IsIncompatibleWithAllNodePools(err)))
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// compareDecisions compares the cached scheduler's decisions with the uncached scheduler's.
// Topology spread breaks ties between equally loaded domains by map iteration order, so either side
// can legitimately produce several outcomes from the same inputs. Both sides are rebuilt and solved
// afresh, up to ten times each, and they agree as soon as one outcome appears on both; a mismatch is
// reported only when the two outcome sets never meet.
func (w *diffWorld) compareDecisions(t *testing.T, cachedCtx, uncachedCtx context.Context, in diffPassInputs, candidate diffCandidate, cachedFirst string, uncached *Scheduler, pods []*corev1.Pod) string {
	t.Helper()
	cachedSeen, uncachedSeen := sets.New(cachedFirst), sets.New[string]()
	solve := func(ctx context.Context, s *Scheduler) (string, error) {
		results, err := s.Solve(ctx, copyPods(pods))
		if err != nil {
			return "", err
		}
		return renderResults(results), nil
	}
	for attempt := range 10 {
		if attempt > 0 {
			uncached, _ = w.buildScheduler(t, uncachedCtx, in, candidate)
		}
		rendered, err := solve(uncachedCtx, uncached)
		if err != nil {
			return fmt.Sprintf("uncached solve: %v", err)
		}
		if uncachedSeen.Insert(rendered); cachedSeen.Has(rendered) {
			return ""
		}
		cached, _ := w.buildScheduler(t, cachedCtx, in, candidate)
		if rendered, err = solve(cachedCtx, cached); err != nil {
			return fmt.Sprintf("cached solve: %v", err)
		}
		if cachedSeen.Insert(rendered); uncachedSeen.Has(rendered) {
			return ""
		}
	}
	return fmt.Sprintf("decisions differ\ncached (%d distinct):\n%s\nuncached (%d distinct):\n%s",
		cachedSeen.Len(), strings.Join(sets.List(cachedSeen), "\n---\n"), uncachedSeen.Len(), strings.Join(sets.List(uncachedSeen), "\n---\n"))
}
