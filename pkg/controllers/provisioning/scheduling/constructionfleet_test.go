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

// A production-shaped fleet for construction benchmarks: a few dozen NodePools over a cloud-provider
// style instance type catalog of several hundred types carrying the requirement keys a real provider
// sets, and a DaemonSet mix whose node selection mirrors a typical cluster (a CNI and node exporters on
// every Linux node, GPU agents behind a GPU label, storage agents behind dedicated pool labels, agents
// excluded from virtual or system nodes through NotIn terms, and per-NodePool agents).

package scheduling

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

const (
	fleetLabelCategory      = "example.com/instance-category"
	fleetLabelFamily        = "example.com/instance-family"
	fleetLabelGeneration    = "example.com/instance-generation"
	fleetLabelSize          = "example.com/instance-size"
	fleetLabelCPU           = "example.com/instance-cpu"
	fleetLabelMemory        = "example.com/instance-memory"
	fleetLabelGPUName       = "example.com/instance-gpu-name"
	fleetLabelGPUCount      = "example.com/instance-gpu-count"
	fleetLabelLocalNVMe     = "example.com/instance-local-nvme"
	fleetLabelHypervisor    = "example.com/instance-hypervisor"
	fleetLabelNetwork       = "example.com/instance-network-bandwidth"
	fleetLabelEncryption    = "example.com/instance-encryption-in-transit"
	fleetLabelZoneID        = "example.com/zone-id"
	fleetLabelComputeType   = "example.com/compute-type"
	fleetLabelNodeType      = "example.com/node-type"
	fleetLabelDiskType      = "example.com/disk-type"
	fleetLabelStorageNode   = "example.com/storage-node-type"
	fleetLabelSandbox       = "example.com/sandbox-runtime"
	fleetLabelVirtual       = "example.com/virtual-node"
	fleetLabelNoCNI         = "example.com/no-cni"
	fleetLabelSharingDaemon = "example.com/gpu-sharing"
	fleetTaintGPU           = "example.com/gpu"
	fleetTaintSandbox       = "example.com/sandbox"
	fleetTaintStorage       = "example.com/storage"
)

var fleetZones = []string{"zone-1a", "zone-1b", "zone-1c", "zone-1d"}

// constructionFleet is the scheduler construction input for a production-shaped cluster.
type constructionFleet struct {
	nodePools     []*v1.NodePool
	instanceTypes map[string][]*cloudprovider.InstanceType
	daemonSetPods []*corev1.Pod
}

// newConstructionFleet builds pools NodePools that all resolve the same catalog of roughly
// instanceTypes instance types, and the DaemonSet pod set described in the file header.
func newConstructionFleet(pools, instanceTypes int) constructionFleet {
	catalog := fleetInstanceTypes(instanceTypes)
	fleet := constructionFleet{instanceTypes: map[string][]*cloudprovider.InstanceType{}}
	for i := range pools {
		np := fleetNodePool(i)
		fleet.nodePools = append(fleet.nodePools, np)
		fleet.instanceTypes[np.Name] = catalog
	}
	fleet.daemonSetPods = fleetDaemonSetPods(fleet.nodePools)
	return fleet
}

// templates builds the fleet's NodeClaimTemplates the way NewScheduler does, without a cache.
func (f constructionFleet) templates(ctx context.Context) []*NodeClaimTemplate {
	recorder := events.NewRecorder(&record.FakeRecorder{})
	var templates []*NodeClaimTemplate
	for _, np := range f.nodePools {
		if nct, ok := nodeClaimTemplateForNodePool(ctx, np, f.instanceTypes[np.Name], karpopts.MinValuesPolicyStrict, 0, recorder); ok {
			templates = append(templates, nct)
		}
	}
	return templates
}

// fleetInstanceTypes returns about n instance types across general purpose, compute, memory, GPU and
// storage categories, each with the requirement keys a cloud provider sets and an offering per zone and
// capacity type.
func fleetInstanceTypes(n int) []*cloudprovider.InstanceType {
	type category struct {
		name      string
		memPerCPU int
		gpu       string
		nvme      bool
	}
	categories := []category{{"c", 2, "", false}, {"m", 4, "", false}, {"r", 8, "", false}, {"x", 16, "", false}, {"i", 8, "", true}, {"g", 4, "gpu-a", true}, {"p", 8, "gpu-b", true}, {"t", 4, "", false}}
	sizes := []struct {
		name string
		cpu  int
	}{{"large", 2}, {"xlarge", 4}, {"2xlarge", 8}, {"4xlarge", 16}, {"8xlarge", 32}, {"12xlarge", 48}, {"16xlarge", 64}, {"24xlarge", 96}, {"48xlarge", 192}}
	variants := []string{"", "d", "n", "dn", "a", "g"}
	var its []*cloudprovider.InstanceType
	for generation := 5; len(its) < n; generation++ {
		for _, c := range categories {
			for _, variant := range variants {
				for _, size := range sizes {
					if len(its) >= n {
						return its
					}
					arch := lo.Ternary(variant == "g", "arm64", "amd64")
					family := fmt.Sprintf("%s%d%s", c.name, generation, variant)
					name := family + "." + size.name
					memory := size.cpu * c.memPerCPU
					gpus := 0
					if c.gpu != "" {
						gpus = max(1, size.cpu/16)
					}
					its = append(its, fleetInstanceType(name, family, c.name, generation, size.name, size.cpu, memory, arch, c.gpu, gpus, c.nvme || variant == "d" || variant == "dn"))
				}
			}
		}
	}
	return its
}

func fleetInstanceType(name, family, category string, generation int, size string, cpu, memoryGi int, arch, gpu string, gpus int, nvme bool) *cloudprovider.InstanceType {
	price := 0.05*float64(cpu) + 0.005*float64(memoryGi) + 0.9*float64(gpus)
	var offerings cloudprovider.Offerings
	for _, zone := range fleetZones {
		for _, capacityType := range []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand} {
			offerings = append(offerings, &cloudprovider.Offering{
				Available: true,
				Price:     lo.Ternary(capacityType == v1.CapacityTypeSpot, price*0.4, price),
				Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityType),
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
				),
			})
		}
	}
	gpuName := scheduling.NewRequirement(fleetLabelGPUName, corev1.NodeSelectorOpDoesNotExist)
	gpuCount := scheduling.NewRequirement(fleetLabelGPUCount, corev1.NodeSelectorOpDoesNotExist)
	if gpu != "" {
		gpuName = scheduling.NewRequirement(fleetLabelGPUName, corev1.NodeSelectorOpIn, gpu)
		gpuCount = scheduling.NewRequirement(fleetLabelGPUCount, corev1.NodeSelectorOpIn, fmt.Sprint(gpus))
	}
	localNVMe := scheduling.NewRequirement(fleetLabelLocalNVMe, corev1.NodeSelectorOpDoesNotExist)
	if nvme {
		localNVMe = scheduling.NewRequirement(fleetLabelLocalNVMe, corev1.NodeSelectorOpIn, fmt.Sprint(cpu*50))
	}
	requirements := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, name),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, arch),
		scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, string(corev1.Linux), string(corev1.Windows)),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, fleetZones...),
		scheduling.NewRequirement(fleetLabelZoneID, corev1.NodeSelectorOpIn, "zid-0", "zid-1", "zid-2", "zid-3"),
		scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot, v1.CapacityTypeOnDemand),
		scheduling.NewRequirement(fleetLabelCategory, corev1.NodeSelectorOpIn, category),
		scheduling.NewRequirement(fleetLabelFamily, corev1.NodeSelectorOpIn, family),
		scheduling.NewRequirement(fleetLabelGeneration, corev1.NodeSelectorOpIn, fmt.Sprint(generation)),
		scheduling.NewRequirement(fleetLabelSize, corev1.NodeSelectorOpIn, size),
		scheduling.NewRequirement(fleetLabelCPU, corev1.NodeSelectorOpIn, fmt.Sprint(cpu)),
		scheduling.NewRequirement(fleetLabelMemory, corev1.NodeSelectorOpIn, fmt.Sprint(memoryGi*1024)),
		scheduling.NewRequirement(fleetLabelHypervisor, corev1.NodeSelectorOpIn, "nitro"),
		scheduling.NewRequirement(fleetLabelNetwork, corev1.NodeSelectorOpIn, fmt.Sprint(cpu*625)),
		scheduling.NewRequirement(fleetLabelEncryption, corev1.NodeSelectorOpIn, "true"),
		gpuName, gpuCount, localNVMe,
	)
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(fmt.Sprint(cpu)),
		corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memoryGi)),
		corev1.ResourcePods:   resource.MustParse(fmt.Sprint(min(737, 8+cpu*8))),
	}
	if gpus > 0 {
		capacity["example.com/gpu"] = resource.MustParse(fmt.Sprint(gpus))
	}
	return &cloudprovider.InstanceType{
		Name:         name,
		Requirements: requirements,
		Offerings:    offerings,
		Capacity:     capacity,
		Overhead: &cloudprovider.InstanceTypeOverhead{
			KubeReserved: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("80m"), corev1.ResourceMemory: resource.MustParse("600Mi")},
		},
	}
}

// fleetNodePool returns the i-th NodePool: general purpose pools over several categories, plus
// tainted GPU, sandbox and storage pools and a system pool, cycling so any pool count mixes all kinds.
func fleetNodePool(i int) *v1.NodePool {
	name := fmt.Sprintf("pool-%02d", i)
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}})
	np.UID = types.UID("uid-" + name)
	np.Generation = 1
	req := func(key string, op corev1.NodeSelectorOperator, values ...string) v1.NodeSelectorRequirementWithMinValues {
		return v1.NodeSelectorRequirementWithMinValues{Key: key, Operator: op, Values: values}
	}
	capacityTypes := req(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot, v1.CapacityTypeOnDemand)
	linux := req(corev1.LabelOSStable, corev1.NodeSelectorOpIn, string(corev1.Linux))
	switch i % 8 {
	case 0, 1, 2:
		np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{capacityTypes, linux,
			req(fleetLabelCategory, corev1.NodeSelectorOpIn, "c", "m", "r"),
			req(fleetLabelGeneration, corev1.NodeSelectorOpGt, "5")}
		np.Spec.Template.Labels = map[string]string{fleetLabelNodeType: "general"}
	case 3:
		np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{linux,
			req(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
			req(fleetLabelCategory, corev1.NodeSelectorOpIn, "g", "p")}
		np.Spec.Template.Spec.Taints = []corev1.Taint{{Key: fleetTaintGPU, Value: "true", Effect: corev1.TaintEffectNoSchedule}}
		np.Spec.Template.Labels = map[string]string{fleetLabelNodeType: "gpu", fleetLabelSharingDaemon: "true"}
	case 4:
		np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{capacityTypes, linux,
			req(fleetLabelCategory, corev1.NodeSelectorOpIn, "m", "c"),
			req(corev1.LabelArchStable, corev1.NodeSelectorOpIn, "amd64")}
		np.Spec.Template.Spec.Taints = []corev1.Taint{{Key: fleetTaintSandbox, Value: "true", Effect: corev1.TaintEffectNoSchedule}}
		np.Spec.Template.Labels = map[string]string{fleetLabelNodeType: "sandbox", fleetLabelSandbox: "true"}
	case 5:
		np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{linux,
			req(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
			req(fleetLabelCategory, corev1.NodeSelectorOpIn, "i")}
		np.Spec.Template.Spec.Taints = []corev1.Taint{{Key: fleetTaintStorage, Value: "true", Effect: corev1.TaintEffectNoSchedule}}
		np.Spec.Template.Labels = map[string]string{fleetLabelNodeType: "storage", fleetLabelStorageNode: "db", fleetLabelDiskType: "nvme"}
	case 6:
		np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{linux,
			req(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
			req(fleetLabelCategory, corev1.NodeSelectorOpIn, "m", "t")}
		np.Spec.Template.Labels = map[string]string{fleetLabelNodeType: "system"}
	default:
		np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{capacityTypes,
			req(fleetLabelCategory, corev1.NodeSelectorOpNotIn, "g", "p", "t")}
	}
	return np
}

// fleetDaemonSetPods returns the DaemonSet pod mix described in the file header, named and shaped
// after the live pods the scheduler reads.
func fleetDaemonSetPods(nodePools []*v1.NodePool) []*corev1.Pod {
	exists := func(key string) corev1.NodeSelectorRequirement {
		return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpExists}
	}
	tolerateAll := []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	tolerate := func(keys ...string) []corev1.Toleration {
		var ts []corev1.Toleration
		for _, k := range keys {
			ts = append(ts, corev1.Toleration{Key: k, Operator: corev1.TolerationOpExists})
		}
		return ts
	}
	pod := func(name, cpu, memory string, initContainers int, selector map[string]string, tolerations []corev1.Toleration, terms ...[]corev1.NodeSelectorRequirement) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: name},
			Spec: corev1.PodSpec{
				NodeSelector: selector,
				Tolerations:  tolerations,
				Containers: []corev1.Container{{Name: "main", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory)},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(memory)},
				}}},
			},
		}
		for i := range initContainers {
			p.Spec.InitContainers = append(p.Spec.InitContainers, corev1.Container{Name: fmt.Sprintf("init-%d", i), Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("16Mi")},
			}})
		}
		if len(terms) != 0 {
			var nodeSelectorTerms []corev1.NodeSelectorTerm
			for _, t := range terms {
				nodeSelectorTerms = append(nodeSelectorTerms, corev1.NodeSelectorTerm{MatchExpressions: t})
			}
			p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: nodeSelectorTerms}}}
		}
		return p
	}
	linuxOnly := map[string]string{corev1.LabelOSStable: string(corev1.Linux)}
	poolsOfKind := func(nodeType string) []string {
		var names []string
		for _, np := range nodePools {
			if np.Spec.Template.Labels[fleetLabelNodeType] == nodeType {
				names = append(names, np.Name)
			}
		}
		return append(names, "absent-pool")
	}
	return []*corev1.Pod{
		pod("cni-agent", "100m", "300Mi", 7, linuxOnly, tolerateAll),
		pod("cni-proxy", "50m", "64Mi", 0, linuxOnly, tolerateAll, []corev1.NodeSelectorRequirement{notIn(fleetLabelNoCNI, "true")}),
		pod("block-csi-node", "30m", "120Mi", 0, linuxOnly, tolerateAll, []corev1.NodeSelectorRequirement{notIn(fleetLabelComputeType, "serverless", "auto", "hybrid")}),
		pod("block-csi-node-windows", "30m", "120Mi", 0, map[string]string{corev1.LabelOSStable: string(corev1.Windows)}, tolerateAll, []corev1.NodeSelectorRequirement{notIn(fleetLabelComputeType, "serverless", "auto", "hybrid")}),
		pod("object-csi-node", "30m", "80Mi", 0, linuxOnly, tolerateAll, []corev1.NodeSelectorRequirement{notIn(fleetLabelComputeType, "serverless", "hybrid")}),
		pod("identity-agent", "25m", "32Mi", 1, nil, tolerateAll, []corev1.NodeSelectorRequirement{in(corev1.LabelOSStable, "linux"), in(corev1.LabelArchStable, "amd64", "arm64"), notIn(fleetLabelComputeType, "serverless", "hybrid", "auto")}),
		pod("node-exporter", "20m", "48Mi", 0, linuxOnly, tolerateAll, []corev1.NodeSelectorRequirement{notIn(fleetLabelComputeType, "serverless"), notIn(fleetLabelVirtual, "true")}),
		pod("log-agent", "100m", "256Mi", 1, nil, tolerateAll),
		pod("profile-agent", "50m", "128Mi", 0, nil, tolerateAll),
		pod("canary", "10m", "32Mi", 0, nil, tolerateAll),
		pod("local-dns", "25m", "32Mi", 0, nil, tolerateAll, []corev1.NodeSelectorRequirement{notIn(fleetLabelNodeType, "system")}),
		pod("gpu-device-plugin", "50m", "64Mi", 0, nil, tolerate(fleetTaintGPU), []corev1.NodeSelectorRequirement{exists(fleetLabelGPUName)}),
		pod("gpu-exporter", "100m", "256Mi", 0, nil, tolerate(fleetTaintGPU), []corev1.NodeSelectorRequirement{in(corev1.LabelOSStable, "linux"), exists(fleetLabelGPUName)}),
		pod("gpu-sharing-daemon", "50m", "64Mi", 1, map[string]string{fleetLabelSharingDaemon: "true"}, tolerate(fleetTaintGPU), []corev1.NodeSelectorRequirement{exists(fleetLabelGPUName)}),
		pod("gpu-health", "50m", "64Mi", 1, nil, tolerate(fleetTaintGPU), []corev1.NodeSelectorRequirement{in(v1.NodePoolLabelKey, poolsOfKind("gpu")...)}),
		pod("gpu-image-prepull", "10m", "32Mi", 0, map[string]string{fleetLabelGPUName: "gpu-a", corev1.LabelArchStable: "amd64"}, tolerate(fleetTaintGPU), []corev1.NodeSelectorRequirement{in(v1.NodePoolLabelKey, poolsOfKind("gpu")...)}),
		pod("sandbox-runtime", "50m", "64Mi", 0, map[string]string{fleetLabelSandbox: "true"}, tolerate(fleetTaintSandbox)),
		pod("sandbox-csi", "50m", "64Mi", 0, nil, tolerate(fleetTaintSandbox), []corev1.NodeSelectorRequirement{in(v1.NodePoolLabelKey, poolsOfKind("sandbox")...)}),
		pod("sandbox-janitor", "20m", "32Mi", 0, nil, tolerate(fleetTaintSandbox), []corev1.NodeSelectorRequirement{in(v1.NodePoolLabelKey, poolsOfKind("sandbox")...)}),
		pod("sandbox-prepull", "20m", "32Mi", 2, nil, tolerate(fleetTaintSandbox), []corev1.NodeSelectorRequirement{in(v1.NodePoolLabelKey, poolsOfKind("sandbox")...)}),
		pod("sandbox-cache", "100m", "256Mi", 0, nil, tolerate(fleetTaintSandbox), []corev1.NodeSelectorRequirement{in(v1.NodePoolLabelKey, poolsOfKind("sandbox")...)}),
		pod("storage-local-csi", "50m", "64Mi", 0, map[string]string{corev1.LabelOSStable: "linux", fleetLabelStorageNode: "db"}, tolerate(fleetTaintStorage)),
		pod("storage-node-setup", "50m", "64Mi", 0, map[string]string{fleetLabelStorageNode: "db"}, tolerate(fleetTaintStorage)),
		pod("nvme-provisioner", "20m", "32Mi", 0, nil, tolerate(fleetTaintStorage), []corev1.NodeSelectorRequirement{in(fleetLabelDiskType, "nvme")}),
		pod("telemetry-agent", "100m", "200Mi", 1, nil, tolerateAll),
		pod("cache-pithos", "100m", "128Mi", 0, map[string]string{v1.NodePoolLabelKey: nodePools[0].Name}, tolerateAll),
	}
}
