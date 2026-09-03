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

package lifecycle

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

var InstanceTerminationDurationSeconds = opmetrics.NewPrometheusHistogram(
	crmetrics.Registry,
	prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodeClaimSubsystem,
		Name:      "instance_termination_duration_seconds",
		Help:      "Duration of CloudProvider Instance termination in seconds, by NodePool, the instance and capacity type the NodeClaim launched with (unknown when it never launched), and the cause of the deletion (a voluntary disruption reason, the termination-cause annotation such as cloud_interrupted, never_initialized, or other).",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 11), //The threshold values generated here are 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
	},
	[]string{metrics.NodePoolLabel, instanceTypeLabel, metrics.CapacityTypeLabel, causeLabel},
)

// NodeClaimLifetimeSeconds records how long each NodeClaim existed, from creation to the removal of its
// termination finalizer, by the decision that launched it and the cause that ended it. Replacement
// NodeClaims carry NodeClaimReplacementOriginAnnotationKey; everything else is attributed to
// provisioning. Together with the age of nodes consolidation disrupts, this shows whether replacement
// nodes live long enough to pay for the disruption that launched them.
var NodeClaimLifetimeSeconds = opmetrics.NewPrometheusHistogram(
	crmetrics.Registry,
	prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodeClaimSubsystem,
		Name:      "lifetime_seconds",
		Help:      "Seconds from NodeClaim creation to deletion, by NodePool, the instance type it launched with (unknown when it never launched), capacity type, the origin that launched it (provisioning, or the replacement-origin annotation set by the disruption queue), and the cause of its termination (a voluntary disruption reason, the termination-cause annotation such as cloud_interrupted, never_initialized, or other).",
		Buckets:   metrics.NodeLifetimeBuckets(),
	},
	[]string{metrics.NodePoolLabel, instanceTypeLabel, metrics.CapacityTypeLabel, originLabel, causeLabel},
)

const (
	instanceTypeLabel = "instance_type"
	originLabel       = "origin"
	causeLabel        = "cause"

	// unknownLabelValue stands in for an instance or capacity type the NodeClaim's labels do not carry,
	// which is the case for a NodeClaim deleted before the cloud provider ever launched it.
	unknownLabelValue = "unknown"

	// provisioningOrigin is the origin of every NodeClaim not launched as a disruption replacement.
	provisioningOrigin = "provisioning"
	// terminationCauseNeverInitialized marks a NodeClaim deleted before it ever became a usable node:
	// a failed launch, a registration or initialization timeout, or an instance that disappeared.
	terminationCauseNeverInitialized = "never_initialized"
	// terminationCauseOther covers deletions the NodeClaim itself does not explain: expiration,
	// garbage collection of a vanished instance, and operator deletes. Cloud-initiated interruption
	// is reported under its own cause when the deleter stamps NodeClaimTerminationCauseAnnotationKey.
	terminationCauseOther = "other"
)

var NodeClaimTerminationDurationSeconds = opmetrics.NewPrometheusHistogram(
	crmetrics.Registry,
	prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodeClaimSubsystem,
		Name:      "termination_duration_seconds",
		Help:      "Duration of NodeClaim termination in seconds, by NodePool, the instance and capacity type the NodeClaim launched with (unknown when it never launched), and the cause of the deletion (a voluntary disruption reason, the termination-cause annotation such as cloud_interrupted, never_initialized, or other).",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 12)}, //The threshold values generated here are 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024. 2048
	[]string{metrics.NodePoolLabel, instanceTypeLabel, metrics.CapacityTypeLabel, causeLabel},
)

const (
	// StageLabel names the slice of the NodeClaim bootstrap a sample covers.
	StageLabel = "stage"
	// StageLaunch covers NodeClaim creation until the CloudProvider instance was created (Launched).
	StageLaunch = "launch"
	// StageRegistration covers Launched until the kubelet joined the cluster (Registered).
	StageRegistration = "registration"
	// StageInitialization covers Registered until the node went Ready with startup taints cleared and requested
	// resources registered (Initialized).
	StageInitialization = "initialization"
	// StageTotal covers NodeClaim creation until Initialized, i.e. the time a replacement holds a disruption
	// command (and its budget slot) before the candidate can be terminated.
	StageTotal = "total"
)

// NodeClaimInitializationDurationSeconds records how long a NodeClaim spent in each bootstrap stage, observed once
// when it becomes Initialized. The existing status condition transition histogram tops out at 10s, which says
// nothing about boots that take minutes; this one is bucketed from 15s to 2h so slow pools (e.g. inf2 cold starts)
// and stuck bootstraps are visible per nodepool and capacity type.
var NodeClaimInitializationDurationSeconds = opmetrics.NewPrometheusHistogram(
	crmetrics.Registry,
	prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodeClaimSubsystem,
		Name:      "initialization_duration_seconds",
		Help:      "Seconds a NodeClaim spent in each bootstrap stage (launch, registration, initialization, total), observed when it becomes Initialized.",
		Buckets:   []float64{15, 30, 60, 120, 180, 240, 300, 420, 600, 900, 1200, 1800, 2700, 3600, 7200},
	},
	[]string{StageLabel, metrics.NodePoolLabel, metrics.CapacityTypeLabel},
)
