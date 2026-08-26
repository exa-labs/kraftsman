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
		Help:      "Duration of CloudProvider Instance termination in seconds, by NodePool and the cause of the deletion (a voluntary disruption reason, the termination-cause annotation such as cloud_interrupted, never_initialized, or other).",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 11), //The threshold values generated here are 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
	},
	[]string{metrics.NodePoolLabel, causeLabel},
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
		Help:      "Seconds from NodeClaim creation to deletion, by NodePool, capacity type, the origin that launched it (provisioning, or the replacement-origin annotation set by the disruption queue), and the cause of its termination (a voluntary disruption reason, the termination-cause annotation such as cloud_interrupted, never_initialized, or other).",
		Buckets:   metrics.NodeLifetimeBuckets(),
	},
	[]string{metrics.NodePoolLabel, metrics.CapacityTypeLabel, originLabel, causeLabel},
)

const (
	originLabel = "origin"
	causeLabel  = "cause"

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
		Help:      "Duration of NodeClaim termination in seconds, by NodePool and the cause of the deletion (a voluntary disruption reason, the termination-cause annotation such as cloud_interrupted, never_initialized, or other).",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 12)}, //The threshold values generated here are 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024. 2048
	[]string{metrics.NodePoolLabel, causeLabel},
)
