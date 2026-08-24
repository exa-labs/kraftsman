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

package metrics

import (
	"context"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
)

const (
	metricLabelController = "controller"
	metricLabelMethod     = "method"
	metricLabelProvider   = "provider"
	metricLabelError      = "error"
	// MetricLabelErrorDefaultVal is the default string value that represents "error type unknown"
	MetricLabelErrorDefaultVal = ""
	// Well-known metricLabelError values
	NodeClaimNotFoundError    = "NodeClaimNotFoundError"
	NodeClassNotReadyError    = "NodeClassNotReadyError"
	InsufficientCapacityError = "InsufficientCapacityError"
)

// decorator implements CloudProvider
var _ cloudprovider.CloudProvider = (*decorator)(nil)

var MethodDuration = opmetrics.NewPrometheusHistogram(
	crmetrics.Registry,
	prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: "cloudprovider",
		Name:      "duration_seconds",
		Help:      "Duration of cloud provider method calls. Labeled by the controller, method name and provider.",
	},
	[]string{
		metricLabelController,
		metricLabelMethod,
		metricLabelProvider,
	},
)

var (
	ErrorsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: "cloudprovider",
			Name:      "errors_total",
			Help:      "Total number of errors returned from CloudProvider calls.",
		},
		[]string{
			metricLabelController,
			metricLabelMethod,
			metricLabelProvider,
			metricLabelError,
		},
	)
)

type decorator struct {
	cloudprovider.CloudProvider
}

// Decorate returns a new `CloudProvider` instance that will delegate all method
// calls to the argument, `cloudProvider`, and publish aggregated latency metrics. The
// value used for the metric label, "controller", is taken from the `Context` object
// passed to the methods of `CloudProvider`.
//
// Do not decorate a `CloudProvider` multiple times or published metrics will contain
// duplicated method call counts and latencies.
func Decorate(cloudProvider cloudprovider.CloudProvider) cloudprovider.CloudProvider {
	return &decorator{cloudProvider}
}

func (d *decorator) Create(ctx context.Context, nodeClaim *v1.NodeClaim) (*v1.NodeClaim, error) {
	method := "Create"
	defer metrics.Measure(MethodDuration, getLabelsMapForDuration(ctx, d, method))()
	nodeClaim, err := d.CloudProvider.Create(ctx, nodeClaim)
	if err != nil {
		ErrorsTotal.Inc(getLabelsMapForError(ctx, d, method, err))
	}
	return nodeClaim, err
}

func (d *decorator) Delete(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	method := "Delete"
	defer metrics.Measure(MethodDuration, getLabelsMapForDuration(ctx, d, method))()
	err := d.CloudProvider.Delete(ctx, nodeClaim)
	if err != nil {
		ErrorsTotal.Inc(getLabelsMapForError(ctx, d, method, err))
	}
	return err
}

func (d *decorator) Get(ctx context.Context, id string) (*v1.NodeClaim, error) {
	method := "Get"
	defer metrics.Measure(MethodDuration, getLabelsMapForDuration(ctx, d, method))()
	nodeClaim, err := d.CloudProvider.Get(ctx, id)
	if err != nil {
		ErrorsTotal.Inc(getLabelsMapForError(ctx, d, method, err))
	}
	return nodeClaim, err
}

func (d *decorator) List(ctx context.Context) ([]*v1.NodeClaim, error) {
	method := "List"
	defer metrics.Measure(MethodDuration, getLabelsMapForDuration(ctx, d, method))()
	nodeClaims, err := d.CloudProvider.List(ctx)
	if err != nil {
		ErrorsTotal.Inc(getLabelsMapForError(ctx, d, method, err))
	}
	return nodeClaims, err
}

func (d *decorator) GetInstanceTypes(ctx context.Context, nodePool *v1.NodePool) ([]*cloudprovider.InstanceType, error) {
	method := "GetInstanceTypes"
	defer metrics.Measure(MethodDuration, getLabelsMapForDuration(ctx, d, method))()
	instanceType, err := d.CloudProvider.GetInstanceTypes(ctx, nodePool)
	if err != nil {
		ErrorsTotal.Inc(getLabelsMapForError(ctx, d, method, err))
	}
	return instanceType, err
}

// GetInstanceTypesWithRevision forwards the optional InstanceTypesRevisionProvider interface,
// reporting the call under the same method label as GetInstanceTypes. When the decorated provider
// does not implement it, the result is equivalent to GetInstanceTypes with a zero revision.
func (d *decorator) GetInstanceTypesWithRevision(ctx context.Context, nodePool *v1.NodePool) ([]*cloudprovider.InstanceType, uint64, error) {
	method := "GetInstanceTypes"
	defer metrics.Measure(MethodDuration, getLabelsMapForDuration(ctx, d, method))()
	revisionProvider, ok := d.CloudProvider.(cloudprovider.InstanceTypesRevisionProvider)
	if !ok {
		instanceTypes, err := d.CloudProvider.GetInstanceTypes(ctx, nodePool)
		if err != nil {
			ErrorsTotal.Inc(getLabelsMapForError(ctx, d, method, err))
		}
		return instanceTypes, 0, err
	}
	instanceTypes, revision, err := revisionProvider.GetInstanceTypesWithRevision(ctx, nodePool)
	if err != nil {
		ErrorsTotal.Inc(getLabelsMapForError(ctx, d, method, err))
	}
	return instanceTypes, revision, err
}

// InstanceTypeRevision forwards the optional InstanceTypeRevisionProvider interface. A lookup on
// the underlying provider's revision counter is too cheap to be worth a duration sample, so it is
// not measured. A provider that does not implement it reports 0 (no stable revision).
func (d *decorator) InstanceTypeRevision(ctx context.Context, nodePool *v1.NodePool) (uint64, error) {
	revisionProvider, ok := d.CloudProvider.(cloudprovider.InstanceTypeRevisionProvider)
	if !ok {
		return 0, nil
	}
	return revisionProvider.InstanceTypeRevision(ctx, nodePool)
}

func (d *decorator) IsDrifted(ctx context.Context, nodeClaim *v1.NodeClaim) (cloudprovider.DriftReason, error) {
	method := "IsDrifted"
	defer metrics.Measure(MethodDuration, getLabelsMapForDuration(ctx, d, method))()
	isDrifted, err := d.CloudProvider.IsDrifted(ctx, nodeClaim)
	if err != nil {
		ErrorsTotal.Inc(getLabelsMapForError(ctx, d, method, err))
	}
	return isDrifted, err
}

// getLabelsMapForDuration is a convenience func that constructs a map[string]string
// for a prometheus Label map used to compose a duration metric spec
func getLabelsMapForDuration(ctx context.Context, d *decorator, method string) map[string]string {
	return map[string]string{
		metricLabelController: injection.GetControllerName(ctx),
		metricLabelMethod:     method,
		metricLabelProvider:   d.Name(),
	}
}

// getLabelsMapForError is a convenience func that constructs a map[string]string
// for a prometheus Label map used to compose a counter metric spec
func getLabelsMapForError(ctx context.Context, d *decorator, method string, err error) map[string]string {
	return map[string]string{
		metricLabelController: injection.GetControllerName(ctx),
		metricLabelMethod:     method,
		metricLabelProvider:   d.Name(),
		metricLabelError:      GetErrorTypeLabelValue(err),
	}
}

// GetErrorTypeLabelValue is a convenience func that returns
// a string representation of well-known CloudProvider error types
func GetErrorTypeLabelValue(err error) string {
	switch {
	case cloudprovider.IsInsufficientCapacityError(err):
		return InsufficientCapacityError
	case cloudprovider.IsNodeClaimNotFoundError(err):
		return NodeClaimNotFoundError
	case cloudprovider.IsNodeClassNotReadyError(err):
		return NodeClassNotReadyError
	default:
		return MetricLabelErrorDefaultVal
	}
}
