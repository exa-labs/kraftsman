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

package metrics_test

import (
	"fmt"
	"testing"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

// benchmarkStoreShape mirrors the node metrics controller's steady state: one store key per node, six resource
// gauges with one series per resource type plus a lifetime gauge, every series carrying the node's well-known
// labels.
const (
	benchmarkNodes         = 1000
	benchmarkResourceTypes = 5
	benchmarkNodeLabels    = 30
)

// benchmarkStoreMetrics builds a full ReplaceAll payload. The value is the only thing that changes between rounds,
// as it is for nodes whose labels are stable between reconciles.
func benchmarkStoreMetrics(resourceGauges []opmetrics.GaugeMetric, lifetime opmetrics.GaugeMetric, value float64) map[string][]*metrics.StoreMetric {
	out := make(map[string][]*metrics.StoreMetric, benchmarkNodes)
	for n := range benchmarkNodes {
		nodeLabels := prometheus.Labels{}
		for l := range benchmarkNodeLabels {
			nodeLabels[fmt.Sprintf("label_%d", l)] = fmt.Sprintf("node-%d-value-%d", n, l)
		}
		var nodeMetrics []*metrics.StoreMetric
		for _, gauge := range resourceGauges {
			for r := range benchmarkResourceTypes {
				labels := prometheus.Labels{"resource_type": fmt.Sprintf("resource_%d", r)}
				for k, v := range nodeLabels {
					labels[k] = v
				}
				nodeMetrics = append(nodeMetrics, &metrics.StoreMetric{GaugeMetric: gauge, Value: value, Labels: labels})
			}
		}
		nodeMetrics = append(nodeMetrics, &metrics.StoreMetric{GaugeMetric: lifetime, Value: value, Labels: nodeLabels})
		out[fmt.Sprintf("node-%d", n)] = nodeMetrics
	}
	return out
}

func BenchmarkStoreReplaceAll(b *testing.B) {
	registry := prometheus.NewRegistry()
	labelNames := make([]string, 0, benchmarkNodeLabels)
	for l := range benchmarkNodeLabels {
		labelNames = append(labelNames, fmt.Sprintf("label_%d", l))
	}
	var resourceGauges []opmetrics.GaugeMetric
	for g := range 6 {
		resourceGauges = append(resourceGauges, opmetrics.NewPrometheusGauge(registry, prometheus.GaugeOpts{Name: fmt.Sprintf("benchmark_gauge_%d", g)}, append([]string{"resource_type"}, labelNames...)))
	}
	lifetime := opmetrics.NewPrometheusGauge(registry, prometheus.GaugeOpts{Name: "benchmark_lifetime"}, labelNames)

	store := metrics.NewStore()
	store.ReplaceAll(benchmarkStoreMetrics(resourceGauges, lifetime, 0))
	rounds := []map[string][]*metrics.StoreMetric{
		benchmarkStoreMetrics(resourceGauges, lifetime, 1),
		benchmarkStoreMetrics(resourceGauges, lifetime, 2),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.ReplaceAll(rounds[i%len(rounds)])
	}
}
