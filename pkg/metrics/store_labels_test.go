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
	"testing"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

// TestStoreTreatsNilAndEmptyLabelsAsTheSameSeries pins the label-set equality Store.update relies on:
// a series stored with nil labels and refreshed with empty labels (or the reverse) is the same
// series and must survive the refresh.
func TestStoreTreatsNilAndEmptyLabelsAsTheSameSeries(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second prometheus.Labels
	}{
		{"nil then empty", nil, prometheus.Labels{}},
		{"empty then nil", prometheus.Labels{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			gauge := opmetrics.NewPrometheusGauge(registry, prometheus.GaugeOpts{Name: "store_label_equality"}, []string{})
			store := metrics.NewStore()
			store.Update("key", []*metrics.StoreMetric{{GaugeMetric: gauge, Value: 1, Labels: tc.first}})
			store.Update("key", []*metrics.StoreMetric{{GaugeMetric: gauge, Value: 2, Labels: tc.second}})

			families, err := registry.Gather()
			if err != nil {
				t.Fatal(err)
			}
			var series []*dto.Metric
			for _, family := range families {
				if family.GetName() == "store_label_equality" {
					series = family.GetMetric()
				}
			}
			if len(series) != 1 || series[0].GetGauge().GetValue() != 2 {
				t.Fatalf("expected the refreshed series to survive with value 2, got %v", series)
			}
		})
	}
}
