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

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

func TestSingleNodeConsolidationComputeCommandsStartsCachePass(t *testing.T) {
	client := fakecr.NewFakeClient()
	cloudProvider := fake.NewCloudProvider()
	clusterClock := clock.RealClock{}
	cluster := state.NewCluster(clusterClock, client, cloudProvider)
	recorder := events.NewRecorder(&record.FakeRecorder{})
	consolidation := MakeConsolidation(
		clusterClock,
		cluster,
		client,
		nil,
		cloudProvider,
		recorder,
		NewQueue(client, recorder, cluster, clusterClock, nil),
	)

	ctx := options.ToContext(context.Background(), &options.Options{})
	if _, err := NewSingleNodeConsolidation(consolidation).ComputeCommands(ctx, nil); err != nil {
		t.Fatalf("single-node consolidation pass failed: %v", err)
	}
}

func TestSingleNodeConsolidationInstallsSchedulerCache(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate test source")
	}
	source, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "singlenodeconsolidation.go"))
	if err != nil {
		t.Fatalf("reading single-node consolidation source: %v", err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "singlenodeconsolidation.go", source, 0)
	if err != nil {
		t.Fatalf("parsing single-node consolidation source: %v", err)
	}

	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "WithDaemonOverheadCache" {
			found = true
			return false
		}
		return true
	})
	if !found {
		t.Fatal("single-node consolidation does not install a daemon overhead cache")
	}
}
