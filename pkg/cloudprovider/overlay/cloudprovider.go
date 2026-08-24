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

package overlay

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeoverlay"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

type decorator struct {
	cloudprovider.CloudProvider
	kubeClient client.Client
	store      *nodeoverlay.InstanceTypeStore
}

// Decorate returns a new `CloudProvider` instance that will delegate the GetInstanceTypes
// calls to the argument, `cloudProvider`, and provide instance types with NodeOverlays applied to them. The
func Decorate(cloudProvider cloudprovider.CloudProvider, kubeClient client.Client, store *nodeoverlay.InstanceTypeStore) cloudprovider.CloudProvider {
	return &decorator{CloudProvider: cloudProvider, kubeClient: kubeClient, store: store}
}

func (d *decorator) GetInstanceTypes(ctx context.Context, nodePool *v1.NodePool) ([]*cloudprovider.InstanceType, error) {
	its, err := d.CloudProvider.GetInstanceTypes(ctx, nodePool)
	if err != nil {
		return []*cloudprovider.InstanceType{}, err
	}
	if options.FromContext(ctx).FeatureGates.NodeOverlay {
		its, err = d.store.ApplyAll(nodePool.Name, its)
		if err != nil {
			return []*cloudprovider.InstanceType{}, fmt.Errorf("applying nodeoverlays, %w", err)
		}
	}
	return its, nil
}

// GetInstanceTypesWithRevision forwards the optional InstanceTypesRevisionProvider interface. When
// the NodeOverlay feature gate is enabled the returned content also depends on the overlay store,
// which the inner provider's revision does not cover, so the revision is reported as 0 (unstable).
func (d *decorator) GetInstanceTypesWithRevision(ctx context.Context, nodePool *v1.NodePool) ([]*cloudprovider.InstanceType, uint64, error) {
	revisionProvider, ok := d.CloudProvider.(cloudprovider.InstanceTypesRevisionProvider)
	if !ok {
		its, err := d.GetInstanceTypes(ctx, nodePool)
		return its, 0, err
	}
	its, revision, err := revisionProvider.GetInstanceTypesWithRevision(ctx, nodePool)
	if err != nil {
		return []*cloudprovider.InstanceType{}, 0, err
	}
	if options.FromContext(ctx).FeatureGates.NodeOverlay {
		its, err = d.store.ApplyAll(nodePool.Name, its)
		if err != nil {
			return []*cloudprovider.InstanceType{}, 0, fmt.Errorf("applying nodeoverlays, %w", err)
		}
		return its, 0, nil
	}
	return its, revision, nil
}

// InstanceTypeRevision forwards the optional InstanceTypeRevisionProvider interface. When the
// NodeOverlay feature gate is enabled the served content also depends on the overlay store, which
// the inner provider's revision does not cover, so the revision is reported as 0 (unstable).
func (d *decorator) InstanceTypeRevision(ctx context.Context, nodePool *v1.NodePool) (uint64, error) {
	if options.FromContext(ctx).FeatureGates.NodeOverlay {
		return 0, nil
	}
	revisionProvider, ok := d.CloudProvider.(cloudprovider.InstanceTypeRevisionProvider)
	if !ok {
		return 0, nil
	}
	return revisionProvider.InstanceTypeRevision(ctx, nodePool)
}
