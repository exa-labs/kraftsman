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

package operator

import (
	"reflect"

	"github.com/awslabs/operatorpkg/option"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/apis/v1alpha1"
)

// byTypeName indexes cache options by the Go type name of their key, since the keys themselves are pointers.
func byTypeName(byObject map[client.Object]cache.ByObject) map[string]cache.ByObject {
	out := map[string]cache.ByObject{}
	for obj, opts := range byObject {
		out[reflect.TypeOf(obj).Elem().Name()] = opts
	}
	return out
}

var _ = Describe("Cache Options", func() {
	leaseField := fields.SelectorFromSet(fields.Set{"metadata.namespace": "kube-node-lease"})

	It("should only scope leases by default", func() {
		got := byTypeName(cacheByObject(option.Resolve[Options]()))
		Expect(got).To(HaveLen(1))
		Expect(got["Lease"].Field.String()).To(Equal(leaseField.String()))
	})
	It("should apply the object selector to the objects that make up an instance's capacity", func() {
		selector := labels.SelectorFromSet(labels.Set{"example.com/instance": "a"})
		got := byTypeName(cacheByObject(option.Resolve(WithObjectSelector(selector))))
		Expect(got).To(HaveLen(5))
		Expect(got["Lease"].Field.String()).To(Equal(leaseField.String()))
		for _, kind := range []string{"NodePool", "NodeClaim", "NodeOverlay", "Node"} {
			Expect(got).To(HaveKey(kind))
			Expect(got[kind].Label.Matches(labels.Set{"example.com/instance": "a"})).To(BeTrue(), kind)
			Expect(got[kind].Label.Matches(labels.Set{"example.com/instance": "b"})).To(BeFalse(), kind)
			Expect(got[kind].Label.Matches(labels.Set{})).To(BeFalse(), kind)
		}
	})
	It("should support selecting objects that do not carry a label", func() {
		selector, err := labels.Parse("!example.com/instance")
		Expect(err).ToNot(HaveOccurred())
		got := byTypeName(cacheByObject(option.Resolve(WithObjectSelector(selector))))
		Expect(got["Node"].Label.Matches(labels.Set{})).To(BeTrue())
		Expect(got["Node"].Label.Matches(labels.Set{"example.com/instance": "a"})).To(BeFalse())
	})
	It("should add caller entries for types the operator does not scope", func() {
		selector := labels.SelectorFromSet(labels.Set{"example.com/instance": "a"})
		got := byTypeName(cacheByObject(option.Resolve(
			WithObjectSelector(selector),
			WithCacheByObject(map[client.Object]cache.ByObject{&corev1.ConfigMap{}: {Label: selector}}),
		)))
		Expect(got).To(HaveLen(6))
		Expect(got["ConfigMap"].Label.String()).To(Equal(selector.String()))
	})
	It("should let a caller entry replace the operator's entry for the same type", func() {
		selector := labels.SelectorFromSet(labels.Set{"example.com/instance": "a"})
		override := labels.SelectorFromSet(labels.Set{"example.com/node": "true"})
		leaseOverride := fields.SelectorFromSet(fields.Set{"metadata.namespace": "other"})
		byObject := cacheByObject(option.Resolve(
			WithObjectSelector(selector),
			WithCacheByObject(map[client.Object]cache.ByObject{
				&corev1.Node{}:          {Label: override},
				&coordinationv1.Lease{}: {Field: leaseOverride},
			}),
		))
		Expect(byObject).To(HaveLen(5))
		got := byTypeName(byObject)
		Expect(got["Node"].Label.String()).To(Equal(override.String()))
		Expect(got["Lease"].Field.String()).To(Equal(leaseOverride.String()))
		Expect(got["NodePool"].Label.String()).To(Equal(selector.String()))
	})
	It("should key every entry by a distinct type", func() {
		selector := labels.SelectorFromSet(labels.Set{"example.com/instance": "a"})
		byObject := cacheByObject(option.Resolve(
			WithObjectSelector(selector),
			WithCacheByObject(map[client.Object]cache.ByObject{&v1.NodePool{}: {Label: selector}, &v1alpha1.NodeOverlay{}: {}}),
		))
		types := map[reflect.Type]int{}
		for obj := range byObject {
			types[reflect.TypeOf(obj)]++
		}
		for t, n := range types {
			Expect(n).To(Equal(1), t.String())
		}
		Expect(byObject).To(HaveLen(5))
	})
})
