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

package pdb_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	karpenterv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

var _ = Describe("ExemptPods", func() {
	var exempt, other *v1.Pod
	var limits pdb.Limits
	var exemption pdb.Option

	BeforeEach(func() {
		budget := test.PodDisruptionBudget(test.PDBOptions{Labels: podLabels, MaxUnavailable: new(intstr.FromInt32(0))})
		exempt = test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: podLabels}})
		other = test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: podLabels}})
		ExpectApplied(ctx, env.Client, budget, exempt, other)
		var err error
		limits, err = pdb.NewLimits(ctx, env.Client)
		Expect(err).NotTo(HaveOccurred())
		exemption = pdb.ExemptPods(func(p *v1.Pod) bool { return p.UID == exempt.UID })
	})

	It("ignores PDBs for exempt pods in CanEvictPods", func() {
		_, canEvict := limits.CanEvictPods([]*v1.Pod{exempt}, env.Clock, nil)
		Expect(canEvict).To(BeFalse())
		_, canEvict = limits.CanEvictPods([]*v1.Pod{exempt}, env.Clock, nil, exemption)
		Expect(canEvict).To(BeTrue())
		_, canEvict = limits.CanEvictPods([]*v1.Pod{exempt, other}, env.Clock, nil, exemption)
		Expect(canEvict).To(BeFalse())
	})
	It("ignores PDBs for exempt pods in IsCurrentlyReschedulable", func() {
		Expect(limits.IsCurrentlyReschedulable(exempt, env.Clock, nil)).To(BeFalse())
		Expect(limits.IsCurrentlyReschedulable(exempt, env.Clock, nil, exemption)).To(BeTrue())
		Expect(limits.IsCurrentlyReschedulable(other, env.Clock, nil, exemption)).To(BeFalse())
	})
	It("evaluates the exemption only for pods a PDB blocks", func() {
		unblocked := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"other": "value"}}})
		ExpectApplied(ctx, env.Client, unblocked)
		var calls int
		counting := pdb.ExemptPods(func(*v1.Pod) bool { calls++; return false })
		_, canEvict := limits.CanEvictPods([]*v1.Pod{unblocked}, env.Clock, nil, counting)
		Expect(canEvict).To(BeTrue())
		Expect(limits.IsCurrentlyReschedulable(unblocked, env.Clock, nil, counting)).To(BeTrue())
		Expect(calls).To(BeZero())

		_, canEvict = limits.CanEvictPods([]*v1.Pod{other}, env.Clock, nil, counting)
		Expect(canEvict).To(BeFalse())
		Expect(calls).To(Equal(1))
	})
	It("still applies the do-not-disrupt annotation to exempt pods", func() {
		exempt.Annotations = map[string]string{karpenterv1.DoNotDisruptAnnotationKey: "true"}
		Expect(limits.IsCurrentlyReschedulable(exempt, env.Clock, nil, exemption)).To(BeFalse())
	})
})
