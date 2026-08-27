/*
Copyright 2026.

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

package trainer

import (
	"testing"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	. "github.com/opendatahub-io/distributed-workloads/tests/common"
	. "github.com/opendatahub-io/distributed-workloads/tests/common/support"
	trainerutils "github.com/opendatahub-io/distributed-workloads/tests/trainer/utils"
)

// TestKueueSchedulingReachesTrainJobPods verifies that scheduling directives (node
// labels and tolerations) carried on a Kueue ResourceFlavor are actually propagated
// onto the JobSet's pod template that Kueue admits, regardless of whether the
// installed Kueue version writes spec.podTemplateOverrides or spec.runtimePatches
// on the TrainJob to get there. We assert on the JobSet's pod template rather than
// on the TrainJob spec so the same assertion holds under both Kueue versions.
func TestKueueSchedulingReachesTrainJobPods(t *testing.T) {
	Tags(t, Tier1)
	test := With(t)
	SetupKueue(test, initialKueueState, TrainJobFramework)

	namespace := test.NewTestNamespace(WithKueueManaged()).Name
	test.T().Logf("Created Kueue-managed namespace: %s", namespace)

	const (
		flavorNodeLabelKey   = "kueue-test-flavor"
		flavorNodeLabelValue = "on"
		flavorTolerationKey  = "kueue-test"
	)

	resourceFlavor := CreateKueueResourceFlavor(test, kueuev1beta2.ResourceFlavorSpec{
		NodeLabels: map[string]string{
			flavorNodeLabelKey: flavorNodeLabelValue,
		},
		Tolerations: []corev1.Toleration{
			{
				Key:      flavorTolerationKey,
				Operator: corev1.TolerationOpEqual,
				Value:    flavorNodeLabelValue,
				Effect:   corev1.TaintEffectNoSchedule,
			},
		},
	})
	defer test.Client().Kueue().KueueV1beta2().ResourceFlavors().Delete(test.Ctx(), resourceFlavor.Name, metav1.DeleteOptions{})

	clusterQueue := CreateKueueClusterQueue(test, kueuev1beta2.ClusterQueueSpec{
		NamespaceSelector: &metav1.LabelSelector{},
		ResourceGroups: []kueuev1beta2.ResourceGroup{
			{
				CoveredResources: []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
				Flavors: []kueuev1beta2.FlavorQuotas{
					{
						Name: kueuev1beta2.ResourceFlavorReference(resourceFlavor.Name),
						Resources: []kueuev1beta2.ResourceQuota{
							{
								Name:         corev1.ResourceCPU,
								NominalQuota: resource.MustParse("8"),
							},
							{
								Name:         corev1.ResourceMemory,
								NominalQuota: resource.MustParse("16Gi"),
							},
						},
					},
				},
			},
		},
	})
	defer test.Client().Kueue().KueueV1beta2().ClusterQueues().Delete(test.Ctx(), clusterQueue.Name, metav1.DeleteOptions{})
	localQueue := CreateKueueLocalQueue(test, namespace, clusterQueue.Name, AsDefaultQueue)

	// No GPU required: this test only cares about the scheduling directives Kueue
	// attaches to the JobSet pod template, not about actual training compute.
	trainJob := &trainerv1alpha1.TrainJob{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "test-runtimepatches-trainjob-",
			Namespace:    namespace,
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": localQueue.Name,
			},
		},
		Spec: trainerv1alpha1.TrainJobSpec{
			RuntimeRef: trainerv1alpha1.RuntimeRef{
				Name: trainerutils.DefaultClusterTrainingRuntimeCPU,
			},
			Trainer: &trainerv1alpha1.Trainer{
				Command: []string{"echo", "test"},
			},
		},
	}

	createdTrainJob, err := test.Client().Trainer().TrainerV1alpha1().TrainJobs(namespace).Create(
		test.Ctx(),
		trainJob,
		metav1.CreateOptions{},
	)
	test.Expect(err).NotTo(HaveOccurred())
	test.T().Logf("Created TrainJob: %s", createdTrainJob.Name)

	test.Eventually(KueueWorkloads(test, namespace), TestTimeoutMedium).Should(
		And(
			HaveLen(1),
			ContainElement(WithTransform(KueueWorkloadAdmitted, BeTrue())),
		),
	)
	test.T().Log("Workload admitted")

	// Assert the admitted JobSet's pod template carries the ResourceFlavor's
	// node label and toleration, however Kueue's TrainJob integration got them there.
	test.Eventually(SingleJobSet(test, namespace), TestTimeoutMedium).Should(
		WithTransform(JobSetReplicatedJobsCount, BeNumerically(">", 0)),
	)
	jobSet := SingleJobSet(test, namespace)(test)

	for _, replicatedJob := range jobSet.Spec.ReplicatedJobs {
		podSpec := replicatedJob.Template.Spec.Template.Spec
		test.Expect(podSpec.NodeSelector).To(
			HaveKeyWithValue(flavorNodeLabelKey, flavorNodeLabelValue),
			"replicated job %q should carry the ResourceFlavor's node label", replicatedJob.Name,
		)
		test.Expect(podSpec.Tolerations).To(
			ContainElement(corev1.Toleration{
				Key:      flavorTolerationKey,
				Operator: corev1.TolerationOpEqual,
				Value:    flavorNodeLabelValue,
				Effect:   corev1.TaintEffectNoSchedule,
			}),
			"replicated job %q should carry the ResourceFlavor's toleration", replicatedJob.Name,
		)
	}
	test.T().Logf("JobSet %q pod templates carry the ResourceFlavor's scheduling directives", jobSet.Name)
}
