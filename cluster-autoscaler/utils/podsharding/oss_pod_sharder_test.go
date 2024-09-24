package podsharding

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestPodSharderWithoutProvReq(t *testing.T) {
	testCases := []shardComputeFunctionTestCase{
		{
			name:                        "no gpu",
			pod:                         v1.Pod{},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{},
		},
		{
			name: "any gpu",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("3"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("8"),
				},
			},
		},
		{
			name: "k80",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.GPULabel: "nvidia-tesla-k80",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("8"),
				},
			},
		},
		{
			name: "k80 with time sharing",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.GPULabel:                 "nvidia-tesla-k80",
						labels.GPUSharingStrategyLabel:  "time-sharing",
						labels.GPUMaxSharedClientsLabel: "3",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("24"),
				},
			},
		},
		{
			name: "k80 with time sharing and default max time shared clients",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.GPULabel:                "nvidia-tesla-k80",
						labels.GPUSharingStrategyLabel: "time-sharing",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("16"),
				},
			},
		},
		{
			name: "GPU sharing and partitioning at once",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.GPULabel:                 "nvidia-tesla-a100",
						labels.GPUSharingStrategyLabel:  "time-sharing",
						labels.GPUPartitionSizeLabel:    "1g.5gb",
						labels.GPUMaxSharedClientsLabel: "3",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.GPULabel: "nvidia-tesla-a100",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("336"),
				},
			},
		},
		{
			name: "k80 affinity",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      labels.GPULabel,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"nvidia-tesla-k80"},
											},
										},
									},
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("8"),
				},
			},
		},
		{
			name: "unknown gpu",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.GPULabel: "nvidia-tesla-blah",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.GPULabel: "nvidia-tesla-blah",
				},
				ExtraResources: map[string]resource.Quantity{},
			},
		},
		{
			name: "any tpu",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									tpu.ResourceGoogleTPU: resource.MustParse("4"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.TPULabel: labels.TpuV4LiteDeviceValue,
				},
				ExtraResources: map[string]resource.Quantity{
					tpu.ResourceGoogleTPU: resource.MustParse("4"),
				},
			},
		},
		{
			name: "tpu_device",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.TPULabel: labels.TpuV4LiteDeviceValue,
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									tpu.ResourceGoogleTPU: resource.MustParse("4"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.TPULabel: labels.TpuV4LiteDeviceValue,
				},
				ExtraResources: map[string]resource.Quantity{
					tpu.ResourceGoogleTPU: resource.MustParse("4"),
				},
			},
		},
		{
			name: "tpu with topology and accelerator count",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.TPULabel:              labels.TpuV4LiteDeviceValue,
						labels.TPUTopologyLabel:      "2x2",
						labels.AcceleratorCountLabel: "4",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									tpu.ResourceGoogleTPU: resource.MustParse("4"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					labels.TPULabel:              labels.TpuV4LiteDeviceValue,
					labels.TPUTopologyLabel:      "2x2",
					labels.AcceleratorCountLabel: "4",
				},
				ExtraResources: map[string]resource.Quantity{
					tpu.ResourceGoogleTPU: resource.MustParse("4"),
				},
			},
		},
		{
			name: "no_workload_separation",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{},
					Tolerations:  []v1.Toleration{},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{},
				Taints: []v1.Taint{},
			},
		},
		{
			name: "one_workload_label",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"workload": "w1",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "workload",
							Operator: v1.TolerationOpEqual,
							Value:    "w1",
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"workload": "w1",
				},
				Taints: []v1.Taint{
					{
						Key:    "workload",
						Value:  "w1",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
		},
		{
			name: "two_workload_labels",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"workload": "wungiel",
						"country":  "Poland",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "workload",
							Operator: v1.TolerationOpEqual,
							Value:    "wungiel",
							Effect:   v1.TaintEffectNoSchedule,
						},
						{
							Key:      "country",
							Operator: v1.TolerationOpExists,
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"workload": "wungiel",
					"country":  "Poland",
				},
				Taints: []v1.Taint{
					{
						Key:    "workload",
						Value:  "wungiel",
						Effect: v1.TaintEffectNoSchedule,
					},
					{
						Key:    "country",
						Value:  "Poland",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
		},
		{
			name: "provisioning_request_ignored",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
					},
				},
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"country": "Poland",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "country",
							Operator: v1.TolerationOpExists,
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"country": "Poland",
				},
				Taints: []v1.Taint{
					{
						Key:    "country",
						Value:  "Poland",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
		},
	}
	testPodSharder(t, NewGkePodSharder(false, false, false, []string{}), testCases)
}

func TestProvisioningRequestShardComputeFunction(t *testing.T) {
	testCases := []shardComputeFunctionTestCase{
		{
			name: "no_provisioning_request",
			pod: v1.Pod{
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{},
				Taints: []v1.Taint{},
			},
		},
		{
			name: "provisioning_request_present, no class name",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
					},
				},
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{},
				Taints: []v1.Taint{},
			},
		},
		{
			name: "provisioning_request_present, unknown class name",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
						"cluster-autoscaler.kubernetes.io/provisioning-class-name":      "unknown",
					},
				},
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels:                map[string]string{},
				Taints:                []v1.Taint{},
				ProvisioningClassName: "unknown",
			},
		},
		{
			name: "provisioning_request_present, check-capacity provisioning class",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
						"cluster-autoscaler.kubernetes.io/provisioning-class-name":      "check-capacity.autoscaling.x-k8s.io",
					},
					Labels: map[string]string{"key": "value"},
				},
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels:                map[string]string{},
				Taints:                []v1.Taint{},
				ProvisioningClassName: provreqv1.ProvisioningClassCheckCapacity,
			},
		},
		{
			name: "two_workload_labels_and_provisioning_request",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
						"cluster-autoscaler.kubernetes.io/provisioning-class-name":      "queued-provisioning.gke.io",
					},
				},
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"workload": "wungiel",
						"country":  "Poland",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "workload",
							Operator: v1.TolerationOpEqual,
							Value:    "wungiel",
							Effect:   v1.TaintEffectNoSchedule,
						},
						{
							Key:      "country",
							Operator: v1.TolerationOpExists,
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"workload": "wungiel",
					"country":  "Poland",
				},
				Taints: []v1.Taint{
					{
						Key:    "workload",
						Value:  "wungiel",
						Effect: v1.TaintEffectNoSchedule,
					},
					{
						Key:    "country",
						Value:  "Poland",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
				ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
			},
		},
	}
	testPodSharder(t, NewGkePodSharder(true, false, false, []string{}), testCases)
}

func TestNpcShardComputeFunction(t *testing.T) {
	testCases := []shardComputeFunctionTestCase{
		{
			name: "CCC not specified, not set in shard",
			pod: v1.Pod{
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{},
			},
		},
		{
			name: "default CCC specified, using default",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.ComputeClassLabel: labels.DefaultNPCName,
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{labels.ComputeClassLabel: labels.DefaultNPCName},
			},
		},
		{
			name: "non default CCC specified, using specified",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.ComputeClassLabel: "some-random-ccc",
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{labels.ComputeClassLabel: "some-random-ccc"},
			},
		},
	}
	testPodSharder(t, NewGkePodSharder(false, false, true, []string{}), testCases)
}

func TestCccShardComputeFunction(t *testing.T) {
	testCases := []shardComputeFunctionTestCase{
		{
			name: "NPC not specified, not set in shard",
			pod: v1.Pod{
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{},
			},
		},
		{
			name: "default NPC specified, using default",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.NodeProvisioningConfigLabel: labels.DefaultNPCName,
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{labels.NodeProvisioningConfigLabel: labels.DefaultNPCName},
			},
		},
		{
			name: "non default NPC specified, using specified",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						labels.NodeProvisioningConfigLabel: "some-random-npc",
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{labels.NodeProvisioningConfigLabel: "some-random-npc"},
			},
		},
	}
	testPodSharder(t, NewGkePodSharder(false, true, false, []string{}), testCases)
}

type shardComputeFunctionTestCase struct {
	name                        string
	pod                         v1.Pod
	expectedNodeGroupDescriptor NodeGroupDescriptor
}

func testPodSharder(t *testing.T, sharder PodSharder, testCases []shardComputeFunctionTestCase) {
	t.Helper()
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			shards := sharder.ComputePodShards([]*v1.Pod{&tc.pod})
			if len(shards) != 1 {
				t.Errorf("Expected to get precisely 1 shard, but got %d. Shards: %v", len(shards), shards)
				return
			}
			assertNodeGroupDescriptorEqual(t, tc.expectedNodeGroupDescriptor, shards[0].NodeGroupDescriptor)
		})
	}
}
