package podsharding

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/predicatechecker"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestGetExpansionMachineTypeName(t *testing.T) {
	var testCases = []struct {
		machineFamily       string
		expectedMachineType string
		gpuLabel            string
		tpuLabel            string
	}{
		{
			machineFamily:       "n1",
			expectedMachineType: machinetypes.N1.LargestAutoprovisionedMachineType(machinetypes.NoConstraints).Name,
		},
		{
			machineFamily:       "nXYZ",
			expectedMachineType: "t3", // this is done to match the test suite
			// as the test suite considers the last machine in the last as the largest
			// machine, whereas in actual family, the largest machine is chosen based
			// on approx. amount of its resources.
		},
		{
			machineFamily:       "n2",
			expectedMachineType: machinetypes.N2.LargestAutoprovisionedMachineType(machinetypes.NoConstraints).Name,
		},
		{
			expectedMachineType: machinetypes.N1.LargestAutoprovisionedMachineType(machinetypes.Constraints{GpuType: "nvidia-tesla-k80", CpuPlatform: machinetypes.AnyPlatform}).Name,
			gpuLabel:            "nvidia-tesla-k80",
		},
		{
			expectedMachineType: machinetypes.A2.LargestAutoprovisionedMachineType(machinetypes.Constraints{GpuType: machinetypes.NvidiaTeslaA100.Name(), CpuPlatform: machinetypes.AnyPlatform}).Name,
			gpuLabel:            machinetypes.NvidiaTeslaA100.Name(),
		},
		{
			expectedMachineType: machinetypes.A2.LargestAutoprovisionedMachineType(machinetypes.Constraints{GpuType: machinetypes.NvidiaA100_80gb.Name(), CpuPlatform: machinetypes.AnyPlatform}).Name,
			gpuLabel:            machinetypes.NvidiaA100_80gb.Name(),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.machineFamily, func(t *testing.T) {
			cloudProvider := gke.NewTestAutoprovisioningCloudProvider(nil, nil, []string{"t1", "t2", "t3"}, nil, false, false, false, "", "")
			selector := machineselection.Selector{CloudProvider: cloudProvider}
			mp := make(map[string]podrequirements.Values)
			if testCase.machineFamily != "" {
				mp["cloud.google.com/machine-family"] = podrequirements.NewValues(testCase.machineFamily)
			}
			requirements := &podrequirements.Requirements{LabelReq: podrequirements.NewLabelRequirements(mp)}
			assert.Equal(t, testCase.expectedMachineType, getExpansionMachineTypeName(selector, requirements, testCase.gpuLabel, testCase.tpuLabel))
		})
	}
}

func TestPredicatePodShardFilterFilterPods(t *testing.T) {
	tests := []struct {
		name             string
		selectedPodShard *PodShard
		allPodShards     []*PodShard
		pods             []*apiv1.Pod
		want             PodFilteringResult
		wantErr          bool
	}{
		{
			name:             "simple test case without provreq",
			selectedPodShard: createTestPodShard("", []string{"label1", "label2"}, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShard("", []string{"label1", "label2"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
					createTestPod("2"),
					createTestPod("3"),
					createTestPod("4"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "pods from provreq are not expanded with regular pods",
			selectedPodShard: createTestPodShard("provreq", []string{"label1", "label2"}, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShard("provreq", []string{"label1", "label2"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
					createTestPod("2"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "pods from first provreq are not expanded, second provreq is ignored",
			selectedPodShard: createTestPodShard("provreq1", []string{"label1", "label2"}, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShard("provreq1", []string{"label1", "label2"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
				createTestPodShard("provreq2", []string{"label1", "label2"}, "5", "6"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
				createTestPod("5"),
				createTestPod("6"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
					createTestPod("2"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "pods from regular shard are NOT expanded with provreq-shard",
			selectedPodShard: createTestPodShard("", []string{"label1"}, "3", "4"),
			allPodShards: []*PodShard{
				createTestPodShard("pr", []string{"label1"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("3"),
					createTestPod("4"),
				},
				ZoneAgnostic: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			machineTypes := []string{"n1-standard-1", "n1-standard-2", "n1-standard-4", "n1-standard-8"}
			machineTempaltes := make(map[string]*framework.NodeInfo, len(machineTypes))
			for _, machineType := range machineTypes {
				frameworkNode := framework.NewNodeInfo()
				frameworkNode.SetNode(test.BuildTestNode(machineType+"-node", 1, 1))
				machineTempaltes[machineType] = frameworkNode
			}
			predicateChecker, err := predicatechecker.NewTestPredicateChecker()
			assert.NoError(t, err)

			ctx := &context.AutoscalingContext{
				CloudProvider:    gke.NewTestAutoprovisioningCloudProvider(nil, nil, machineTypes, nil, false, false, true, "", ""),
				ClusterSnapshot:  clustersnapshot.NewBasicClusterSnapshot(),
				PredicateChecker: predicateChecker,
			}
			p := NewPredicatePodShardFilter()
			got, err := p.FilterPods(ctx, tt.selectedPodShard, tt.allPodShards, tt.pods)
			if (err != nil) != tt.wantErr {
				t.Errorf("PredicatePodShardFilter.FilterPods() error = %v\nwantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PredicatePodShardFilter.FilterPods() = %v\nwant %v", got, tt.want)
			}
		})
	}
}

func createTestPodShard(provReqClass string, labels []string, podUIDs ...string) *PodShard {
	podUIDsMap := make(map[types.UID]bool, len(podUIDs))
	for _, podUID := range podUIDs {
		podUIDsMap[types.UID(podUID)] = true
	}
	labelsMap := make(map[string]string, len(labels))
	for _, label := range labels {
		labelsMap[label] = "true"
	}

	return &PodShard{
		PodUids: podUIDsMap,
		NodeGroupDescriptor: NodeGroupDescriptor{
			Labels:                labelsMap,
			ProvisioningClassName: provReqClass,
		},
	}
}

func createTestPod(uid string) *apiv1.Pod {
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:               types.UID(uid),
			Namespace:         "default",
			Name:              uid,
			CreationTimestamp: metav1.NewTime(time.Date(2024, 8, 9, 12, 13, 14, 0, time.UTC)),
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{},
					},
				},
			},
		},
	}
	return pod
}
