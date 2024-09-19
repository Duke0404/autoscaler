package podrequirements

import (
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"github.com/stretchr/testify/assert"
)

func TestExtractWorkloadID(t *testing.T) {
	testCases := []struct {
		name      string
		taints    []apiv1.Taint
		labels    map[string]string
		resources map[apiv1.ResourceName]resource.Quantity
		expected  string
	}{
		{
			name:     "no taints or labels",
			expected: "",
		},
		{
			name: "no labels",
			taints: []apiv1.Taint{
				{Key: "workload", Value: "workload1", Effect: apiv1.TaintEffectNoSchedule},
			},
			expected: "",
		},
		{
			name:     "no taints",
			labels:   map[string]string{"key": "value"},
			expected: "",
		},
		{
			name: "label matching NoSchedule taint",
			taints: []apiv1.Taint{
				{Key: "key", Value: "value", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key": "value"},
			expected: "NoSchedule:key:value",
		},
		{
			name: "label matching NoExecute taint",
			taints: []apiv1.Taint{
				{Key: "key", Value: "value", Effect: apiv1.TaintEffectNoExecute},
			},
			labels:   map[string]string{"key": "value"},
			expected: "NoExecute:key:value",
		},
		{
			name: "label not matching taint value",
			taints: []apiv1.Taint{
				{Key: "key", Value: "value", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key": "value2"},
			expected: "",
		},
		{
			name: "label not matching taint key",
			taints: []apiv1.Taint{
				{Key: "key", Value: "value", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key2": "value"},
			expected: "",
		},
		{
			name: "one labels and two taints",
			taints: []apiv1.Taint{
				{Key: "key1", Value: "value1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key1": "value1"},
			expected: "NoSchedule:key1:value1",
		},
		{
			name: "two labels and two taints",
			taints: []apiv1.Taint{
				{Key: "key1", Value: "value1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key1": "value1", "key2": "value2"},
			expected: "NoSchedule:key1:value1,NoSchedule:key2:value2",
		},
		{
			name: "two labels and two taints reversed",
			taints: []apiv1.Taint{
				{Key: "key2", Value: "value2", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "key1", Value: "value1", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key1": "value1", "key2": "value2"},
			expected: "NoSchedule:key1:value1,NoSchedule:key2:value2",
		},
		{
			name: "disregarded system label",
			taints: []apiv1.Taint{
				{Key: "cloud.google.com/disregard-label", Value: "value1", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"cloud.google.com/disregard-label": "value1"},
			expected: "",
		},
		{
			name: "gpu taint and resource",
			taints: []apiv1.Taint{
				{Key: gpu.ResourceNvidiaGPU, Value: "present", Effect: apiv1.TaintEffectNoSchedule},
			},
			resources: map[apiv1.ResourceName]resource.Quantity{gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI)},
			expected:  "NoSchedule:nvidia.com/gpu:present",
		},
		{
			name: "gpu taint but not resource",
			taints: []apiv1.Taint{
				{Key: gpu.ResourceNvidiaGPU, Value: "present", Effect: apiv1.TaintEffectNoSchedule},
			},
			expected: "",
		},
		{
			name:      "gpu resource but not taint",
			resources: map[apiv1.ResourceName]resource.Quantity{gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI)},
			expected:  "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := BuildTestNode("test", 1000, 1000)
			node.Spec.Taints = tc.taints
			node.Labels = tc.labels
			for key, value := range tc.resources {
				node.Status.Capacity[key] = value
				node.Status.Allocatable[key] = value
			}
			assert.Equal(t, tc.expected, ExtractWorkloadID(node))
		})
	}
}
