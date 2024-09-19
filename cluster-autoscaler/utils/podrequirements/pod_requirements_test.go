package podrequirements

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/pods"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestGetTolerations(t *testing.T) {
	tests := []struct {
		name        string
		tolerations []apiv1.Toleration
		expected    []apiv1.Toleration
	}{
		{"empty", []apiv1.Toleration{}, nil},
		{
			"all kinds of tolerations",
			[]apiv1.Toleration{
				{Key: "ne", Operator: apiv1.TolerationOpExists, Value: "bar", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "ns", Operator: apiv1.TolerationOpExists, Value: "bas", Effect: apiv1.TaintEffectNoExecute},
				{Key: "ps", Operator: apiv1.TolerationOpExists, Value: "bat", Effect: apiv1.TaintEffectPreferNoSchedule},
			},
			[]apiv1.Toleration{
				{Key: "ne", Operator: apiv1.TolerationOpExists, Value: "bar", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "ns", Operator: apiv1.TolerationOpExists, Value: "bas", Effect: apiv1.TaintEffectNoExecute},
			},
		},
		{
			"all kinds of operators",
			[]apiv1.Toleration{
				{Key: "ne", Operator: apiv1.TolerationOpExists, Value: "bar", Effect: apiv1.TaintEffectNoExecute},
				{Key: "ns", Operator: apiv1.TolerationOpEqual, Value: "bas", Effect: apiv1.TaintEffectNoExecute},
			},
			[]apiv1.Toleration{
				{Key: "ne", Operator: apiv1.TolerationOpExists, Value: "bar", Effect: apiv1.TaintEffectNoExecute},
				{Key: "ns", Operator: apiv1.TolerationOpEqual, Value: "bas", Effect: apiv1.TaintEffectNoExecute},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: tc.tolerations,
				},
			}
			r := getTolerations(p)
			if !reflect.DeepEqual(tc.expected, r) {
				t.Errorf("Expected: %v, got: %v", tc.expected, r)
			}
		})
	}
}

func TestGetNodeSelectors(t *testing.T) {
	tests := []struct {
		name      string
		nSelector map[string]string
		expected  map[string]Values
	}{
		{"empty", nil, map[string]Values{}},
		{
			"a few",
			map[string]string{"a": "b", "c": "d"},
			map[string]Values{
				"a": NewValues("b"),
				"c": NewValues("d"),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: tc.nSelector,
				},
			}
			r := getNodeSelectors(p)
			if !reflect.DeepEqual(tc.expected, r) {
				t.Errorf("Expected: %v, got: %v", tc.expected, r)
			}
		})
	}
}

func TestGetNodeAffinities(t *testing.T) {
	tests := []struct {
		name     string
		nsTerms  []apiv1.NodeSelectorTerm
		expected map[string]Values
	}{
		{"empty", []apiv1.NodeSelectorTerm{}, map[string]Values{}},
		{
			"extracts only In and Exists matches",
			[]apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpIn, Values: []string{"b"}},
						{Key: "aa", Operator: apiv1.NodeSelectorOpIn, Values: []string{"bb", "cc"}},
						{Key: "b", Operator: apiv1.NodeSelectorOpNotIn, Values: []string{"d"}},
						{Key: "c", Operator: apiv1.NodeSelectorOpExists, Values: []string{""}},
						{Key: "d", Operator: apiv1.NodeSelectorOpDoesNotExist, Values: []string{""}},
						{Key: "e", Operator: apiv1.NodeSelectorOpGt, Values: []string{"1"}},
						{Key: "f", Operator: apiv1.NodeSelectorOpLt, Values: []string{"2"}},
					},
				},
			},
			map[string]Values{"a": NewValues("b"), "aa": NewValues("bb", "cc"), "c": AnyValue()},
		},
		{
			"two terms",
			[]apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpIn, Values: []string{"b"}},
						{Key: "x", Operator: apiv1.NodeSelectorOpExists},
					},
				},
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "c", Operator: apiv1.NodeSelectorOpIn, Values: []string{"d", "e"}},
						{Key: "y", Operator: apiv1.NodeSelectorOpExists},
					},
				},
			},
			map[string]Values{"a": NewValues("b"), "c": NewValues("d", "e"), "x": AnyValue(), "y": AnyValue()},
		},
		{
			"Exists matches don't overwrite In matches",
			[]apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpIn, Values: []string{"b"}},
						{Key: "a", Operator: apiv1.NodeSelectorOpExists},
					},
				},
			},
			map[string]Values{"a": NewValues("b")},
		},
		{
			"Exists matches don't overwrite In matches across terms",
			[]apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpIn, Values: []string{"b"}},
					},
				},
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpExists},
					},
				},
			},
			map[string]Values{"a": NewValues("b")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: tc.nsTerms,
							},
						},
					},
				},
			}
			r := getNodeAffinities(p)
			if !reflect.DeepEqual(tc.expected, r) {
				t.Errorf("Expected: %v, got: %v", tc.expected, r)
			}
		})
	}
}

func TestWorkloadSeparationMatch(t *testing.T) {
	tests := []struct {
		name         string
		toleration   apiv1.Toleration
		requirements map[string]Values
		expectedB    bool
		expectedS    string
	}{
		{
			"exists toleration match",
			apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpExists, Value: ""},
			map[string]Values{
				"k":  NewValues("v"),
				"k2": NewValues("v2"),
			},
			true,
			"v",
		},
		{
			"exists toleration match, but affinity for multiple values",
			apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpExists, Value: ""},
			map[string]Values{
				"k":  NewValues("v", "v1"),
				"k2": NewValues("v2"),
			},
			false,
			"",
		},
		{
			"exists toleration match, but without label value specified",
			apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpExists, Value: ""},
			map[string]Values{
				"k":  AnyValue(),
				"k2": NewValues("v2"),
			},
			false,
			"",
		},
		{
			"equal toleration match",
			apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v"},
			map[string]Values{
				"k":  NewValues("v"),
				"k2": NewValues("v2"),
			},
			true,
			"v",
		},
		{
			"equal toleration match, but affinity for multiple values",
			apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v"},
			map[string]Values{
				"k":  NewValues("v", "v1"),
				"k2": NewValues("v2"),
			},
			false,
			"",
		},
		{
			"equal toleration match, but without label value specified",
			apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v"},
			map[string]Values{
				"k":  AnyValue(),
				"k2": NewValues("v2"),
			},
			false,
			"",
		},
		{
			"exists toleration mismatch",
			apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpExists, Value: ""},
			map[string]Values{
				"k1": NewValues("v1"),
				"k2": NewValues("v2"),
			},
			false,
			"",
		},
		{
			"equal toleration mismatch",
			apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v"},
			map[string]Values{
				"k":  NewValues("v1"),
				"k2": NewValues("v2"),
			},
			false,
			"",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs, rb := workloadSeparationMatch(tc.toleration, NewLabelRequirements(tc.requirements))
			if rs != tc.expectedS || rb != tc.expectedB {
				t.Errorf("Expected (%s, %v), got (%s, %v)", tc.expectedS, tc.expectedB, rs, rb)
			}
		})
	}
}

func TestWorkloadSeparationTaintsAndLabels(t *testing.T) {
	tests := []struct {
		name                           string
		tolerations                    []apiv1.Toleration
		requirements                   map[string]Values
		allowlistedSystemLabelPatterns []string
		expectLabels                   map[string]string
		expectTaints                   []apiv1.Taint
		expectErr                      error
	}{
		{
			name:         "no requirements and no tolerations",
			tolerations:  nil,
			requirements: nil,
			expectLabels: map[string]string{},
			expectTaints: []apiv1.Taint{},
			expectErr:    nil,
		},
		{
			name: "match with exists",
			tolerations: []apiv1.Toleration{
				{Key: "k", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k": NewValues("v")},
			expectLabels: map[string]string{"k": "v"},
			expectTaints: []apiv1.Taint{
				{Key: "k", Value: "v", Effect: apiv1.TaintEffectNoSchedule},
			},
			expectErr: nil,
		},
		{
			name: "match with equals",
			tolerations: []apiv1.Toleration{
				{Key: "k2", Operator: apiv1.TolerationOpEqual, Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k": NewValues("v"), "k2": NewValues("v2")},
			expectErr:    ErrInvalidWorkloadSeparation,
		},
		{
			name: "two matches",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k2", Operator: apiv1.TolerationOpEqual, Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": NewValues("v"), "k2": NewValues("v2")},
			expectLabels: map[string]string{"k1": "v", "k2": "v2"},
			expectTaints: []apiv1.Taint{
				{Key: "k1", Value: "v", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "toleration without a match is ok",
			tolerations: []apiv1.Toleration{
				{Key: "k", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			expectTaints: []apiv1.Taint{},
			expectLabels: map[string]string{},
		},
		{
			name:         "requirement without a toleration is an error",
			requirements: map[string]Values{"k": NewValues("v")},
			expectErr:    ErrInvalidWorkloadSeparation,
		},
		{
			name: "match with additional unmatched tolerations is ok",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k2", Operator: apiv1.TolerationOpEqual, Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": NewValues("v1")},
			expectLabels: map[string]string{"k1": "v1"},
			expectTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "match with additional unmatched requirements is an error",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": NewValues("v1"), "k2": NewValues("v2")},
			expectErr:    ErrInvalidWorkloadSeparation,
		},
		{
			name: "system match",
			tolerations: []apiv1.Toleration{
				{Key: labels.GPULabel, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{labels.GPULabel: NewValues("v")},
			expectTaints: []apiv1.Taint{},
			expectLabels: map[string]string{},
		},
		{
			name: "one system match, one non-system match",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: labels.GPULabel, Operator: apiv1.TolerationOpEqual, Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": NewValues("v1"), labels.GPULabel: NewValues("v2")},
			expectLabels: map[string]string{"k1": "v1"},
			expectTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "no match for any value",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": AnyValue()},
			expectErr:    ErrInvalidWorkloadSeparation,
		},
		{
			name: "no match for same keys but different values",
			tolerations: []apiv1.Toleration{
				{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k": NewValues("v2")},
			expectErr:    ErrInvalidWorkloadSeparation,
		},
		{
			name: "allow listed system label match",
			tolerations: []apiv1.Toleration{
				{Key: labels.GPULabel, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements:                   map[string]Values{labels.GPULabel: NewValues("true")},
			allowlistedSystemLabelPatterns: []string{labels.GPULabel},
			expectLabels:                   map[string]string{labels.GPULabel: "true"},
			expectTaints: []apiv1.Taint{
				{Key: labels.GPULabel, Value: "true", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "allow listed & non allow listed system label",
			tolerations: []apiv1.Toleration{
				{Key: labels.GPULabel, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/my-feature", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements:                   map[string]Values{"cloud.google.com/my-feature": NewValues("true"), labels.GPULabel: NewValues("true")},
			allowlistedSystemLabelPatterns: []string{"cloud.google.com/my-feature"},
			expectLabels:                   map[string]string{"cloud.google.com/my-feature": "true"},
			expectTaints: []apiv1.Taint{
				{Key: "cloud.google.com/my-feature", Value: "true", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "allow listed & non allow listed system label patterns",
			tolerations: []apiv1.Toleration{
				{Key: labels.GPULabel, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/my-feature-1", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/my-feature-2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/unrecognized", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{
				labels.GPULabel:                 NewValues("true"),
				"cloud.google.com/my-feature-1": NewValues("true"),
				"cloud.google.com/my-feature-2": NewValues("false"),
				"cloud.google.com/unrecognized": NewValues("skip"),
			},
			allowlistedSystemLabelPatterns: []string{`cloud.google.com/my-feature-\d`},
			expectLabels: map[string]string{
				"cloud.google.com/my-feature-1": "true",
				"cloud.google.com/my-feature-2": "false",
			},
			expectTaints: []apiv1.Taint{
				{Key: "cloud.google.com/my-feature-1", Value: "true", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/my-feature-2", Value: "false", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := Requirements{Tolerations: tc.tolerations, LabelReq: NewLabelRequirements(tc.requirements)}
			c := NewWorkloadSeparationWorkloadChecker(tc.allowlistedSystemLabelPatterns)
			resultTaints, resultLabels, resultErr := req.WorkloadSeparationTaintsAndLabels(c)
			if !reflect.DeepEqual(tc.expectTaints, resultTaints) {
				t.Errorf("Expected taints %v got %v", tc.expectTaints, resultTaints)
			}
			if !reflect.DeepEqual(tc.expectLabels, resultLabels) {
				t.Errorf("Expected labels %v got %v", tc.expectLabels, resultLabels)
			}
			if tc.expectErr != resultErr {
				t.Errorf("Expected error %v got %v", tc.expectErr, resultErr)
			}
		})
	}
}

func TestGetRequirements(t *testing.T) {
	tests := []struct {
		name                          string
		pod                           *apiv1.Pod
		expectedTolerations           []apiv1.Toleration
		expectedLabelReq              map[string]Values
		expectedPodCapacity           string
		expectedQueuedProvisioningReq QueuedProvisioningRequirements
	}{
		{
			"plain pod",
			&apiv1.Pod{},
			nil,
			map[string]Values{},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"pod with tolerations (soft tolerations not taken into account)",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: []apiv1.Toleration{
						{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoExecute},
						{Key: "k2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
						{Key: "k3", Operator: apiv1.TolerationOpExists, Effect: ""},
						{Key: "k4", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectPreferNoSchedule},
					},
				},
			},
			[]apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoExecute},
				{Key: "k2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k3", Operator: apiv1.TolerationOpExists},
			},
			map[string]Values{},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"pod with node selectors",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{"k1": "v1", "k2": "v2"},
				},
			},
			nil,
			map[string]Values{"k1": NewValues("v1"), "k2": NewValues("v2")},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"pod with node affinity (only required affinity taken into account)",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k1",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v1"},
											},
											{
												Key:      "k2",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v2", "v3"},
											},
											{
												Key:      "k3",
												Operator: apiv1.NodeSelectorOpExists,
											},
										},
									},
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k4",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v4"},
											},
										},
									},
								},
							},
							PreferredDuringSchedulingIgnoredDuringExecution: []apiv1.PreferredSchedulingTerm{
								{
									Preference: apiv1.NodeSelectorTerm{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k5",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v5"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nil,
			map[string]Values{"k1": NewValues("v1"), "k2": NewValues("v2", "v3"), "k3": AnyValue(), "k4": NewValues("v4")},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"pod with tolerations, node selectors, and node affinities",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{"k1": "v1", "k2": "v2"},
					Tolerations: []apiv1.Toleration{
						{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoExecute},
						{Key: "k2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
					},
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k3",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v3"},
											},
											{
												Key:      "k4",
												Operator: apiv1.NodeSelectorOpExists,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			[]apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoExecute},
				{Key: "k2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			map[string]Values{"k1": NewValues("v1"), "k2": NewValues("v2"), "k3": NewValues("v3"), "k4": AnyValue()},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"affinities with values override node selectors",
			&apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "abc",
					Namespace: "def",
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{"k1": "v10", "k2": "v20"},
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k2",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v21"},
											},
											{
												Key:      "k3",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v30"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nil,
			map[string]Values{"k1": NewValues("v10"), "k2": NewValues("v21"), "k3": NewValues("v30")},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"affinities without values don't override node selectors",
			&apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "abc",
					Namespace: "def",
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{"k1": "v10", "k2": "v20"},
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k2",
												Operator: apiv1.NodeSelectorOpExists,
											},
											{
												Key:      "k3",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v30"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nil,
			map[string]Values{"k1": NewValues("v10"), "k2": NewValues("v20"), "k3": NewValues("v30")},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"pod with ppvm 1",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName(labels.PodCapacityLabel): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			nil,
			map[string]Values{},
			"1",
			QueuedProvisioningRequirements{},
		},
		{
			"pod with ppvm 2",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("other"): resource.MustParse("1"),
								},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName(labels.PodCapacityLabel): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			nil,
			map[string]Values{},
			"1",
			QueuedProvisioningRequirements{},
		},
		{
			"pod with ppvm 3",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName(labels.PodCapacityLabel): resource.MustParse("1"),
								},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName(labels.PodCapacityLabel): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			nil,
			map[string]Values{},
			"2",
			QueuedProvisioningRequirements{},
		},
		{
			"pod without ppvm 1",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{},
						},
					},
				},
			},
			nil,
			map[string]Values{},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"pod without ppvm 2",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			nil,
			map[string]Values{},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"pod with queued provisioning label",
			&apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{pods.DeprecatedProvisioningRequestPodAnnotationKey: "prov-req-1"},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			nil,
			map[string]Values{},
			"",
			QueuedProvisioningRequirements{true, "prov-req-1"},
		},
		{
			"resource label selector",
			&apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"annotationKey": "refkey:refvalue"},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"cloud.google.com/resourcelabel_annotationKey": "",
					},
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			nil,
			map[string]Values{"cloud.google.com/resourcelabel_annotationKey": NewValues("refkey:refvalue")},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"resource label selector, missing annotation",
			&apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"cloud.google.com/resourcelabel_annotationKey": "",
					},
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			nil,
			map[string]Values{"cloud.google.com/resourcelabel_annotationKey": NewValues("")},
			"",
			QueuedProvisioningRequirements{},
		},
		{
			"resource label selector, non empty value",
			&apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"annotationKey": "refkey:refvalue"},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"cloud.google.com/resourcelabel_annotationKey": "non-empty",
					},
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			nil,
			map[string]Values{"cloud.google.com/resourcelabel_annotationKey": NewValues("non-empty")},
			"",
			QueuedProvisioningRequirements{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := GetRequirements(tc.pod)

			if !reflect.DeepEqual(req.Tolerations, tc.expectedTolerations) {
				t.Errorf("Expected tolerations %v, got %v", tc.expectedTolerations, req.Tolerations)
			}
			if !reflect.DeepEqual(req.LabelReq, NewLabelRequirements(tc.expectedLabelReq)) {
				t.Errorf("Expected label requirements %v, got %v", NewLabelRequirements(tc.expectedLabelReq), req.LabelReq)
			}
			if req.PodCapacity != tc.expectedPodCapacity {
				t.Errorf("Expected podCapacity %v, got %v", tc.expectedPodCapacity, req.PodCapacity)
			}
			if req.QueuedProvisioningReq != tc.expectedQueuedProvisioningReq {
				t.Errorf("Expected queuedProvisioningReq %v, got %v", tc.expectedQueuedProvisioningReq, req.QueuedProvisioningReq)
			}
		})
	}
}

func TestGetPrivateNodeRequirement(t *testing.T) {
	tests := []struct {
		name                   string
		pod                    *apiv1.Pod
		expectedPrivateNodeReq PrivateNodeRequirement
		expectedErr            errors.AutoscalerError
	}{
		{
			"no private node affinity",
			&apiv1.Pod{},
			PrivateNodeRequirementUnspecified,
			nil,
		},
		{
			"true private node affinity",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{labels.PrivateNodeLabel: "true"},
				},
			},
			PrivateNodeRequirementTrue,
			nil,
		},
		{
			"false private node affinity",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{labels.PrivateNodeLabel: "false"},
				},
			},
			PrivateNodeRequirementFalse,
			nil,
		},
		{
			"invalid private node affinity",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{labels.PrivateNodeLabel: "maybe"},
				},
			},
			PrivateNodeRequirementUnspecified,
			NewInvalidLabelValueError(labels.PrivateNodeLabel, "maybe"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := GetRequirements(tc.pod)
			privateNodeReq, err := req.GetPrivateNodeRequirement()

			if diff := cmp.Diff(tc.expectedPrivateNodeReq, privateNodeReq); diff != "" {
				t.Errorf("Unexpected difference in private nodes requirements (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expectedErr, err); diff != "" {
				t.Errorf("Unepected difference in errors (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetSingleValue(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		expectedVal   string
		expectedFound bool
		req           map[string]Values
	}{
		{
			name:          "single value present",
			key:           "k",
			expectedVal:   "v",
			expectedFound: true,
			req:           map[string]Values{"k": NewValues("v")},
		},
		{
			name:          "multiple values present",
			key:           "k",
			expectedVal:   "",
			expectedFound: false,
			req:           map[string]Values{"k": NewValues("v", "w")},
		},
		{
			name:          "any value present",
			key:           "k",
			expectedVal:   "",
			expectedFound: false,
			req:           map[string]Values{"k": AnyValue()},
		},
		{
			name:          "no value",
			key:           "k",
			expectedVal:   "",
			expectedFound: false,
			req:           map[string]Values{},
		},
		{
			name:          "nil map",
			key:           "k",
			expectedVal:   "",
			expectedFound: false,
			req:           nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			labelReq := NewLabelRequirements(tc.req)
			val, isSpecified := labelReq.GetSingleValue(tc.key)
			if val != tc.expectedVal {
				t.Errorf("GetSingleValue value: expected %s, got %s", tc.expectedVal, val)
			}
			if isSpecified != tc.expectedFound {
				t.Errorf("GetSingleValue found: expected %v, got %v", tc.expectedFound, isSpecified)
			}
		})
	}
}

func TestGetValues(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		expectedVals  Values
		expectedFound bool
		req           map[string]Values
	}{
		{
			name:          "single value present",
			key:           "k",
			expectedVals:  NewValues("v"),
			expectedFound: true,
			req:           map[string]Values{"k": NewValues("v")},
		},
		{
			name:          "multiple values present",
			key:           "k",
			expectedVals:  NewValues("v", "w", "z"),
			expectedFound: true,
			req:           map[string]Values{"k": NewValues("v", "w", "z")},
		},
		{
			name:          "any value present",
			key:           "k",
			expectedVals:  AnyValue(),
			expectedFound: true,
			req:           map[string]Values{"k": AnyValue()},
		},
		{
			name:          "no value",
			key:           "k",
			expectedVals:  Values{},
			expectedFound: false,
			req:           map[string]Values{},
		},
		{
			name:          "nil map",
			key:           "k",
			expectedVals:  Values{},
			expectedFound: false,
			req:           nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			labelReq := NewLabelRequirements(tc.req)
			vals, found := labelReq.GetValues(tc.key)
			compareAllUnexportedOpt := cmp.Exporter(func(t reflect.Type) bool { return true })
			if diff := cmp.Diff(tc.expectedVals, vals, compareAllUnexportedOpt); diff != "" {
				t.Errorf("GetValues values diff (-want +got):\n%s", diff)
			}
			if found != tc.expectedFound {
				t.Errorf("GetValues found: expected %v, got %v", tc.expectedFound, found)
			}
		})
	}
}

func TestNetworkingRequirements(t *testing.T) {
	for desc, tc := range map[string]struct {
		pod            *apiv1.Pod
		wantNetworkReq NetworkingRequirements
	}{
		"no networking resources": {
			pod: &apiv1.Pod{},
		},
		"multi networking resources": {
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			wantNetworkReq: NetworkingRequirements{
				AdditionalNetworkResources: []string{"networking.gke.io.networks/red-net.IP"},
			},
		},
		"high performance networking resources": {
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/netdev-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
									apiv1.ResourceName("networking.gke.io.networks/netdev-net"):    *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			wantNetworkReq: NetworkingRequirements{
				AdditionalNetworkResources: []string{"networking.gke.io.networks/netdev-net", "networking.gke.io.networks/netdev-net.IP"},
			},
		},
		"networking resources in multiple containers": {
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/blue-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			wantNetworkReq: NetworkingRequirements{
				AdditionalNetworkResources: []string{"networking.gke.io.networks/blue-net.IP", "networking.gke.io.networks/red-net.IP"},
			},
		},
		"networking resources in one of containers": {
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceMemory: *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			wantNetworkReq: NetworkingRequirements{
				AdditionalNetworkResources: []string{"networking.gke.io.networks/red-net.IP"},
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotReq := GetRequirements(tc.pod)
			sort.Strings(gotReq.NetworkingReq.AdditionalNetworkResources)
			if diff := cmp.Diff(tc.wantNetworkReq, gotReq.NetworkingReq); diff != "" {
				t.Errorf("GetRequirements.NetworkingReq values diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInterfaceAnnotation(t *testing.T) {
	for desc, tc := range map[string]struct {
		pod            *apiv1.Pod
		wantAnnotation string
	}{
		"no annotations": {
			pod: &apiv1.Pod{},
		},
		"empty annotations": {
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
		},
		"interface annotation specified": {
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						labels.NetworkingInterfaceAnnotationKey: "interface-test",
					},
				},
			},
			wantAnnotation: "interface-test",
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotReq := GetRequirements(tc.pod)
			if diff := cmp.Diff(tc.wantAnnotation, gotReq.NetworkingAnnotation); diff != "" {
				t.Errorf("GetRequirements.NetworkingAnnotation values diff (-want +got):\n%s", diff)
			}
		})
	}
}
