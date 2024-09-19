package podrequirements

import (
	"fmt"
	"sort"
	"strings"

	apiv1 "k8s.io/api/core/v1"
)

// ExtractWorkloadID produces string describing dedicated workload from a node
func ExtractWorkloadID(node *apiv1.Node) string {
	if node == nil {
		return ""
	}
	var keyValuePairs []string
	for _, taint := range node.Spec.Taints {
		// if gkelabels.IsSystemLabel(taint.Key) {
		// 	continue
		// }
		if taint.Effect != apiv1.TaintEffectNoSchedule && taint.Effect != apiv1.TaintEffectNoExecute {
			continue
		}
		// Check for matching label
		labelValue, found := node.Labels[taint.Key]
		if !found || taint.Value != labelValue {
			// Check for matching resource
			_, found = node.Status.Capacity[apiv1.ResourceName(taint.Key)]
			if !found {
				continue
			}
		}
		pair := fmt.Sprintf("%s:%s:%s", taint.Effect, taint.Key, taint.Value)
		keyValuePairs = append(keyValuePairs, pair)
	}
	sort.Strings(keyValuePairs)
	return strings.Join(keyValuePairs, ",")
}
