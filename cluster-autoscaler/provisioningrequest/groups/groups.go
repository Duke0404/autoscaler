/*
Copyright 2019 The Kubernetes Authors.

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

package groups

import (
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/equivalence"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pr_pods "k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/pods"
)

// ProvisioningRequestGroup contains pod equivalence groups that are part of the same provisioning request.
type ProvisioningRequestGroup struct {
	ControllerRef *metav1.OwnerReference
	PodGroups     []*equivalence.PodGroup
}

// CreateProvisioningRequestGroups creates provisioning request groups from pod equivalence groups.
func CreateProvisioningRequestGroups(podGroups []*equivalence.PodGroup) []*ProvisioningRequestGroup {
	provisioningRequestGroups := []*ProvisioningRequestGroup{}

	for _, podGroup := range podGroups {
		representant := podGroup.Pods[0]
		// Skip pods that are not controlled by a provisioning request.
		if _, found := pr_pods.ProvisioningClassName(representant); !found {
			continue
		}

		controllerRef := drain.ControllerRef(representant)

		// Find the provisioning request group that the pod belongs to.
		var provisioningRequestGroup *ProvisioningRequestGroup

		for _, group := range provisioningRequestGroups {
			if group.ControllerRef.UID == controllerRef.UID {
				provisioningRequestGroup = group
				break
			}
		}

		if provisioningRequestGroup == nil {
			provisioningRequestGroup = &ProvisioningRequestGroup{
				ControllerRef: controllerRef,
			}
			provisioningRequestGroups = append(provisioningRequestGroups, provisioningRequestGroup)
		}

		provisioningRequestGroup.PodGroups = append(provisioningRequestGroup.PodGroups, podGroup)
	}

	return provisioningRequestGroups
}