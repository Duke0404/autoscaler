package podsharding

import (
	v1 "k8s.io/api/core/v1"
	pr_pods "k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/pods"
)

func NewOssPodSharder(provisioningRequestsEnabled bool) PodSharder {
	computeFunctions := []FeatureShardComputeFunction{
		{
			"label_name",
			labelNameShard,
		},
	}

	if provisioningRequestsEnabled {
		computeFunctions = append(computeFunctions, FeatureShardComputeFunction{
			"provisioning_request",
			provisioningRequestShard,
		})
	}

	return NewCompositePodSharder(computeFunctions)
}

func labelNameShard(pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	nodeGroupDescriptor.Labels = pod.Labels
}

func provisioningRequestShard(pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	provClass, found := pr_pods.ProvisioningClassName(pod)
	if !found {
		return
	}
	nodeGroupDescriptor.ProvisioningClassName = provClass
}
