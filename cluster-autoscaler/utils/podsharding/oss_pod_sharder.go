package podsharding

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	// gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	// "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	// gketpu "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	// "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/autoscaler/cluster-autoscaler/utils/podrequirements"
	klog "k8s.io/klog/v2"
)

func NewPodSharder() PodSharder {
	return NewCompositePodSharder([]FeatureShardComputeFunction{
		{
			"tpu_type",
			computeTpuTypeShard,
		},
		{
			"gpu_type",
			computeGpuTypeShard,
		},
	})
}

func computeGpuTypeShard(pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	var podGpuCount resource.Quantity
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			contanerGpuQuantity, found := container.Resources.Requests[gpu.ResourceNvidiaGPU]
			if found {
				podGpuCount.Add(contanerGpuQuantity)
			}
		}
	}
	if podGpuCount.Value() == 0 {
		return
	}

	req := podrequirements.GetRequirements(pod)
	gpuType := "*"
	if gpuTypeFromSelector, isSpecified := req.LabelReq.GetSingleValue(gkelabels.GPULabel); isSpecified {
		gpuType = gpuTypeFromSelector
	}

	nodeGpuType := gpuType
	if nodeGpuType == "*" {
		// TODO(lukaszos) here we should use the logic which takes into consideration for which gpu we have resource limits set
		nodeGpuType = machinetypes.DefaultGPU
	}
	nodeGroupDescriptor.SystemLabels[gkelabels.GPULabel] = nodeGpuType

	gpuPartitionSize, _ := req.LabelReq.GetSingleValue(gkelabels.GPUPartitionSizeLabel)
	gpuMaxSharedClients, _ := req.LabelReq.GetSingleValue(gkelabels.GPUMaxSharedClientsLabel)
	if gpuMaxSharedClients == "" {
		// If max shared clients label is not set, but a valid sharing strategy is
		// provided, max shared clients will be set to a default value.
		sharingStrategy, found := req.LabelReq.GetSingleValue(gkelabels.GPUSharingStrategyLabel)
		if found && machinetypes.ValidateGpuSharingStrategy(sharingStrategy) == nil {
			gpuMaxSharedClients = machinetypes.DefaultGPUMaxSharedClients
		}
	}
	// We want to group all the pods requesting given type of gpu withing single shard. We use maximum possible number
	// of gpus for given type for building expansion node info.
	maxGpuCountInt64, err := machinetypes.GetMaxGpuCount(nodeGpuType, gpuPartitionSize, gpuMaxSharedClients)
	if err == nil {
		nodeGroupDescriptor.ExtraResources[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(maxGpuCountInt64, resource.DecimalSI)
	} else {
		klog.Warningf("Got error while obtaining maxGpuCount for gpu type %v; %v", gpuType, err)
	}
}

func computeTpuTypeShard(pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	var podTpuCount resource.Quantity
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			contanerTpuQuantity, found := container.Resources.Requests[gketpu.ResourceGoogleTPU]
			if found {
				podTpuCount.Add(contanerTpuQuantity)
			}
		}
	}
	if podTpuCount.Value() == 0 {
		return
	}

	req := podrequirements.GetRequirements(pod)

	tpuType := gketpu.DefaultTPU
	if tpuTypeFromSelector, isSpecified := req.LabelReq.GetSingleValue(gkelabels.TPULabel); isSpecified {
		tpuType = tpuTypeFromSelector
	}
	nodeGroupDescriptor.SystemLabels[gkelabels.TPULabel] = tpuType

	if tpuTopologyFromSelector, isSpecified := req.LabelReq.GetSingleValue(gkelabels.TPUTopologyLabel); isSpecified {
		nodeGroupDescriptor.SystemLabels[gkelabels.TPUTopologyLabel] = tpuTopologyFromSelector
	}

	if acceleratorCountFromSelector, isSpecified := req.LabelReq.GetSingleValue(gkelabels.AcceleratorCountLabel); isSpecified {
		nodeGroupDescriptor.SystemLabels[gkelabels.AcceleratorCountLabel] = acceleratorCountFromSelector
	}

	// We want to group all the pods requesting given type of tpu withing single shard.
	// We use maximum possible number of tpus for given type for building expansion node info.
	maxTpuCountInt64, err := gketpu.GetMaxTpuCount(tpuType)
	if err == nil {
		nodeGroupDescriptor.ExtraResources[gketpu.ResourceGoogleTPU] = *resource.NewQuantity(maxTpuCountInt64, resource.DecimalSI)
	} else {
		klog.Warningf("Got error while obtaining maxTpuCount for tpu type %v; %v", tpuType, err)
	}
}