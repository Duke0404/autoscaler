package podsharding

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	klog "k8s.io/klog/v2"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// PredicatePodShardingCloudProvider is cloud provider interface extended for predicate pod sharding use cases.
type PredicatePodShardingCloudProvider interface {
	cloudprovider.CloudProvider

	IsNodeAutoprovisioningEnabled() bool
	GetExistingNodeGroupLocations() []string
	GetAutoprovisioningLocations() []string
	GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily
}

// PredicatePodShardFilter implements PodShardFilter interface. Initial set of pods is based on list of pod UIDs stored in
// PodShard. Then list is extended by testing scheduler predicates for other pods on test NodeInfos build via cloudprovider
// based on PodShard.NodeGroupDescriptor.
type PredicatePodShardFilter struct{}

// NewPredicatePodShardFilter creates an instance of PredicatePodShardFilter
func NewPredicatePodShardFilter() *PredicatePodShardFilter {
	return &PredicatePodShardFilter{}
}

// FilterPods filters pod list against PodShard
func (p *PredicatePodShardFilter) FilterPods(context *context.AutoscalingContext, selectedPodShard *PodShard, allPodShards []*PodShard, pods []*apiv1.Pod) (PodFilteringResult, error) {
	cloudProvider, ok := context.CloudProvider.(PredicatePodShardingCloudProvider)
	if !ok {
		klog.Fatalf("Could not cast context.CloudProvider to PredicatePodShardingCloudProvider")
	}
	autoprovisioningCloudProvider, ok := context.CloudProvider.(napcloudprovider.AutoprovisioningCloudProvider)
	if !ok {
		klog.Fatalf("Could not cast context.CloudProvider to AutoprovisioningCloudProvider")
	}
	machineSelector := machineselection.Selector{CloudProvider: autoprovisioningCloudProvider}

	podsByUid := make(map[types.UID]*apiv1.Pod)
	for _, pod := range pods {
		podsByUid[pod.UID] = pod
	}

	if len(selectedPodShard.PodUids) < 1 {
		return PodFilteringResult{}, fmt.Errorf("not enough pods associated to the selected PodShard")
	}
	selectedPod := podsByUid[selectedPodShard.PodUidsSlice()[0]]

	extensionNodeInfos := p.getExtensionNodeInfosForShard(cloudProvider, selectedPodShard, machineSelector, selectedPod)

	// list of shards for which we want to have
	finalPodShards := make(map[ShardSignature]bool)

	// add selected shard to list of final shards
	finalPodShards[selectedPodShard.Signature()] = true

	zoneAgnostic := true
	podsMatchingPredicates := make(map[types.UID]bool)
	if len(extensionNodeInfos) > 0 {
		firstLocation := true
		for _, extensionNodeInfo := range extensionNodeInfos {
			context.ClusterSnapshot.Fork()

			var pods []*apiv1.Pod
			for _, podInfo := range extensionNodeInfo.Pods {
				pods = append(pods, podInfo.Pod)
			}
			if err := context.ClusterSnapshot.AddNodeWithPods(extensionNodeInfo.Node(), pods); err != nil {
				context.ClusterSnapshot.Revert()
				return PodFilteringResult{}, err
			}

			for _, shard := range allPodShards {
				// Don't expand non-Provisioning Request pod shards with Provisioning Request ones and vice versa
				if shard.NodeGroupDescriptor.ProvisioningClassName != selectedPodShard.NodeGroupDescriptor.ProvisioningClassName {
					continue
				}
				signature := shard.Signature()
				for podUid := range shard.PodUids {
					pod := podsByUid[podUid]
					predicateErr := context.PredicateChecker.CheckPredicates(context.ClusterSnapshot, pod, extensionNodeInfo.Node().Name)
					podMatchesPredicates := predicateErr == nil
					// TODO(b/132594875) Consider using PredicateMeta

					if !firstLocation && (podMatchesPredicates != podsMatchingPredicates[podUid]) {
						zoneAgnostic = false
					}
					if podMatchesPredicates {
						// mark that a pod matched predicates
						podsMatchingPredicates[podUid] = true

						// if pod from shard matches predicates we want to extend the final list of pod with pods
						// from this shard
						finalPodShards[signature] = true
					}

				}
			}
			firstLocation = false
			context.ClusterSnapshot.Revert()
		}
	}

	if len(extensionNodeInfos) < 2 {
		// If we did not have at least 2 locations we are not making any assumptions on whether pods are zone agnostic or not
		zoneAgnostic = false
	}

	// iterate over all selected shards and build final set of Pod UIDs
	selectedPodUids := make(map[types.UID]bool)
	for _, shard := range allPodShards {
		if finalPodShards[shard.Signature()] {
			for podUid := range shard.PodUids {
				selectedPodUids[podUid] = true
			}
		}
	}

	// translate UIDs of selected Pods to Pods
	var selectedPods []*apiv1.Pod
	for _, pod := range pods {
		if selectedPodUids[pod.UID] {
			selectedPods = append(selectedPods, pod)
		}
	}

	return PodFilteringResult{
		Pods:         selectedPods,
		ZoneAgnostic: zoneAgnostic,
	}, nil
}

func (p *PredicatePodShardFilter) getExtensionNodeInfosForShard(cloudProvider PredicatePodShardingCloudProvider, shard *PodShard, machineSelector machineselection.Selector, pod *apiv1.Pod) []*schedulerframework.NodeInfo {
	// If Provisioning Request shard was selected we don't want to expand the number of pods.
	// This avoids situations where one pod shard contains both in-memory injected pods and actual pods from the cluster.
	if shard.NodeGroupDescriptor.ProvisioningClassName != "" {
		return nil
	}

	// machines to try from biggest to smallest.
	// we need to consider smaller ones because if we request machine with GPUs we are constrained on number of CPUs
	podRequirements := podrequirements.GetRequirements(pod)
	var gpuLabel = ""
	if g, ok := shard.NodeGroupDescriptor.SystemLabels[labels.GPULabel]; ok {
		gpuLabel = g
	}
	var tpuLabel = ""
	if t, ok := shard.NodeGroupDescriptor.SystemLabels[labels.TPULabel]; ok {
		tpuLabel = t
	}
	expansionMachineTypeName := getExpansionMachineTypeName(machineSelector, podRequirements, gpuLabel, tpuLabel)

	locations := p.getExtensionNodeLocations(cloudProvider)

	if len(locations) == 0 {
		klog.Warningf("Error building extension nodeInfo for shard %v; no node locations", shard.Signature())
		return nil
	}

	var nodeInfos []*schedulerframework.NodeInfo
	descriptor := shard.NodeGroupDescriptor.DeepCopy()
	for _, location := range locations {
		// TODO(lukaszos) add logic to check all valid locations and see of pod set matching all of those is the same to avoid extra checks later down the execution path.

		descriptor.SystemLabels[apiv1.LabelZoneFailureDomain] = location
		descriptor.SystemLabels[labels.NapNodePoolHashLabel] = fmt.Sprintf("%v-%s", podRequirements, location)

		nodeGroup, err := cloudProvider.NewNodeGroup(expansionMachineTypeName, descriptor.Labels, descriptor.SystemLabels, descriptor.Taints, descriptor.ExtraResources)
		if err != nil {
			klog.Infof("Could not build extension nodeInfo for shard %v, machine type %v, location=%v; error on NewNodeGroup(); %v", shard.Signature(), expansionMachineTypeName, location, err)
			continue
		}
		nodeInfo, err := nodeGroup.TemplateNodeInfo()
		if err != nil {
			klog.Warningf("Could not build extension nodeInfo for shard %v, machine type %v, location=%v; error on TemplateNodeInfo(); %v", shard.Signature(), expansionMachineTypeName, location, err)
			continue
		}
		klog.Infof("Built extension nodeInfo for shard %v, machine type %v, location=%v", shard.Signature(), expansionMachineTypeName, location)
		nodeInfos = append(nodeInfos, nodeInfo)
	}
	if len(nodeInfos) == 0 {
		klog.Warningf("Could not build any extension node for shard %v", shard.Signature())
	}
	return nodeInfos
}

func getExpansionMachineTypeName(selector machineselection.Selector, requirements *podrequirements.Requirements, gpuLabel, tpuLabel string) string {
	// TODO(bwroblewski): We don't include the npcRule and wantsSpot in machine spec selection as it does not work well conceptually.
	//The long term solution is to remove the sharding entirely and replace it with the pod requirements.
	wantsSpot := false
	machineSpec, _, err := selector.Select(requirements.LabelReq, gpuLabel, tpuLabel, nil, wantsSpot)
	if err != nil {
		klog.Warningf("Could not get machine family for %v: %v", requirements.LabelReq, err)
		return selector.CloudProvider.GetAutoprovisioningDefaultFamily().LargestAutoprovisionedMachineType(machinetypes.NoConstraints).Name
	}
	return machineSpec.LargestAutoprovisionedMachineType().Name
}

func (p *PredicatePodShardFilter) getExtensionNodeLocations(cloudProvider PredicatePodShardingCloudProvider) []string {
	existingLocations := cloudProvider.GetExistingNodeGroupLocations()
	var autoprovisioningLocations []string

	if cloudProvider.IsNodeAutoprovisioningEnabled() {
		autoprovisioningLocations = cloudProvider.GetAutoprovisioningLocations()
	}

	finalLocationsMap := make(map[string]bool)
	for _, location := range existingLocations {
		finalLocationsMap[location] = true
	}
	for _, location := range autoprovisioningLocations {
		finalLocationsMap[location] = true
	}

	var finalLocations []string
	for location := range finalLocationsMap {
		finalLocations = append(finalLocations, location)
	}
	return finalLocations
}
