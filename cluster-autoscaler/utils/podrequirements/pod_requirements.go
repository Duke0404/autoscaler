package podrequirements

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	networkingutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
	klog "k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	provreq_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
)

// InvalidWorkloadSeparationError means that a pod has an invalid workload separation config (e.g. a node selector for a
// non-system label, but no toleration for the corresponding taint).
const InvalidWorkloadSeparationError errors.AutoscalerErrorType = "InvalidWorkloadSeparationError"

// ErrInvalidWorkloadSeparation is an instance of AutoscalerError with InvalidWorkloadSeparationError type and an appropriate
// error message.
var ErrInvalidWorkloadSeparation = errors.NewAutoscalerError(InvalidWorkloadSeparationError, "pod requests an unknown label without a matching toleration")

// Values represents possible values expressed by node selector/affinity: a set of strings (most commonly a singleton), or
// the special "Any" value.
type Values struct {
	vals map[string]bool
	any  bool
}

// Get returns the inner string values. Will return nil for the "Any" value.
func (v Values) Get() map[string]bool {
	return v.vals
}

// GetSingle returns a single value and true if these Values represent exactly one value. If that's not the case, an empty string and false are returned.
func (v Values) GetSingle() (string, bool) {
	if v.IsAny() || len(v.vals) != 1 {
		return "", false
	}
	for val := range v.vals {
		return val, true
	}
	return "", false
}

// IsAny returns whether this Values object is the special "Any" value.
func (v Values) IsAny() bool {
	return v.any
}

// NewValues creates a new Values object containing actual strings.
func NewValues(vals ...string) Values {
	valSet := map[string]bool{}
	for _, val := range vals {
		valSet[val] = true
	}
	return Values{vals: valSet}
}

// AnyValue returns a new Values object representing the special "Any" value.
func AnyValue() Values {
	return Values{any: true}
}

// LabelRequirements represents the scheduling label requirements of a pod extracted from node selector and required affinity.
// The values can either be a set of strings, or the special "Any" value. Set of strings means that the pod requires that
// particular key and any of the strings as value. "Any" value means that the pod requires a key and doesn't care about the
// value (this can be expressed using the Exists operator in node affinity).
//
// Note: If a pod has node selector/affinity defined for the same key in multiple places (both selector and affinity,
//
//	multiple affinity terms, multiple affinity expressions), the resulting LabelRequirements only take one arbitrary
//	place into account. This means that the behavior is pretty much undefined in this case (two places where affinity
//	for the same key is defined could be compatible, or in conflict with each other - we'll still pick only one of them
//	arbitrarily). Ideally we should probably be rejecting any such config as an error. Doing this would require adding
//	a lot of error-handling code across the code base, and considering backwards-compatibility for configs that have
//	historically been allowed, but really result in undefined behavior.
//	TODO(b/235823013): Rethink podrequirements error policy.
type LabelRequirements struct {
	// This could technically be the whole object, but then we couldn't prevent accessing the map directly.
	req map[string]Values
}

// GetValues returns the Values for the given key, and a bool specifying whether the key is present in the requirements.
// If the key is not present, the zero value for Values is returned in the first parameter.
func (lr LabelRequirements) GetValues(key string) (Values, bool) {
	val, found := lr.req[key]
	return val, found
}

// GetSingleValue returns the string value specified for the given key and true, if exactly one value is specified for the key.
// If that's not the case, an empty string and false are returned.
func (lr LabelRequirements) GetSingleValue(key string) (string, bool) {
	val, found := lr.req[key]
	if !found {
		return "", false
	}
	return val.GetSingle()
}

// GetFirstKeyValueMatch returns the key-value pair for the first key that match regex pattern.
// If none found, an empty strings and false are returned.
func (lr LabelRequirements) GetFirstKeyValueMatch(pattern string) (string, string, bool) {
	for key, vals := range lr.req {
		matched, _ := regexp.MatchString(pattern, key)
		if matched {
			value, ok := vals.GetSingle()
			return key, value, ok
		}
	}
	return "", "", false
}

// KeysWithPrefix returns all keys with a given prefix that are present in the requirements.
func (lr LabelRequirements) KeysWithPrefix(prefix string) map[string]bool {
	result := map[string]bool{}
	for key := range lr.req {
		if strings.HasPrefix(key, prefix) {
			result[key] = true
		}
	}
	return result
}

// NewLabelRequirements returns a new LabelRequirements object.
func NewLabelRequirements(req map[string]Values) LabelRequirements {
	return LabelRequirements{req: req}
}

// QueuedProvisioningRequirements contains information about Provisioning Request requirements.
type QueuedProvisioningRequirements struct {
	Enabled           bool
	ResizeRequestName string
}

// Requirements contain information about scheduling requirements of a pod.
type Requirements struct {
	// Toleration lists all tolerations for "hard" (not preferred) taints.
	Tolerations []apiv1.Toleration
	// LabelReq contains scheduling label requirements of a pod.
	LabelReq                    LabelRequirements
	PodCapacity                 string
	NetworkingReq               NetworkingRequirements
	NetworkingAnnotation        string
	QueuedProvisioningReq       QueuedProvisioningRequirements
	DiskEncryptionKeyAnnotation string
}

type NetworkingRequirements struct {
	AdditionalNetworkResources []string
}

// PrivateNodeRequirement contains information about private nodes requirement
type PrivateNodeRequirement string

const (
	// PrivateNodeRequirementUnspecified - private nodes requirement is unspecified
	PrivateNodeRequirementUnspecified = PrivateNodeRequirement("")
	// PrivateNodeRequirementFalse - private nodes requirement is false
	PrivateNodeRequirementFalse = PrivateNodeRequirement("false")
	// PrivateNodeRequirementTrue - private nodes requirement is true
	PrivateNodeRequirementTrue = PrivateNodeRequirement("true")
)

// GetPrivateNodeRequirement returns if pod specify private/public node.
func (r *Requirements) GetPrivateNodeRequirement() (PrivateNodeRequirement, errors.AutoscalerError) {
	val, found := r.LabelReq.GetSingleValue(gkelabels.PrivateNodeLabel)
	if !found {
		return PrivateNodeRequirementUnspecified, nil
	}
	_, err := strconv.ParseBool(val)
	if err != nil {
		return PrivateNodeRequirementUnspecified, NewInvalidLabelValueError(gkelabels.PrivateNodeLabel, val)
	}
	return PrivateNodeRequirement(val), nil
}

// WorkloadSeparationTaintsAndLabels returns taints and labels containing the same keys.
// They do not include taints or labels containing non allow listed system keys.
//
// Also if the label requirements have at least one non-system key for which there
// does not exist a corresponding toleration, the function returns an error.
// It does that because we don't want a situation where a pod with insufficient
// tolerations may be picked by NAP and block creation of new node-pools.
func (r *Requirements) WorkloadSeparationTaintsAndLabels(checker *WorkloadSeparationLabelsChecker) ([]apiv1.Taint, map[string]string, errors.AutoscalerError) {
	taints := make([]apiv1.Taint, 0)
	labels := make(map[string]string)
	for _, t := range r.Tolerations {
		value, matched := workloadSeparationMatch(t, r.LabelReq)
		if checker.canLabelSeparateWorkloads(t.Key) && matched {
			taints = append(taints, apiv1.Taint{Key: t.Key, Value: value, Effect: t.Effect})
			labels[t.Key] = value
		}
	}
	for k := range r.LabelReq.req {
		_, isLabelWithTaint := labels[k]
		if !gkelabels.IsSystemLabel(k) && !isLabelWithTaint {
			return nil, nil, ErrInvalidWorkloadSeparation
		}
	}
	return taints, labels, nil
}

// WorkloadSeparationLabelsChecker is used to check whether given label can
// separate workloads.
type WorkloadSeparationLabelsChecker struct {
	allowlistedRegexps []*regexp.Regexp
}

// NewWorkloadSeparationWorkloadChecker returns new instance of WorkloadSeparationLabelsChecker.
func NewWorkloadSeparationWorkloadChecker(allowlistedSystemLabelPatterns []string) *WorkloadSeparationLabelsChecker {
	allowlistedRegexps := make([]*regexp.Regexp, 0, len(allowlistedSystemLabelPatterns))
	for _, pattern := range allowlistedSystemLabelPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			klog.Errorf("Invalid allowlisted system label pattern %v, ignoring: %v", pattern, err)
			continue
		}
		allowlistedRegexps = append(allowlistedRegexps, re)
	}
	return &WorkloadSeparationLabelsChecker{
		allowlistedRegexps: allowlistedRegexps,
	}
}

func (c *WorkloadSeparationLabelsChecker) canLabelSeparateWorkloads(label string) bool {
	if !gkelabels.IsSystemLabel(label) {
		return true
	}

	for _, pattern := range c.allowlistedRegexps {
		if pattern.MatchString(label) {
			return true
		}
	}

	return false
}

// GetRequirements returns information about scheduling requirements of a pod.
func GetRequirements(pod *apiv1.Pod) *Requirements {
	nodeSelectors := getNodeSelectors(pod)
	nodeAffinities := getNodeAffinities(pod)
	podAnnotations := pod.GetAnnotations()
	labelReq := make(map[string]Values)

	for key, val := range nodeSelectors {
		labelReq[key] = val

		if gkelabels.IsResourceLabel(key) {
			if str, single := val.GetSingle(); !single || str != "" {
				klog.Warningf("Resource label contains non-empty string value, ignoring.")
				continue
			}

			if resourceLabelValue, err := getResourceLabel(key, podAnnotations); err != nil {
				klog.Warningf("Unable to get resource label for %s: %v", key, err)
			} else {
				// HACK: modifying resource value label value to contain referenced value instead
				labelReq[key] = NewValues(resourceLabelValue)
			}
		}
	}

	for k, affinityVal := range nodeAffinities {
		if selectorVal, found := nodeSelectors[k]; found {
			if affinityVal.IsAny() {
				klog.Warningf("Pod %s/%s, specifies both node selector for label %s with value %v and Exists affinity. Ignoring affinity.", pod.Namespace, pod.Name, k, selectorVal.Get())
				continue
			} else {
				klog.Warningf("Pod %s/%s, specifies both node selector for label %s with value %v and affinity for value %v. Ignoring node selector.", pod.Namespace, pod.Name, k, selectorVal.Get(), affinityVal.Get())
			}
		}
		labelReq[k] = affinityVal
	}

	podCap := getPodPerVMRequirements(pod)

	var queuedProvisioningReq QueuedProvisioningRequirements
	if resizeRequestName, found := provreq_pods.ProvisioningRequestName(pod); found {
		queuedProvisioningReq = QueuedProvisioningRequirements{true, resizeRequestName}
	}

	networkingAnnotation := strings.ReplaceAll(podAnnotations[gkelabels.NetworkingInterfaceAnnotationKey], "\n", "")

	var diskEncryptionKeyAnnotation string
	if bootDiskEncryptionAnnotationKey, ok := labelReq[gkelabels.BootDiskEncryptionLabelKey]; ok {
		var err error
		diskEncryptionKeyAnnotation, err = getBootDiskEncryptionKey(bootDiskEncryptionAnnotationKey, podAnnotations)
		if err != nil {
			klog.Warningf("Unable to get boot disk encryption annotation value: %v", err)
		}
	}

	return &Requirements{
		Tolerations:                 getTolerations(pod),
		LabelReq:                    LabelRequirements{req: labelReq},
		PodCapacity:                 podCap,
		NetworkingReq:               getNetworkingRequirements(pod),
		NetworkingAnnotation:        networkingAnnotation,
		QueuedProvisioningReq:       queuedProvisioningReq,
		DiskEncryptionKeyAnnotation: diskEncryptionKeyAnnotation,
	}
}

func getBootDiskEncryptionKey(labelValue Values, annotations map[string]string) (string, error) {
	diskEncryptionAnnotation, single := labelValue.GetSingle()
	if !single {
		return "", fmt.Errorf("multiple values set for boot disk encryption key")
	}

	diskEncryptionKey, ok := annotations[diskEncryptionAnnotation]
	if !ok {
		return "", fmt.Errorf("boot disk encryption key is not found under %q annotation", diskEncryptionAnnotation)
	}

	return diskEncryptionKey, nil
}

// getResourceLabel extracts referenced node selector value which is contained inside pod annotations
func getResourceLabel(labelKey string, annotations map[string]string) (string, error) {
	if !gkelabels.IsResourceLabel(labelKey) {
		return "", fmt.Errorf("invalid resource label key")
	}

	annotationKey := gkelabels.ExtractResourceLabelAnnotationKey(labelKey)
	if annotationValue, ok := annotations[annotationKey]; ok {
		return annotationValue, nil
	}

	return "", fmt.Errorf(`unable to find annotation with key "%s"`, annotationKey)
}

// getPodPerVMRequirements returns the pod-per-vm capacity value requested
// as a string or empty string if non pod-per-vm capacity is requested.
func getPodPerVMRequirements(pod *apiv1.Pod) string {
	var podSlotCount resource.Quantity
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			podSlotCount.Add(container.Resources.Requests[gkelabels.PodCapacityLabel])
		}
	}
	if podSlotCount.Value() != 0 {
		return podSlotCount.String()
	}
	return ""
}

// getNetworkingRequirements returns AdditionalNetworkResources associated with
// a given pod based on resource requests of pod containers.
func getNetworkingRequirements(pod *apiv1.Pod) NetworkingRequirements {
	res := NetworkingRequirements{}
	for _, c := range pod.Spec.Containers {
		networks := networkingutils.GetNetworkResourcesNamesFromResourceList(c.Resources.Requests)
		if len(networks) > 0 {
			res.AdditionalNetworkResources = append(res.AdditionalNetworkResources, networks...)
		}
	}
	return res
}

// getTolerations returns all tolerations for NoSchedule and NoExecute effects.
func getTolerations(pod *apiv1.Pod) []apiv1.Toleration {
	var result []apiv1.Toleration
	for _, t := range pod.Spec.Tolerations {
		if t.Effect == apiv1.TaintEffectNoSchedule || t.Effect == apiv1.TaintEffectNoExecute || string(t.Effect) == "" {
			result = append(result, t)
		}
	}
	return result
}

// getNodeSelectors returns all node selector preferences.
func getNodeSelectors(pod *apiv1.Pod) map[string]Values {
	result := make(map[string]Values)
	if pod.Spec.NodeSelector == nil {
		return result
	}
	for k, v := range pod.Spec.NodeSelector {
		result[k] = NewValues(v)
	}
	return result
}

// getNodeAffinities return these node affinity preferences which contain
// affinity only for a single value.
func getNodeAffinities(pod *apiv1.Pod) map[string]Values {
	result := make(map[string]Values)
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return result
	}
	for _, ns := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, expression := range ns.MatchExpressions {
			if expression.Operator == apiv1.NodeSelectorOpIn {
				result[expression.Key] = NewValues(expression.Values...)
			}
			if expression.Operator == apiv1.NodeSelectorOpExists {
				// We don't want to override In affinities with Exists affinities, since Exists affinities with the same
				// key will always be more permissive.
				if _, alreadyPresent := result[expression.Key]; !alreadyPresent {
					result[expression.Key] = AnyValue()
				}
			}
		}
	}
	return result
}

func workloadSeparationMatch(toleration apiv1.Toleration, labelReq LabelRequirements) (string, bool) {
	v, isSpecified := labelReq.GetSingleValue(toleration.Key)
	if isSpecified && (toleration.Operator == apiv1.TolerationOpExists || toleration.Value == v) {
		return v, true
	}
	return "", false
}
