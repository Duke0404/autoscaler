/*
Copyright 2024 The Kubernetes Authors.

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

package checkcapacity

import (
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/processors/provreq"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/conditions"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/klog/v2"

	ca_processors "k8s.io/autoscaler/cluster-autoscaler/processors"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

type checkCapacityProvClass struct {
	context                         *context.AutoscalingContext
	client                          *provreqclient.ProvisioningRequestClient
	injector                        *scheduling.HintingSimulator
	batchProcessing                 bool
	maxBatchSize                    int
	batchTimebox                    time.Duration
	provisioningRequestPodsInjector *provreq.ProvisioningRequestPodsInjector
}

// New create check-capacity scale-up mode.
func New(
	client *provreqclient.ProvisioningRequestClient,
	provisioningRequestPodsInjector *provreq.ProvisioningRequestPodsInjector,
) *checkCapacityProvClass {
	return &checkCapacityProvClass{client: client, provisioningRequestPodsInjector: provisioningRequestPodsInjector}
}

func (o *checkCapacityProvClass) Initialize(
	autoscalingContext *context.AutoscalingContext,
	processors *ca_processors.AutoscalingProcessors,
	clusterStateRegistry *clusterstate.ClusterStateRegistry,
	estimatorBuilder estimator.EstimatorBuilder,
	taintConfig taints.TaintConfig,
	injector *scheduling.HintingSimulator,
) {
	o.context = autoscalingContext
	o.injector = injector
	o.batchProcessing = autoscalingContext.CheckCapacityBatchProcessing
	o.batchTimebox = autoscalingContext.BatchTimebox
	o.maxBatchSize = autoscalingContext.MaxBatchSize
}

// Provision return if there is capacity in the cluster for pods from ProvisioningRequest.
func (o *checkCapacityProvClass) Provision(
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*schedulerframework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	combinedStatus := NewCombinedStatusSet()
	provisioningRequestsProcessed := 0
	startTime := time.Now()

	o.context.ClusterSnapshot.Fork()
	defer o.context.ClusterSnapshot.Revert()

	for len(unschedulablePods) > 0 {
		prs := provreqclient.ProvisioningRequestsForPods(o.client, unschedulablePods)
		prs = provreqclient.FilterOutProvisioningClass(prs, v1.ProvisioningClassCheckCapacity)
		if len(prs) == 0 {
			break
		}

		// Pick 1 ProvisioningRequest.
		pr := prs[0]

		scaleUpIsSuccessful, err := o.checkcapacity(unschedulablePods, pr)
		if err != nil {
			st, err := status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerError(errors.InternalError, "error during ScaleUp: %s", err.Error()))

			if o.batchProcessing {
				combinedStatus.Add(st)
			} else {
				return st, err
			}
		}
		if scaleUpIsSuccessful {
			combinedStatus.Add(&status.ScaleUpStatus{Result: status.ScaleUpSuccessful})
		} else {
			combinedStatus.Add(&status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable})
		}

		if !o.batchProcessing {
			break
		}

		if o.provisioningRequestPodsInjector == nil {
			klog.Errorf("ProvisioningRequestPodsInjector is not set, falling back to non-batch processing")
			break
		}

		if o.maxBatchSize <= 1 {
			klog.Errorf("MaxBatchSize is set to %d, falling back to non-batch processing", o.maxBatchSize)
			break
		}

		provisioningRequestsProcessed++
		if provisioningRequestsProcessed >= o.maxBatchSize {
			break
		}

		if time.Since(startTime) > o.batchTimebox {
			klog.Infof("Batch timebox exceeded, processed %d check capacity provisioning requests this iteration", provisioningRequestsProcessed)
			break
		}

		unschedulablePods, err = (*o.provisioningRequestPodsInjector).GetPodsFromNextRequest(func(pr *provreqwrapper.ProvisioningRequest) bool {
			return pr.Spec.ProvisioningClassName == v1.ProvisioningClassCheckCapacity
		})
		if err != nil {
			st, _ := status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerError(errors.InternalError, "error during ScaleUp: %s", err.Error()))

			combinedStatus.Add(st)
		}
	}

	return combinedStatus.Export(), nil
}

// Assuming that all unschedulable pods comes from one ProvisioningRequest.
func (o *checkCapacityProvClass) checkcapacity(unschedulablePods []*apiv1.Pod, provReq *provreqwrapper.ProvisioningRequest) (capacityAvailable bool, err error) {
	capacityAvailable = true
	st, _, err := o.injector.TrySchedulePods(o.context.ClusterSnapshot, unschedulablePods, scheduling.ScheduleAnywhere, true)
	if len(st) < len(unschedulablePods) || err != nil {
		conditions.AddOrUpdateCondition(provReq, v1.Provisioned, metav1.ConditionFalse, conditions.CapacityIsNotFoundReason, "Capacity is not found, CA will try to find it later.", metav1.Now())
		capacityAvailable = false
	} else {
		conditions.AddOrUpdateCondition(provReq, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg, metav1.Now())
	}
	_, updErr := o.client.UpdateProvisioningRequest(provReq.ProvisioningRequest)
	if updErr != nil {
		return false, fmt.Errorf("failed to update Provisioned condition to ProvReq %s/%s, err: %v", provReq.Namespace, provReq.Name, updErr)
	}
	return capacityAvailable, err
}

// combinedStatusSet is a helper struct to combine multiple ScaleUpStatuses into one. It keeps track of the best result and all errors that occurred during the ScaleUp process.
type combinedStatusSet struct {
	Result        status.ScaleUpResult
	ScaleupErrors map[*errors.AutoscalerError]bool
}

// Add adds a ScaleUpStatus to the combinedStatusSet.
func (c *combinedStatusSet) Add(status *status.ScaleUpStatus) {
	// This relies on the fact that the ScaleUpResult enum is ordered in a way that the higher the value, the worse the result. This way we can just take the minimum of the results. If new results are added, either the enum should be updated keeping the order, or a different approach should be used to combine the results.
	if c.Result > status.Result {
		c.Result = status.Result
	}
	if status.ScaleUpError != nil {
		if _, found := c.ScaleupErrors[status.ScaleUpError]; !found {
			c.ScaleupErrors[status.ScaleUpError] = true
		}
	}
}

// formatMessageFromBatchErrors formats a message from a list of errors.
func (c *combinedStatusSet) formatMessageFromBatchErrors(errs []errors.AutoscalerError, printErrorTypes bool) string {
	firstErr := errs[0]
	var builder strings.Builder
	builder.WriteString(firstErr.Error())
	builder.WriteString(" ...and other concurrent errors: [")
	formattedErrs := map[errors.AutoscalerError]bool{
		firstErr: true,
	}
	for _, err := range errs {
		if _, has := formattedErrs[err]; has {
			continue
		}
		formattedErrs[err] = true
		var message string
		if printErrorTypes {
			message = fmt.Sprintf("[%s] %s", err.Type(), err.Error())
		} else {
			message = err.Error()
		}
		if len(formattedErrs) > 2 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%q", message))
	}
	builder.WriteString("]")
	return builder.String()
}

// combineBatchScaleUpErrors combines multiple errors into one. If there is only one error, it returns that error. If there are multiple errors, it combines them into one error with a message that contains all the errors.
func (c *combinedStatusSet) combineBatchScaleUpErrors() *errors.AutoscalerError {
	if len(c.ScaleupErrors) == 0 {
		return nil
	}
	if len(c.ScaleupErrors) == 1 {
		for err := range c.ScaleupErrors {
			return err
		}
	}
	uniqueMessages := make(map[string]bool)
	uniqueTypes := make(map[errors.AutoscalerErrorType]bool)
	for err := range c.ScaleupErrors {
		uniqueTypes[(*err).Type()] = true
		uniqueMessages[(*err).Error()] = true
	}
	if len(uniqueTypes) == 1 && len(uniqueMessages) == 1 {
		for err := range c.ScaleupErrors {
			return err
		}
	}
	// sort to stabilize the results and easier log aggregation
	errs := make([]errors.AutoscalerError, 0, len(c.ScaleupErrors))
	for err := range c.ScaleupErrors {
		errs = append(errs, *err)
	}
	sort.Slice(errs, func(i, j int) bool {
		errA := errs[i]
		errB := errs[j]
		if errA.Type() == errB.Type() {
			return errs[i].Error() < errs[j].Error()
		}
		return errA.Type() < errB.Type()
	})
	firstErr := errs[0]
	printErrorTypes := len(uniqueTypes) > 1
	message := c.formatMessageFromBatchErrors(errs, printErrorTypes)
	combinedErr := errors.NewAutoscalerError(firstErr.Type(), message)
	return &combinedErr
}

// Export converts the combinedStatusSet into a ScaleUpStatus.
func (c *combinedStatusSet) Export() *status.ScaleUpStatus {
	result := &status.ScaleUpStatus{Result: c.Result}
	if len(c.ScaleupErrors) > 0 {
		result.ScaleUpError = c.combineBatchScaleUpErrors()
	}
	return result
}

// NewCombinedStatusSet creates a new combinedStatusSet.
func NewCombinedStatusSet() combinedStatusSet {
	return combinedStatusSet{
		Result:        status.ScaleUpNotTried,
		ScaleupErrors: make(map[*errors.AutoscalerError]bool),
	}
}
