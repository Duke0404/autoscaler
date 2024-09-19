package podrequirements

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// InvalidLabelValueErrorType - an invalid label value is specified
const InvalidLabelValueErrorType errors.AutoscalerErrorType = "invalidLabelValue"

// ErrInvalidLabelValue represents an error caused by invalid label value
type ErrInvalidLabelValue struct {
	Prefix string
	Label  string
	Value  string
}

// Error returns an error message applicable for the error type.
func (e *ErrInvalidLabelValue) Error() string {
	return e.Prefix + fmt.Sprintf("pod requests an invalid value %q for label %q", e.Value, e.Label)
}

// Type returns the type of the error.
func (e *ErrInvalidLabelValue) Type() errors.AutoscalerErrorType {
	return InvalidLabelValueErrorType
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrInvalidLabelValue) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewInvalidLabelValueError creates a specific error type.
func NewInvalidLabelValueError(label, value string) errors.AutoscalerError {
	return &ErrInvalidLabelValue{Label: label, Value: value}
}
