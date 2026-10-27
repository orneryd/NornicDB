package localization

import "reflect"

// projectedError presents owned display/classification copies while retaining
// the original causal chain for errors.Is, errors.As and errors.Unwrap.
type projectedError struct {
	cause          error
	text           string
	localized      *LocalizedError
	classification error
}

// ProjectError constructs an immutable diagnostic projection. localized and
// classification are caller-owned copies and must not be mutated after this
// call. For example, a credential boundary supplies a redacted localized copy
// while a source mapper may also supply a corrected domain classification.
// Unwrap returns cause directly, preserving existing retry/transport contracts.
func ProjectError(cause error, text string, localized *LocalizedError, classification error) error {
	return &projectedError{cause: cause, text: text, localized: localized, classification: classification}
}

func (e *projectedError) Error() string { return e.text }
func (e *projectedError) Unwrap() error { return e.cause }

func (e *projectedError) As(target any) bool {
	targetValue := reflect.ValueOf(target)
	if !targetValue.IsValid() || targetValue.Kind() != reflect.Pointer || targetValue.IsNil() {
		return false
	}
	targetValue = targetValue.Elem()
	for _, candidate := range [2]error{e.classification, e.localized} {
		value := reflect.ValueOf(candidate)
		if !value.IsValid() || value.Kind() == reflect.Pointer && value.IsNil() {
			continue
		}
		if value.Type().AssignableTo(targetValue.Type()) {
			targetValue.Set(value)
			return true
		}
	}
	return false
}
