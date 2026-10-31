package util

import "errors"

// ErrorIsOnly reports that every non-nil leaf is the expected status. Testing
// errors.Is on an aggregate only establishes that one branch matches; it must
// not suppress a different cleanup outcome joined beside that branch.
func ErrorIsOnly(err, target error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		found := false
		for _, child := range joined.Unwrap() {
			if child == nil {
				continue
			}
			found = true
			if !ErrorIsOnly(child, target) {
				return false
			}
		}
		return found
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return ErrorIsOnly(child, target)
		}
	}
	return errors.Is(err, target)
}
