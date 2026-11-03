// Author: Claude, NOR-180 composition owner, 30 Oct 2026.

// Package compose owns the one composition of several failures into one error.
// It has no dependencies, so storage and every layer above it share it;
// pkg/errors adds terminal-outcome normalization on top of the same rule.
package compose

import "errors"

// Outcomes composes a primary failure with the other failures of the same
// operation (cleanup, rollback, close, shutdown): the primary stays first,
// nil failures are skipped and a failure the primary already wraps is not
// repeated. With no primary, it aggregates the failures in order.
func Outcomes(primary error, others ...error) error {
	causes := make([]error, 0, len(others)+1)
	if primary != nil {
		causes = append(causes, primary)
	}
	for _, err := range others {
		if err == nil || primary != nil && errors.Is(primary, err) {
			continue
		}
		causes = append(causes, err)
	}
	switch len(causes) {
	case 0:
		return nil
	case 1:
		return causes[0]
	}
	return errors.Join(causes...)
}
