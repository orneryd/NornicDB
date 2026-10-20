package fabric

import "context"

// GraphSelectionFromContext preserves the catalog reference that selected a
// local/remote location, including its alias, after location resolution.
func GraphSelectionFromContext(ctx context.Context) string {
	return executionValuesFromContext(ctx).graph
}
