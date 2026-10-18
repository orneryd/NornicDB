package fabric

import "context"

type graphSelectionContextKey struct{}

// GraphSelectionFromContext preserves the catalog reference that selected a
// local/remote location, including its alias, after location resolution.
func GraphSelectionFromContext(ctx context.Context) string {
	selection, _ := ctx.Value(graphSelectionContextKey{}).(string)
	return selection
}
