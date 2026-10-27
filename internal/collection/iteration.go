package collection

// ApplyInOrder iterates over items in order, calling apply for each.
// On first error, returns immediately without applying remaining items.
// Empty slices return nil (no calls). Nil slice is valid input with no calls.
// This helper provides first-error semantics: once an error occurs, the rest
// of the slice is not processed and no atomicity is guaranteed.
func ApplyInOrder[T any](items []T, apply func(T) error) error {
	for _, item := range items {
		if err := apply(item); err != nil {
			return err
		}
	}
	return nil
}
