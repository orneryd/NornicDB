package storage

// appendDoubled bounds copying for commit accumulators that can hold thousands
// of entries. Capacity doubles only when full; existing entry order is retained.
func appendDoubled[T any](items []T, item T) []T {
	if len(items) == cap(items) {
		grown := make([]T, len(items), max(1, 2*len(items)))
		copy(grown, items)
		items = grown
	}
	return append(items, item)
}
