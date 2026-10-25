// Package collection owns deterministic map views and independent slice-map snapshots.
package collection

import (
	"maps"
	"slices"
	"sync"
)

// SortedKeys returns a non-nil ordered key slice with one sized allocation.
func SortedKeys[V any](values map[string]V) []string {
	keys := slices.AppendSeq(make([]string, 0, len(values)), maps.Keys(values))
	slices.Sort(keys)
	return keys
}

// CloneSliceValues returns an independent map and independent value slices.
// Empty input maps remain non-nil; nil and empty value slices normalize to nil.
func CloneSliceValues[K comparable, V any](values map[K][]V) map[K][]V {
	cloned := maps.Clone(values)
	if cloned == nil {
		return make(map[K][]V)
	}
	for key, value := range cloned {
		cloned[key] = slices.Concat(value)
	}
	return cloned
}

// ReadSnapshot runs read while holding the collection owner's read lock.
// The callback loads the current collection under that same lock, including
// owners that replace their map rather than mutating it in place.
func ReadSnapshot[T any](mu *sync.RWMutex, read func() T) T {
	mu.RLock()
	defer mu.RUnlock()
	return read()
}
