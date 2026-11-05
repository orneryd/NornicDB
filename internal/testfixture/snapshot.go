package testfixture

import "sync"

// Snapshot returns a detached shallow copy of a fixture's slice while holding
// its existing owner lock. Pass the address of the slice, not an already-read
// slice value: reading a concurrently appended slice before taking the lock
// would race. Producers and atomic resets keep their existing lock and state.
// Empty snapshots are nil, matching append([]T(nil), values...).
func Snapshot[T any](owner *sync.Mutex, values *[]T) []T {
	owner.Lock()
	defer owner.Unlock()
	return append([]T(nil), (*values)...)
}
