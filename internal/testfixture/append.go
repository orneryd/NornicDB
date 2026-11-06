package testfixture

import "sync"

// Append adds one recorded value under the existing owner lock of a fixture's
// slice. Pass the address of the slice, not an already-read slice value: a
// producer writing a slice value it read before taking the lock would race
// with a concurrent reader and could drop a value. The fixture keeps its single
// writer, and every reader takes that same lock through Snapshot.
//
// Appending never copies and never sorts: order is the order in which the lock
// was taken. A fixture that drains its values atomically keeps its own
// lock-held take, because Snapshot followed by a reset would lose values
// recorded between the two steps.
func Append[T any](owner *sync.Mutex, values *[]T, value T) {
	owner.Lock()
	defer owner.Unlock()
	*values = append(*values, value)
}
