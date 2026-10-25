package observability

import (
	"slices"
	"sync"
	"sync/atomic"
)

type canonicalNameSnapshot struct {
	keys  []string
	names *sensitiveNames
}

var canonicalNames = func() *atomic.Pointer[canonicalNameSnapshot] {
	cache := new(atomic.Pointer[canonicalNameSnapshot])
	cache.Store(&canonicalNameSnapshot{keys: slices.Clone(DefaultRedactKeys), names: builtInSensitiveNames})
	return cache
}()

var canonicalNamesMu sync.Mutex

// Display callers share the same current canonical list that new log/span
// processors snapshot. Compare contents so replacement and in-place startup
// edits work too; never retain the caller's mutable slice in the cache.
// Built-in matches bypass this cache in IsSensitiveFieldName. Stable lists
// require no locks, allocations or matcher compilation on the display path.
func currentCanonicalSensitiveNames() *sensitiveNames {
	snapshot := canonicalNames.Load()
	if slices.Equal(snapshot.keys, DefaultRedactKeys) {
		return snapshot.names
	}
	canonicalNamesMu.Lock()
	defer canonicalNamesMu.Unlock()
	snapshot = canonicalNames.Load()
	if slices.Equal(snapshot.keys, DefaultRedactKeys) {
		return snapshot.names
	}
	snapshot = &canonicalNameSnapshot{
		keys:  slices.Clone(DefaultRedactKeys),
		names: newSensitiveNames(defaultRedactSet()),
	}
	canonicalNames.Store(snapshot)
	return snapshot.names
}
