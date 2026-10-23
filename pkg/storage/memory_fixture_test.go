package storage

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
)

// Test fixtures own their Badger workers even when a wrapper's Close is a
// no-op or an injected error. Register cleanup at construction so a failed
// assertion cannot leave those workers and their arenas in later tests.
func newTestMemoryEngine(t testing.TB) *MemoryEngine {
	t.Helper()
	return testfixture.Own(t, testfixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
}
