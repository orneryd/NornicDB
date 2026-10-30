package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// newTestMemoryEngine creates an in-memory storage engine that is automatically
// closed when the test completes. This prevents Badger background goroutines
// (compaction, GC) from racing with subsequent tests.
func newTestMemoryEngine(t testing.TB) *storage.MemoryEngine {
	t.Helper()
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	runtimeFixtureOwner.Own(t, engine)
	return engine
}
