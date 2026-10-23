package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The service borrows its storage engine. Closing the service does not stop
// Badger's background workers; the test that creates the engine owns it.
func newTestMemoryEngine(t testing.TB) *storage.MemoryEngine {
	t.Helper()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}
