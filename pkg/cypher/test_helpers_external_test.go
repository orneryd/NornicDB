package cypher_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// External tests own their stores just as tests in the cypher package do.
func newTestMemoryEngine(t testing.TB) *storage.MemoryEngine {
	t.Helper()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("close test store: %v", err)
		}
	})
	return engine
}
