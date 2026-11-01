package cypher_test

// Author of fixture cleanup changes: L4 implementation lane (Claude and Codex), 1 November 2026.

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// External tests own their stores just as tests in the cypher package do.
func newTestMemoryEngine(t testing.TB) *storage.MemoryEngine {
	t.Helper()
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	runtimeFixtureOwner.Own(t, engine)
	return engine
}
