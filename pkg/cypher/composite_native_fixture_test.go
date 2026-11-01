package cypher

// Fixture cleanup migration: L4 implementation lane (Codex), 1 November 2026.

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// newCompositeNativeManager adopts the manager over its already-owned engine.
func newCompositeNativeManager(t *testing.T, cfg *multidb.Config) *multidb.DatabaseManager {
	t.Helper()
	mgr, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), cfg)
	require.NoError(t, err)
	return fixtureowner.Own(t, mgr)
}
