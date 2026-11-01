// Package testutil provides shared database fixtures for integration tests.
package testutil

// Author of fixture cleanup changes: L4 implementation lane (Claude and Codex), 1 November 2026.

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

// DatabaseManager creates a manager borrowing db's storage. Cleanup joins the
// database workers through their existing owner before the manager closes that
// shared storage. The caller supplies the closer captured at DB adoption.
func DatabaseManager(t *testing.T, db *nornicdb.DB, closeDB func()) *multidb.DatabaseManager {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(db.LifetimeContext(), db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	return testfixture.Own(t, manager, testfixture.CloseFunc(func() error { closeDB(); return nil }))
}
