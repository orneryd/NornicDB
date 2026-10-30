// Package testutil provides shared database fixtures for integration tests.
package testutil

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

// DatabaseManager creates a manager borrowing db's storage. Cleanup joins the
// database workers before the manager closes that shared storage.
func DatabaseManager(t *testing.T, db *nornicdb.DB) *multidb.DatabaseManager {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(db.LifetimeContext(), db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	return testfixture.Own(t, manager, db)
}
