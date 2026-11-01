package graphql

// Fixture cleanup migration: L4 implementation lane (Codex), 1 November 2026.

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testutil"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
)

func TestDBManagerCleanupClosesOwningDB(t *testing.T) {
	t.Run("manager cleanup", func(t *testing.T) {
		db, closeDB := fixtureowner.OwnWithClose(t, testDB(t))
		// Observe the manager cleanup after it consumes the DB owner closer.
		// The earlier registered DB owner must be a no-op afterward.
		t.Cleanup(func() {
			_, err := db.GetNode(context.Background(), "cleanup-probe")
			assert.ErrorIs(t, err, nornicdb.ErrClosed,
				"manager cleanup must stop the owning DB before closing its shared storage")
		})
		_ = testutil.DatabaseManager(t, db, closeDB)
	})
}
