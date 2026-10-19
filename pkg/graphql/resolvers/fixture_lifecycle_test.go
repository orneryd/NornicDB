package resolvers

import (
	"context"
	"github.com/orneryd/nornicdb/internal/testutil"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
)

func TestDBManagerCleanupClosesOwningDB(t *testing.T) {
	t.Run("manager cleanup", func(t *testing.T) {
		db := testDB(t)
		// Cleanups run in reverse registration order: inspect the DB after the
		// manager's cleanup, before testDB's own cleanup can hide the mistake.
		t.Cleanup(func() {
			_, err := db.GetNode(context.Background(), "cleanup-probe")
			assert.ErrorIs(t, err, nornicdb.ErrClosed,
				"manager cleanup must stop the owning DB before closing its shared storage")
		})
		_ = testutil.DatabaseManager(t, db)
	})
}
