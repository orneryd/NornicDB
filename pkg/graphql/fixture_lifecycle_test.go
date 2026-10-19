package graphql

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
)

func TestDBManagerCleanupClosesOwningDB(t *testing.T) {
	t.Run("manager cleanup", func(t *testing.T) {
		db := testDB(t)
		// Observe the manager's cleanup before testDB's later cleanup can
		// conceal that its background workers were left running.
		t.Cleanup(func() {
			_, err := db.GetNode(context.Background(), "cleanup-probe")
			assert.ErrorIs(t, err, nornicdb.ErrClosed,
				"manager cleanup must stop the owning DB before closing its shared storage")
		})
		_ = testDBManager(t, db)
	})
}
