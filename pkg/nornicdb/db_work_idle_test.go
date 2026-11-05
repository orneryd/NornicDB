package nornicdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitDBWorkIdle waits until every admitted DB task has returned, without
// stopping the DB: tests use it to let background work settle mid-life.
func waitDBWorkIdle(t testing.TB, db *DB) {
	t.Helper()
	require.Eventually(t, func() bool { return db.work.Active() == 0 }, 30*time.Second, time.Millisecond)
}
