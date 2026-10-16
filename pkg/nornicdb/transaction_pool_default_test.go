package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// With only the Go memory limit set, the global transaction pool is
// unlimited (#1343): a statement whose estimate is above 70% of the limit,
// Neo4j's default, is not rejected. Set explicitly, the pool applies.
func TestTransactionPoolUnlimitedWithOnlyMemoryLimit(t *testing.T) {
	t.Cleanup(func() { cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{}) })
	// ~64 MiB of collected values in the estimate: above 70% (~45 MiB) of
	// a 64 MiB memory limit.
	const statement = "UNWIND range(1, 4000000) AS x RETURN size(collect(x)) AS n"
	open := func(pool int64) *DB {
		config := DefaultConfig()
		config.Memory.DecayEnabled = false
		config.Memory.AutoLinksEnabled = false
		config.Memory.RuntimeLimit = 64 << 20
		config.Memory.RuntimeLimitStr = "64"
		config.Memory.TransactionTotalMax = pool
		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	db := open(-1)
	require.Zero(t, db.config.Memory.EffectiveTransactionTotalMax())
	result, err := db.ExecuteCypher(context.Background(), statement, nil)
	require.NoError(t, err, "only the memory limit is set: the pool is unlimited")
	require.Equal(t, int64(4000000), result.Rows[0][0])
	require.NoError(t, db.Close())

	db = open(45 << 20)
	_, err = db.ExecuteCypher(context.Background(), statement, nil)
	require.Error(t, err, "an explicit pool applies")
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", code)
}
