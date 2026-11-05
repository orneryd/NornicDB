package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestFailedOpenPreservesServingTransactionMemoryLimits(t *testing.T) {
	t.Cleanup(func() { cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{}) })
	config := DefaultConfig()
	config.Memory.TransactionMax = 1 << 20
	config.Memory.TransactionTotalMax = 8 << 20
	db, err := Open("", config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	query := "UNWIND range(1, 200000) AS x RETURN size(collect(x)) AS n"
	check := func() {
		_, err := db.ExecuteCypher(context.Background(), query, nil)
		require.Error(t, err, "the serving DB must retain its configured limit")
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.General.TransactionOutOfMemoryError", code)
	}
	check()
	bad := DefaultConfig()
	bad.Database.EncryptionEnabled = true
	bad.Database.EncryptionPassword = ""
	other, err := Open(t.TempDir(), bad)
	require.Error(t, err)
	require.Nil(t, other)
	check()
	result, err := db.ExecuteCypher(context.Background(), "RETURN 1 AS ok", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
}
