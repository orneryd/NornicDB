package txsession

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// endTransactionThroughOwner ends session's explicit transaction the way its
// end owner does, with TERMINATE TRANSACTIONS (pkg/cypher, NOR-284). A
// session decides no transaction's end: past its idle deadline it is retired
// only once its transaction has ended (endOwnerKeeps). The running
// transactions are process-wide and this package's tests run one at a time,
// so every idle explicit transaction listed is this test's.
func endTransactionThroughOwner(t *testing.T, session *Session) {
	t.Helper()
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = store.Close() })
	admin := cypher.NewStorageExecutor(store)
	ctx := context.Background()
	result, err := admin.Execute(ctx, "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery = '' RETURN transactionId", nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.Rows, "the session's transaction is not listed")
	for _, row := range result.Rows {
		_, err := admin.Execute(ctx, fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", row[0]), nil)
		require.NoError(t, err)
	}
	require.True(t, session.Executor.TransactionEnded(), "TERMINATE did not end the session's transaction")
}
