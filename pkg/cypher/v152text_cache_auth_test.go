package cypher

// v1.5.2 verification, built from the text of Plane item N15-152 (NOR-388): a
// warmed query-result cache can be returned before the caller's authorization
// is checked (executor_entry.go). The repository covers the composite (Fabric)
// read; this is the plain single-database read on the same public Execute path.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestV152TextWarmedCacheDoesNotBypassAuthorization(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 100})
		_, err := exec.Execute(context.Background(), "CREATE (:V152Secret {v:'private-value'})", nil)
		require.NoError(t, err)
		const query = "MATCH (s:V152Secret) RETURN s.v AS v"
		allowed := WithPermissionChecker(context.Background(), func(string) bool { return true })
		denied := WithPermissionChecker(context.Background(), func(string) bool { return false })

		// Control: the denied caller on a cold executor.
		cold := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 100})
		_, coldErr := cold.Execute(denied, query, nil)
		require.Error(t, coldErr, "control: a denied caller must be refused on an uncached execution")

		warm, err := exec.Execute(allowed, query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"private-value"}}, warm.Rows)
		result, err := exec.Execute(denied, query, nil)
		var rows any
		if result != nil {
			rows = result.Rows
		}
		t.Logf("V152TEXT NOR-388 cold denied err=%v; warmed denied rows=%v err=%v", coldErr, rows, err)
		require.Error(t, err, "a denied caller received the warmed cached result")
	})
}
