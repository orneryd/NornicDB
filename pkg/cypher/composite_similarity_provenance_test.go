package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The rows procedure exists only on develop. No feature implementation or
// fixture from this adapter is part of the release copy.
func TestSimilarityRowsKeepCompositeProvenance(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, database := range []string{"one", "two"} {
		require.NoError(t, mgr.CreateDatabase(database))
		store, err := mgr.GetStorage(database)
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: "same", Labels: []string{"P"}})
		require.NoError(t, err)
	}
	require.NoError(t, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
	}))
	store, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	result, err := exec.Execute(context.Background(), "USE comp CALL { USE comp.one MATCH (n:P) RETURN n } CALL { USE comp.two MATCH (m:P) RETURN m } WITH n,m CALL nornicdb.similarity.stream([{source:n,thing:'shared'},{source:m,thing:'shared'}],{sourceNode:n}) YIELD node1,node2 RETURN node1,node2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"4:one:same", "4:two:same"}}, result.Rows)
}
