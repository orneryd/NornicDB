package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Temporary repro for two composite-database bugs (to be filed).
func TestZZIssueCompositeProbe(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()
	require.NoError(t, mgr.CreateDatabase("one"))
	require.NoError(t, mgr.CreateDatabase("two"))
	require.NoError(t, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
	}))
	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	run := func(query string) {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			t.Logf("ISSUE|%s|ERROR %v", query, err)
			return
		}
		t.Logf("ISSUE|%s|columns=%v rows=%v", query, res.Columns, res.Rows)
	}
	for _, db := range []string{"one", "two"} {
		s, err := mgr.GetStorage(db)
		require.NoError(t, err)
		_, err = s.CreateNode(&storage.Node{ID: "same", Labels: []string{"P"}, Properties: map[string]interface{}{"db": db}})
		require.NoError(t, err)
	}
	// 1. elementId of nodes from two constituents with the same stored id.
	run("USE comp CALL { USE comp.one MATCH (n:P) RETURN n } CALL { USE comp.two MATCH (m:P) RETURN m } RETURN n.db AS a, elementId(n) AS en, m.db AS b, elementId(m) AS em, elementId(n) = elementId(m) AS sameElementId")
	run("USE comp CALL { USE comp.one MATCH (n:P) RETURN n UNION ALL USE comp.two MATCH (n:P) RETURN n } RETURN n.db AS db, elementId(n) AS e ORDER BY db")
	// 2. aggregation after a composite CALL.
	run("USE comp CALL { USE comp.one UNWIND [1, 2] AS k RETURN k } RETURN collect(k) AS ks")
	run("USE comp CALL { USE comp.one UNWIND [1, 2] AS k RETURN k } RETURN count(*) AS c")
	run("USE comp CALL { USE comp.one UNWIND [2, 1] AS k RETURN k } WITH k ORDER BY k RETURN collect(k) AS ks")
	run("USE comp CALL { USE comp.one UNWIND [1, 2] AS k RETURN k } RETURN k ORDER BY k")
}
