package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeConstituentEntityIdentity(t *testing.T) {
	for _, mode := range []string{"autocommit", "explicit"} {
		t.Run(mode, func(t *testing.T) { testCompositeConstituentEntityIdentity(t, mode == "explicit") })
	}
}

func testCompositeConstituentEntityIdentity(t *testing.T, explicit bool) {
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, db := range []string{"one", "two"} {
		require.NoError(t, mgr.CreateDatabase(db))
		s, err := mgr.GetStorage(db)
		require.NoError(t, err)
		_, err = s.CreateNode(&storage.Node{ID: "same", Labels: []string{"P"}, Properties: map[string]interface{}{"db": db}})
		require.NoError(t, err)
		_, err = s.CreateNode(&storage.Node{ID: "target", Labels: []string{"Q"}})
		require.NoError(t, err)
		require.NoError(t, s.CreateEdge(&storage.Edge{ID: "same-rel", StartNode: "same", EndNode: "target", Type: "R"}))
	}
	require.NoError(t, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
	}))
	database := mgr.DefaultDatabaseName()
	if explicit {
		database = "comp"
	}
	s, err := mgr.GetStorage(database)
	require.NoError(t, err)
	exec := NewStorageExecutor(s)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	if explicit {
		_, err := exec.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
	}
	prefix := "USE comp CALL { USE comp.one MATCH p=(n:P)-[r:R]->(z:Q) RETURN n,r,p } CALL { USE comp.two MATCH q=(m:P)-[s:R]->(y:Q) RETURN m,s,q } "
	for _, tt := range []struct {
		name, query string
		want        interface{}
	}{
		{"element ids", "RETURN elementId(n) = elementId(m)", false},
		{"equality", "RETURN n = m", false},
		{"distinct", "UNWIND [n,m,n] AS x RETURN count(DISTINCT x)", int64(2)},
		{"nested distinct", "UNWIND [[n],[m],[n]] AS x RETURN count(DISTINCT x)", int64(2)},
		{"same entity", "RETURN n = n", true},
		{"relationship element ids", "RETURN elementId(r) = elementId(s)", false},
		{"relationship equality", "RETURN r = s", false},
		{"relationship distinct", "UNWIND [r,s,r] AS x RETURN count(DISTINCT x)", int64(2)},
		{"path equality", "RETURN p = q", false},
		{"path distinct", "UNWIND [p,q,p] AS x RETURN count(DISTINCT x)", int64(2)},
		{"map distinct", "UNWIND [{n:n},{n:m},{n:n}] AS x RETURN count(DISTINCT x)", int64(2)},
		{"membership", "RETURN elementId(n) IN [elementId(m)]", false},
		{"comparison chain", "RETURN elementId(n) = elementId(n) = elementId(m)", false},
		{"raw id extension", "RETURN elementId(n) = 'same'", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), prefix+tt.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tt.want}}, result.Rows)
		})
	}
	t.Run("procedure-created values", func(t *testing.T) {
		result, err := exec.Execute(context.Background(), "USE comp CALL { USE comp.one CALL db.schema.visualization() YIELD nodes RETURN nodes[0] AS n } CALL { USE comp.two CALL db.schema.visualization() YIELD nodes RETURN nodes[0] AS m } RETURN n = m", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{false}}, result.Rows)
	})
	if !explicit {
		t.Run("projected algorithm keys", func(t *testing.T) {
			_, err := exec.Execute(context.Background(), "USE comp CALL gds.graph.project('composite_identity', '*', '*')", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "USE comp CALL gds.graph.drop('composite_identity')", nil) })
			result, err := exec.Execute(context.Background(), "USE comp CALL gds.fastRP.stream('composite_identity', {embeddingDimension: 4}) YIELD nodeId RETURN count(DISTINCT nodeId)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
			result, err = exec.Execute(context.Background(), "USE comp CALL gds.fastRP.stream('composite_identity', {embeddingDimension: 4}) YIELD nodeId RETURN count(DISTINCT elementId(gds.util.asNode(nodeId)))", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
		})
	}
}
