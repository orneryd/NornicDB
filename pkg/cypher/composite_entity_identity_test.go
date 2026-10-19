package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeConstituentEntityIdentity(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, db := range []string{"one", "two"} {
		require.NoError(t, mgr.CreateDatabase(db))
		s, err := mgr.GetStorage(db)
		require.NoError(t, err)
		_, err = s.CreateNode(&storage.Node{ID: "same", Labels: []string{"P"}, Properties: map[string]interface{}{"db": db}})
		require.NoError(t, err)
	}
	require.NoError(t, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
	}))
	s, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(s)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	prefix := "USE comp CALL { USE comp.one MATCH (n:P) RETURN n } CALL { USE comp.two MATCH (m:P) RETURN m } "
	for _, tt := range []struct{ name, query string; want interface{} }{
		{"element ids", "RETURN elementId(n) = elementId(m)", false},
		{"equality", "RETURN n = m", false},
		{"distinct", "UNWIND [n,m,n] AS x RETURN count(DISTINCT x)", int64(2)},
		{"nested distinct", "UNWIND [[n],[m],[n]] AS x RETURN count(DISTINCT x)", int64(2)},
		{"same entity", "RETURN n = n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), prefix+tt.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tt.want}}, result.Rows)
		})
	}
}
