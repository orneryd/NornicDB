package bolt

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A durable value has the same concrete numeric type whether a commit has
// left its input in the cache or a reopened engine decodes the stored body.
// Keep the storage, Execute and actual Bolt wire observations in one repro.
func TestRule73NumericReadTypesConverge(t *testing.T) {
	dir := t.TempDir()
	const query = "MATCH (n:Rule73Numeric {key:'n'}) RETURN n.native, n.boundary, n.negative, n.wide"
	want := []any{int64(3), int64(128), int64(-129), int64(3)}
	for _, phase := range []string{"warm", "cold"} {
		t.Run(phase, func(t *testing.T) {
			base, err := storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "nornic")
			if phase == "warm" {
				_, err = store.CreateNode(&storage.Node{
					ID: "n", Labels: []string{"Rule73Numeric"},
					Properties: map[string]any{"key": "n", "native": int(3), "boundary": int(128), "negative": int(-129), "wide": int64(3)},
				})
				require.NoError(t, err)
			}
			checkNode := func(t *testing.T, node *storage.Node, err error) {
				t.Helper()
				require.NoError(t, err)
				got := []any{node.Properties["native"], node.Properties["boundary"], node.Properties["negative"], node.Properties["wide"]}
				t.Logf("storage types: %T %T %T %T", got[0], got[1], got[2], got[3])
				assert.Equal(t, want, got)
			}
			for name, read := range map[string]func() (*storage.Node, error){
				"point": func() (*storage.Node, error) { return base.GetNode("nornic:n") },
				"query": func() (*storage.Node, error) { return base.GetQueryNode("nornic:n") },
				"projected": func() (*storage.Node, error) {
					return base.GetNodeProjected("nornic:n", []string{"native", "boundary", "negative", "wide"})
				},
			} {
				t.Run(name, func(t *testing.T) {
					node, err := read()
					checkNode(t, node, err)
				})
			}
			t.Run("transaction", func(t *testing.T) {
				tx, err := base.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				node, err := tx.GetNode("nornic:n")
				checkNode(t, node, err)
			})
			t.Run("execute", func(t *testing.T) {
				exec := cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
				result, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				t.Logf("Execute types: %T %T %T %T", result.Rows[0][0], result.Rows[0][1], result.Rows[0][2], result.Rows[0][3])
				assert.Equal(t, want, result.Rows[0])
			})
			t.Run("bolt", func(t *testing.T) {
				mgr, err := multidb.NewDatabaseManager(base, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, mgr.Close()) })
				server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, mgr)
				port := startBoltTestServer(t, server)
				ctx := context.Background()
				driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
				session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic", AccessMode: neo4jdriver.AccessModeRead})
				defer session.Close(ctx)
				result, err := session.Run(ctx, query, nil)
				require.NoError(t, err)
				record, err := result.Single(ctx)
				require.NoError(t, err)
				assert.Equal(t, want, record.Values)
			})
		})
	}
}
