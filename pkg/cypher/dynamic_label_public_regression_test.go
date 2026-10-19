package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/dynamiclabels"
	"github.com/stretchr/testify/require"
)

func TestDynamicLabelPublicExecute(t *testing.T) {
	for _, normalization := range []string{"true", "false"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(normalization == "true")
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			for _, c := range dynamiclabels.Cases() {
				t.Run(c.Name, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_public"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, dynamiclabels.Fixture, nil)
					require.NoError(t, err)
					for range 2 {
						result, err := exec.Execute(ctx, c.Query, c.Params)
						if c.Code != "" { require.Error(t, err); requireStatusCode(t, err, c.Code); continue }
						require.NoError(t, err, c.Query)
						require.Equal(t, c.Rows, result.Rows, c.Query)
					}
				})
			}
		})
	}
}

func TestDynamicLabelPublicDatabaseRouting(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase("dynamic_target"))
	require.NoError(t, manager.CreateCompositeDatabase("dynamic_composite", []multidb.ConstituentRef{
		{Alias: "data", DatabaseName: "dynamic_target", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := context.Background()
	_, err = exec.Execute(ctx, "USE dynamic_target "+dynamiclabels.Fixture, nil)
	require.NoError(t, err)
	for _, query := range []string{
		"USE dynamic_target WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id ORDER BY id",
		"PROFILE USE dynamic_target WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id ORDER BY id",
		"USE dynamic_composite CALL { USE dynamic_composite.data WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id } RETURN id ORDER BY id",
		"PROFILE USE dynamic_composite CALL { USE dynamic_composite.data WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id } RETURN id ORDER BY id",
	} {
		t.Run(query, func(t *testing.T) {
			for _, c := range []struct { labels []any; rows [][]any }{
				{[]any{"A"}, [][]any{{int64(1)}, {int64(2)}}},
				{[]any{"B"}, [][]any{{int64(2)}, {int64(3)}}},
				{[]any{"A"}, [][]any{{int64(1)}, {int64(2)}}},
			} {
				result, err := exec.Execute(ctx, query, map[string]any{"labels": c.labels})
				require.NoError(t, err)
				require.Equal(t, c.rows, result.Rows)
			}
		})
	}
}
