package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fabricLogicalTypeExecutor(t *testing.T) *StorageExecutor {
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase("one"))
	require.NoError(t, manager.CreateCompositeDatabase("comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"}}))
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	return exec
}

func TestFabricLogicalStaticAlternatives(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, explicit := range []bool{false, true} {
			for _, composite := range []bool{false, true} {
				for _, tc := range []struct{ name, query string }{
					{"asymmetric_import_index", "WITH 1.0 AS x CALL (x) { MATCH (n:ReviewEmpty) WITH n.v * 1.0 AS x WITH count(*) AS c RETURN [1,2][x] AS v } RETURN v"},
					{"asymmetric_import_slice", "WITH 1.0 AS x CALL (x) { MATCH (n:ReviewEmpty) WITH n.v * 1.0 AS x WITH count(*) AS c RETURN [1,2][..x] AS v } RETURN v"},
					{"asymmetric_import_filtered", "UNWIND [] AS unused WITH 1.0 AS x CALL (x) { MATCH (n:ReviewEmpty) WITH n.v * 1.0 AS x WITH count(*) AS c RETURN [1,2][x] AS v } RETURN v"},
					{"numeric_case_empty", "UNWIND [] AS unused RETURN (CASE WHEN true THEN 1 ELSE 2.0 END) + {a:1} AS v"},
					{"numeric_case_nonempty", "RETURN (CASE WHEN true THEN 1 ELSE 2.0 END) + {a:1} AS v"},
					{"numeric_case_predicate", "UNWIND [] AS unused RETURN CASE WHEN (CASE WHEN true THEN 1 ELSE 2.0 END) THEN 1 ELSE 0 END AS v"},
				} {
					t.Run(fmt.Sprintf("normalization_%v/explicit_%v/composite_%v/%s", normalize, explicit, composite, tc.name), func(t *testing.T) {
						exec := fabricLogicalTypeExecutor(t)
						if composite {
							exec = fabricCallRowsCompositeExecutor(t, exec)
						}
						ctx := context.Background()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							if composite {
								_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
								require.True(t, ok)
							}
							defer exec.Execute(ctx, "ROLLBACK", nil)
						}
						query := tc.query
						if composite {
							query = "USE comp.one " + query
						}
						result, err := exec.Execute(ctx, query, nil)
						if assert.Error(t, err, "query=%s result=%+v", query, result) {
							code, _ := nornicerrors.Neo4jStatus(err)
							assert.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "expected compile-time error, got %v", err)
						}
					})
				}
			}
		}
	}
}
