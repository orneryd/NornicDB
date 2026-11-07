package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTraversalLimitPreservesWrappedNamespaceAndTransaction(t *testing.T) {
	for _, parser := range []struct {
		name string
		use  func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			restore := parser.use()
			defer restore()
			for _, wrapper := range []string{"plain", "traced", "wal", "traced-wal"} {
				for _, explicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/transaction=%t", wrapper, explicit), func(t *testing.T) {
						ctx := t.Context()
						base := newTestMemoryEngine(t)
						tenant := storage.NewNamespacedEngine(base, "limit_tenant")
						other := storage.NewNamespacedEngine(base, "limit_other")
						setup := NewStorageExecutor(tenant)
						_, err := setup.Execute(ctx, `CREATE (:P {id:1})-[:R {value:0}]->(:Q), (:P {id:2})-[:R {value:1}]->(:Q)`, nil)
						require.NoError(t, err)
						_, err = NewStorageExecutor(other).Execute(ctx, `CREATE (:P {id:0})-[:R {value:1}]->(:Q)`, nil)
						require.NoError(t, err)
						_, err = setup.Execute(ctx, `CREATE INDEX p_id FOR (p:P) ON (p.id)`, nil)
						require.NoError(t, err)
						var view storage.Engine = tenant
						if wrapper == "traced" || wrapper == "traced-wal" {
							view = storage.NewTracedEngine(view)
						}
						if wrapper == "wal" || wrapper == "traced-wal" {
							wal, err := storage.NewWAL(t.TempDir(), nil)
							require.NoError(t, err)
							t.Cleanup(func() { require.NoError(t, wal.Close()) })
							view = storage.NewWALEngine(view, wal)
						}
						exec := NewStorageExecutor(view)
						if explicit {
							_, err = exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
						}
						result, err := exec.Execute(ctx, `MATCH (p:P)-[:R {value:1}]->(:Q) WHERE p.id >= 0 RETURN p.id ORDER BY p.id LIMIT 1`, nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
					})
				}
			}
		})
	}
}
