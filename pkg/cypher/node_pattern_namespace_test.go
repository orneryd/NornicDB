package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNodePatternNamespaceRepresentations(t *testing.T) {
	base := newTestMemoryEngine(t)
	system := storage.NewNamespacedEngine(base, "system")
	tenant := storage.NewNamespacedEngine(base, "tenant")
	for _, test := range []struct {
		name string
		view storage.Engine
		hide bool
	}{
		{"unscoped", base, true},
		{"nil", nil, true},
		{"system", system, false},
		{"tenant", tenant, true},
		{"wrapped-system", wrappedDatabaseEngine{Engine: tenant, namespace: "system"}, false},
		{"wrapped-tenant", wrappedDatabaseEngine{Engine: system, namespace: "tenant"}, true},
		{"transaction-system", &transactionStorageWrapper{underlying: tenant, namespace: "system"}, false},
		{"transaction-tenant", &transactionStorageWrapper{underlying: system, namespace: "tenant"}, true},
		{"system-procedure-view", systemGraphView{Engine: system}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.hide, shouldHideSystemNodes(test.view))
		})
	}
}

// Public system statements are admitted before MATCH. Keep that policy separate
// from the visibility of records to the matcher operating on a transaction view.
func TestNodePatternSystemStatementAdmission(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, query := range []string{
			"MATCH (n:NamespaceRecord) WHERE n.id=1 RETURN n.id LIMIT 1",
			"MATCH (n:NamespaceRecord) WITH n MATCH (n:NamespaceRecord) RETURN n.id",
		} {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, query), func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
				require.NoError(t, store.GetSchema().AddPropertyIndex("namespace_record_id", "NamespaceRecord", []string{"id"}))
				executor := NewStorageExecutor(store)
				ctx := context.Background()
				if explicit {
					_, err := executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
				}
				_, err := executor.Execute(ctx, query, nil)
				requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
			})
		}
	}
}

// Real implicit and explicit adapters must preserve their database's visibility
// inside MATCH. This deliberately enters below public system-statement admission;
// public MATCH on system is refused, rather than an API for reading its records.
func TestNodePatternNamespaceTransactionVisibility(t *testing.T) {
	for _, namespace := range []string{"system", "tenant"} {
		for _, explicit := range []bool{false, true} {
			for _, indexed := range []bool{false, true} {
				for _, bound := range []bool{false, true} {
					t.Run(fmt.Sprintf("namespace=%s/explicit=%t/index=%t/bound=%t", namespace, explicit, indexed, bound), func(t *testing.T) {
						store := storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace)
						for id := 1; id <= 2; id++ {
							labels := []string{"NamespaceRecord"}
							if id == 1 {
								labels = append(labels, "_internal")
							}
							_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("record-%d", id)), Labels: labels, Properties: map[string]interface{}{"id": int64(id)}})
							require.NoError(t, err)
						}
						if indexed {
							// Use the schema owner's backfill; an empty declared index
							// would discard both hidden and ordinary fixture records.
							require.NoError(t, NewStorageExecutor(store).addPropertyIndex("namespace_record_id", "NamespaceRecord", []string{"id"}))
						}
						executor := NewStorageExecutor(store)
						query := "MATCH (n:NamespaceRecord) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"
						if bound {
							query = "WITH $node AS n MATCH (n:NamespaceRecord) RETURN n.id"
						}
						node, err := store.GetNode("record-1")
						require.NoError(t, err)
						ctx := withQueryParams(context.Background(), map[string]interface{}{"node": node})
						var result *ExecuteResult
						if explicit {
							_, err = executor.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							t.Cleanup(func() { _, _ = executor.Execute(context.Background(), "ROLLBACK", nil) })
							result, err = executor.executeInTransaction(ctx, query, strings.ToUpper(query))
						} else {
							result, err = executor.executeWithImplicitTransactionCallback(ctx, query, strings.ToUpper(query), func(ctx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
								require.IsType(t, &transactionStorageWrapper{}, txExec.getStorage(ctx))
								return txExec.executeWithoutTransaction(ctx, query, strings.ToUpper(query))
							})
						}
						require.NoError(t, err)
						want := [][]interface{}{{int64(1)}}
						if namespace != "system" {
							want = [][]interface{}{{int64(2)}}
							if bound {
								want = [][]interface{}{}
							}
						}
						require.Equal(t, want, result.Rows)
					})
				}
			}
		}
	}
}
