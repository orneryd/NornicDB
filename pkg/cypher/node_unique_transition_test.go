package cypher

import (
	"context"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type mergeNoSchemaEngine struct{ storage.Engine }

func (mergeNoSchemaEngine) GetSchema() *storage.SchemaManager { return nil }

func TestMergeUniqueLookupStorageFailure(t *testing.T) {
	b, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	store := storage.NewNamespacedEngine(b, "test")
	require.NoError(t, store.GetSchema().AddUniqueConstraint("doc_id", "Doc", "id"))
	exec := NewStorageExecutor(store)
	labels, props := []string{"Doc"}, map[string]interface{}{"id": "claimed"}
	_, indexed, err := exec.mergeNodeIndexedCandidateIDs(mergeNoSchemaEngine{store}, labels, props)
	require.NoError(t, err)
	require.False(t, indexed)
	require.NoError(t, b.Close())
	for name, lookup := range map[string]func() error{
		"single match":     func() error { _, err := exec.findMergeNode(store, labels, props); return err },
		"candidate IDs":    func() error { _, _, err := exec.mergeNodeIndexedCandidateIDs(store, labels, props); return err },
		"multiple matches": func() error { _, _, err := exec.findMergeNodesScanned(store, labels, props); return err },
	} {
		t.Run(name, func(t *testing.T) {
			err := lookup()
			var failure *storage.CommitFailureError
			require.ErrorAs(t, err, &failure)
			require.ErrorIs(t, err, storage.ErrStorageClosed)
			require.NotErrorIs(t, err, storage.ErrConflict)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
		})
	}
}

func TestSearchProcedureExcludesOwnDeletedHits(t *testing.T) {
	b, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetDecayEnabled(false)
	store := storage.NewNamespacedEngine(b, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	for _, id := range []string{"gone", "keep"} {
		_, err := exec.Execute(ctx, "CREATE (:Doc {id:$id, title:'zephyrine'})", map[string]interface{}{"id": id})
		require.NoError(t, err)
	}
	service := search.NewServiceWithDimensions(store, 3)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)
	for _, where := range []bool{false, true} {
		t.Run(fmt.Sprintf("where=%t", where), func(t *testing.T) {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer exec.Execute(ctx, "ROLLBACK", nil)
			result, err := exec.Execute(ctx, "MATCH (n:Doc {id:'gone'}) DELETE n", nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, result.Stats.NodesDeleted)
			req := map[string]interface{}{"query": "zephyrine", "types": []interface{}{"Doc"}, "limit": 5}
			if where {
				req["where"] = "node.id = 'keep'"
			}
			result, err = exec.Execute(ctx, "CALL db.retrieve($req) YIELD node RETURN node.id", map[string]interface{}{"req": req})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"keep"}}, result.Rows)
		})
	}
}
