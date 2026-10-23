package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR73EmptyUnwindWrite(t *testing.T) {
	ctx := context.Background()
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_r73_empty"))
			_, err := e.Execute(ctx, "CREATE (:ReviewItem {k:2})", nil)
			require.NoError(t, err)
			if explicit {
				_, err = e.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer e.Execute(ctx, "ROLLBACK", nil)
			}
			r, err := e.Execute(ctx, "MATCH (n:ReviewItem) UNWIND [] AS x CREATE (:ReviewUnexpected) RETURN count(*)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, r.Rows)
			require.Zero(t, r.Stats.NodesCreated)
		})
	}
}

func TestReviewR73SeedContracts(t *testing.T) {
	ctx := context.Background()
	t.Run("element_id_batch", func(t *testing.T) {
		s := storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_r73_ids")
		e := NewStorageExecutor(s)
		left, err := s.CreateNode(&storage.Node{ID: "left", Labels: []string{"ReviewEntity"}})
		require.NoError(t, err)
		right, err := s.CreateNode(&storage.Node{ID: "right", Labels: []string{"ReviewEntity"}})
		require.NoError(t, err)
		rows := []interface{}{map[string]interface{}{"src": string(left), "dst": string(right)}, map[string]interface{}{"src": string(right), "dst": string(left)}}
		r, err := e.Execute(ctx, "UNWIND $rows AS row MATCH (a) WHERE elementId(a)=row.src MATCH (b) WHERE elementId(b)=row.dst CREATE (a)-[:REVIEW_LINK]->(b) RETURN count(a)", map[string]interface{}{"rows": rows})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, r.Rows)
		require.True(t, e.LastHotPathTrace().UnwindMultiMatchCreateBatch)
	})
}

func TestReviewR73FabricContinuation(t *testing.T) {
	ctx := context.Background()
	mgr, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	defer mgr.Close()
	require.NoError(t, mgr.CreateDatabase("review_shard"))
	require.NoError(t, mgr.CreateCompositeDatabase("review_cmp", []multidb.ConstituentRef{{Alias: "source", DatabaseName: "review_shard", Type: "local", AccessMode: "read_write"}}))
	s, err := mgr.GetStorage("review_shard")
	require.NoError(t, err)
	_, err = s.CreateNode(&storage.Node{ID: "r73", Labels: []string{"ReviewItem"}, Properties: map[string]interface{}{"k": int64(2)}})
	require.NoError(t, err)
	ds, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	e := NewStorageExecutor(ds)
	e.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	r, err := e.Execute(ctx, "USE review_cmp CALL { USE review_cmp.source MATCH (n:ReviewItem) UNWIND [n.k] AS x RETURN x } RETURN x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, r.Rows)
}
