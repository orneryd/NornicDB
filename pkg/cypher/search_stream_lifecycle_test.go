package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamLifecycleProcedures(t *testing.T) {
	for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
		for _, mode := range []string{"id", "ranked", "ranked_then_id"} {
			t.Run(procedure+"/"+mode, func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
				for _, id := range []storage.NodeID{"a", "b", "c"} {
					_, err := store.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"content": "library transcript"}})
					require.NoError(t, err)
				}
				svc := search.NewService(store, search.DefaultVectorDimensions, nil)
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				require.NoError(t, svc.BuildIndexes(context.Background()))
				registry, err := resultstream.NewRegistry(resultstream.Config{MaxStreams: 1, MaxStreamsPerOwner: 1})
				require.NoError(t, err)
				t.Cleanup(registry.Close)
				svc.SetContinuationRegistry(registry)
				exec := NewStorageExecutor(store)
				exec.SetSearchService(svc)
				for i := 0; i < 4; i++ {
					request := map[string]any{"mode": mode, "query": "transcript", "limit": int64(3), "n": int64(1), "repetition": int64(i)}
					count := 0
					for {
						result, err := exec.Execute(context.Background(), "CALL "+procedure+"($request)", map[string]any{"request": request})
						require.NoError(t, err, "sequential continuation %d", i)
						page := result.Rows[0][0].(map[string]any)
						count += len(page["results"].([]any))
						if !page["has_more"].(bool) {
							break
						}
						request = map[string]any{"qid": page["qid"], "n": int64(1)}
					}
					require.Equal(t, 3, count)
				}
			})
		}
	}
}

func TestSearchStreamLifecycleCachePolicy(t *testing.T) {
	for _, query := range []string{
		"CALL db.rretrieve($request)",
		"CALL db.rretrieve({query: $query, limit: 5})",
		"WITH $request AS request CALL db.rretrieve(request)",
		"CALL db.rretrieve({query: 'x', n: 1})",
		"CALL `db`.`rretrieve`({`qid`: $qid, n: 1})",
		"CALL { CALL db.rretrieve({query: 'x', 'mode': 'ranked', n: 1}) } RETURN 1",
		"CALL db.rretrieve({qid: $qid, discard: true})",
		"CALL db.rretrieve({query: 'x', group_by: $group})",
		"CALL db.rretrieve({query: 'x', rankedLimit: 2})",
	} {
		require.False(t, isCacheableReadQuery(query), query)
	}
	for _, query := range []string{
		"CALL db.rretrieve('query')",
		"CALL db.rretrieve({query: 'n: qid', filters: {n: 1}})",
		"CALL `db`.`rretrieve`({query: 'x', limit: 5})",
	} {
		require.True(t, isCacheableReadQuery(query), query)
	}
	// Eligibility now classifies actual executable inputs. A missing binding
	// is unknown; the ordinary bound form must still hit the result cache.
	exec := solR2CacheExecutor(t)
	const query = "CALL db.rretrieve({query: $query, limit: 5})"
	params := map[string]any{"query": "transcript"}
	first, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err)
	second := requireOwnedResultCacheHit(t, exec, first, func() (*ExecuteResult, error) {
		return exec.Execute(context.Background(), query, params)
	})
	require.Len(t, second.Rows, 2)
}
