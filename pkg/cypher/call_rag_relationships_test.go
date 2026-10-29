package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// db.retrieveRelationships returns matching relationships with their start
// and end nodes; it can't be continued, and fails on a database without
// relationship search; db.retrieve still searches nodes (#1065).
func TestCallDbRetrieveRelationships(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	exec.SetSearchService(svc)

	_, err := exec.Execute(ctx, "CREATE (:Doc {name: 'a'})-[:CITES {reason: 'zephyrine bridge'}]->(:Doc {name: 'b'})", nil)
	require.NoError(t, err)
	var result *ExecuteResult
	require.Eventually(t, func() bool {
		result, err = exec.Execute(ctx, "CALL db.retrieveRelationships({query: 'zephyrine'})", nil)
		return err == nil && len(result.Rows) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, []string{"relationship", "start", "end", "score", "rrf_score", "vector_rank", "bm25_rank", "search_method", "fallback_triggered", "fallback_reason"}, result.Columns)
	row := result.Rows[0]
	relationship := row[0].(*storage.Edge)
	require.Equal(t, "CITES", relationship.Type)
	require.Equal(t, "zephyrine bridge", relationship.Properties["reason"])
	require.Equal(t, "a", row[1].(*storage.Node).Properties["name"])
	require.Equal(t, "b", row[2].(*storage.Node).Properties["name"])
	require.Equal(t, "relationship_bm25", row[7])
	yielded, err := exec.Execute(ctx, "CALL db.retrieveRelationships({query: 'zephyrine'}) YIELD relationship, start, end, score RETURN type(relationship) AS type, start.name AS start, end.name AS end", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"CITES", "a", "b"}}, yielded.Rows)

	// An endpoint no longer visible is null.
	require.NoError(t, store.DeleteNode(relationship.EndNode))
	require.Nil(t, exec.relationshipSearchResult(&search.SearchResponse{Results: []search.SearchResult{{
		Relationship: relationship,
	}}}).Rows[0][2])

	_, err = exec.Execute(ctx, "CALL db.retrieveRelationships({query: 'zephyrine', n: 5})", nil)
	require.ErrorContains(t, err, "can't be continued")

	plain := NewStorageExecutor(store)
	plainService := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = plainService.Close() })
	plain.SetSearchService(plainService)
	_, err = plain.Execute(ctx, "CALL db.retrieveRelationships({query: 'zephyrine'})", nil)
	require.ErrorContains(t, err, "NORNICDB_SEARCH_RELATIONSHIPS_ENABLED")
}

// db.retrieve ignores a relationships key: it searches nodes.
func TestCallDbRetrieveIgnoresRelationshipsKey(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	exec.SetSearchService(svc)
	_, err := exec.Execute(ctx, "CREATE (:Doc {name: 'zephyrine'})", nil)
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(ctx))
	result, err := exec.Execute(ctx, "CALL db.retrieve({query: 'zephyrine', relationships: true})", nil)
	require.NoError(t, err)
	require.Equal(t, "node", result.Columns[0])
}

// db.retrieveRelationships is listed with a localized description (#1065).
func TestRetrieveRelationshipsMetadata(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	procedure, ok := globalProcedureRegistry.Get("db.retrieveRelationships")
	require.True(t, ok)
	spec := procedure.Spec
	require.NotEmpty(t, spec.DescriptionMessage.ID)
	require.Equal(t, spec.Description, spec.DescriptionMessage.Fallback)
	require.Equal(t, ProcedureModeRead, spec.Mode)
	require.Contains(t, spec.Signature, "relationship :: RELATIONSHIP, start :: NODE, end :: NODE")
}

// db.retrieveRelationships takes db.retrieve's request: a map (a parameter
// too, left unchanged) or a string as the query; null is refused (#1065).
func TestCallDbRetrieveRelationshipsRequestForms(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	exec.SetSearchService(svc)
	_, err := exec.Execute(ctx, "CREATE (:Doc)-[:CITES {reason: 'zephyrine'}]->(:Doc)", nil)
	require.NoError(t, err)
	request := map[string]interface{}{"query": "zephyrine"}
	for _, tc := range []struct {
		query  string
		params map[string]interface{}
	}{
		{"CALL db.retrieveRelationships($request)", map[string]interface{}{"request": request}},
		{"CALL db.retrieveRelationships('zephyrine')", nil},
	} {
		require.Eventually(t, func() bool {
			result, err := exec.Execute(ctx, tc.query, tc.params)
			return err == nil && len(result.Rows) == 1
		}, 5*time.Second, 20*time.Millisecond, tc.query)
	}
	require.Equal(t, map[string]interface{}{"query": "zephyrine"}, request, "the parameter map is not marked")
	_, err = exec.Execute(ctx, "CALL db.retrieveRelationships(null)", nil)
	require.Error(t, err)
}
