package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The RAG procedures read their request from the call's evaluated argument,
// so a candidate list bound earlier in the statement is the same request as
// a parameter (#907, Personal Documents I23).
func TestRagProcedureRequestIsTheEvaluatedArgument(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	ids := func(t *testing.T, query string, params map[string]interface{}) []interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		out := make([]interface{}, len(result.Rows))
		for i, row := range result.Rows {
			out[i] = row[0]
		}
		return out
	}

	t.Run("parameter", func(t *testing.T) {
		got := ids(t, "CALL db.rerank({query: 'q', candidates: $c}) YIELD id RETURN id",
			map[string]interface{}{"c": []interface{}{map[string]interface{}{"id": "a", "content": "x"}}})
		require.Equal(t, []interface{}{"a"}, got)
	})
	t.Run("list bound by WITH", func(t *testing.T) {
		got := ids(t, "WITH [{id: 'a', content: 'x'}] AS c CALL db.rerank({query: 'q', candidates: c}) YIELD id RETURN id", nil)
		require.Equal(t, []interface{}{"a"}, got)
	})
	t.Run("list collected from rows", func(t *testing.T) {
		got := ids(t, "UNWIND ['a', 'b'] AS k WITH collect({id: k, content: 'x ' + k}) AS c CALL db.rerank({query: 'q', candidates: c}) YIELD id RETURN id ORDER BY id", nil)
		require.Equal(t, []interface{}{"a", "b"}, got)
	})
	t.Run("whole request bound by WITH", func(t *testing.T) {
		got := ids(t, "WITH {query: 'q', candidates: [{id: 'a', content: 'x'}]} AS request CALL db.rerank(request) YIELD id RETURN id", nil)
		require.Equal(t, []interface{}{"a"}, got)
	})
	t.Run("null request is the procedure's failure", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.rerank(null) YIELD id RETURN id", nil)
		require.Error(t, err)
	})
	t.Run("a STRING request is the query", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.rerank('q') YIELD id RETURN id", nil)
		require.ErrorContains(t, err, "requires non-empty candidates")
	})
}

func TestRagProcedureRequestReader(t *testing.T) {
	req, err := ragProcedureRequest("db.retrieve", []interface{}{map[string]interface{}{"query": "alpha", "limit": int64(5)}})
	require.NoError(t, err)
	require.Equal(t, "alpha", req["query"])

	req, err = ragProcedureRequest("db.retrieve", []interface{}{"alpha"})
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"query": "alpha"}, req)

	_, err = ragProcedureRequest("db.retrieve", nil)
	require.Error(t, err)

	_, err = ragProcedureRequest("db.retrieve", []interface{}{int64(123)})
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
}

// countingReranker records how many candidates reach the provider.
type countingReranker struct{ received int }

func (r *countingReranker) Name() string                          { return "counting" }
func (r *countingReranker) Enabled() bool                         { return true }
func (r *countingReranker) IsAvailable(ctx context.Context) bool  { return true }
func (r *countingReranker) Rerank(ctx context.Context, query string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	r.received = len(candidates)
	results := make([]search.RerankResult, len(candidates))
	for i, candidate := range candidates {
		results[i] = search.RerankResult{ID: candidate.ID, Content: candidate.Content, OriginalRank: i + 1, NewRank: i + 1, FinalScore: 1}
	}
	return results, nil
}

// A computed value inside the request map is its value: rerankTopK:
// size($c) - 2 sends one of three candidates to the provider (Personal
// Documents I24).
func TestRagProcedureRequestComputedOption(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(engine)
	reranker := &countingReranker{}
	service := search.NewService(engine)
	service.SetReranker(reranker)
	exec.SetSearchService(service)
	candidates := []interface{}{
		map[string]interface{}{"id": "a", "content": "x"},
		map[string]interface{}{"id": "b", "content": "y"},
		map[string]interface{}{"id": "c", "content": "z"},
	}
	for _, query := range []string{
		"CALL db.rerank({query: 'q', candidates: $c, rerankTopK: size($c) - 2}) YIELD id RETURN id",
		"WITH $c AS c CALL db.rerank({query: 'q', candidates: c, rerankTopK: size(c) - 2}) YIELD id RETURN id",
	} {
		reranker.received = 0
		result, err := exec.Execute(ctx, query, map[string]interface{}{"c": candidates})
		require.NoError(t, err, query)
		require.Equal(t, 1, reranker.received, query)
		require.Len(t, result.Rows, 1, query)
	}
}
