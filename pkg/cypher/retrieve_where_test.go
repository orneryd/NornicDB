package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// db.retrieve's where is a Cypher predicate on each candidate, bound as
// node, with the statement's parameters, checked before the limit: a negation
// that admits a missing property, and a pattern from the candidate through the
// graph.
func TestDbRetrieveWhere(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, `
CREATE (open:Collection {id: 'open', archived: false}), (old:Collection {id: 'old', archived: true}),
       (d1:Document)-[:MEMBER_OF]->(open), (d2:Document)-[:MEMBER_OF]->(old),
       (d1)-[:HAS_VERSION]->(v1:Version {current_source_id: 's1'}), (d2)-[:HAS_VERSION]->(v2:Version {current_source_id: 's2'}),
       (v1)-[:HAS_SOURCE]->(:SearchSource {id: 's1', content: 'late fee clause', current: true}),
       (v1)-[:HAS_SOURCE]->(:SearchSource {id: 's1old', content: 'late fee clause', current: false}),
       (v2)-[:HAS_SOURCE]->(:SearchSource {id: 's2', content: 'late fee clause'})`, nil)
	require.NoError(t, err)
	service := search.NewService(store, search.DefaultVectorDimensions, nil)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)

	retrieve := func(request map[string]interface{}, params map[string]interface{}) []string {
		t.Helper()
		request["query"] = "late fee clause"
		if params == nil {
			params = map[string]interface{}{}
		}
		params["request"] = request
		result, err := exec.Execute(ctx, "CALL db.retrieve($request) YIELD node RETURN node.id AS id ORDER BY id", params)
		require.NoError(t, err)
		ids := []string{}
		for _, row := range result.Rows {
			ids = append(ids, row[0].(string))
		}
		return ids
	}
	require.Equal(t, []string{"s1", "s1old", "s2"}, retrieve(map[string]interface{}{"limit": int64(10)}, nil))
	// A negation: current <> false is null, not false, for a source without the
	// property, so it needs IS NULL to be admitted.
	require.Equal(t, []string{"s1", "s2"}, retrieve(map[string]interface{}{"limit": int64(10), "where": "node.current IS NULL OR node.current <> false"}, nil))
	require.Equal(t, []string{"s2"}, retrieve(map[string]interface{}{"where": "node.current IS NULL"}, nil))
	// A pattern: a source of a non-archived collection in $ids that is its
	// version's current source.
	inCollections := `EXISTS { (node)<-[:HAS_SOURCE]-(v:Version)<-[:HAS_VERSION]-(:Document)-[:MEMBER_OF]->(c:Collection)
		WHERE c.id IN $ids AND NOT c.archived AND v.current_source_id = node.id }`
	require.Equal(t, []string{"s1"}, retrieve(map[string]interface{}{"where": inCollections}, map[string]interface{}{"ids": []interface{}{"open", "old"}}))
	require.Equal(t, []string{}, retrieve(map[string]interface{}{"where": inCollections}, map[string]interface{}{"ids": []interface{}{"old"}}), "not a cached result for other parameters")
	// The limit counts admitted candidates.
	require.Len(t, retrieve(map[string]interface{}{"limit": int64(1), "where": "node.id = 's2'"}, nil), 1)
	// With property filters, both apply.
	require.Equal(t, []string{"s1"}, retrieve(map[string]interface{}{"filters": map[string]interface{}{"current": true}, "where": "node.id STARTS WITH 's1'"}, nil))

	_, err = exec.Execute(ctx, "CALL db.retrieve({query: 'late fee clause', where: 1})", nil)
	require.Error(t, err)
}

// With a supplied embedding, where filters the vector candidates before the
// limit, so limit results all pass: the nearest
// nodes fail the predicate, and the result is the nearest one that passes.
func TestDbRetrieveWhereFiltersVectorCandidates(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	for index, node := range []*storage.Node{
		{ID: "near1", Labels: []string{"S"}, Properties: map[string]interface{}{"name": "near1", "state": "retired"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}},
		{ID: "near2", Labels: []string{"S"}, Properties: map[string]interface{}{"name": "near2", "state": "retired"}, ChunkEmbeddings: [][]float32{{0.99, 0.1, 0}}},
		{ID: "pass", Labels: []string{"S"}, Properties: map[string]interface{}{"name": "pass", "state": "current"}, ChunkEmbeddings: [][]float32{{0.7, 0.7, 0}}},
		{ID: "far", Labels: []string{"S"}, Properties: map[string]interface{}{"name": "far", "state": "current"}, ChunkEmbeddings: [][]float32{{0, 0, 1}}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err, index)
	}
	service := search.NewService(store, 3, nil)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)
	result, err := exec.Execute(ctx, "CALL db.retrieve($request) YIELD node RETURN node.name AS name", map[string]interface{}{"request": map[string]interface{}{
		"query": "zzz", "embedding": []interface{}{1.0, 0.0, 0.0}, "limit": int64(1), "where": "node.state = 'current'",
	}})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"pass"}}, result.Rows)
}

// A paged retrieval in id mode checks where on every page, the later ones
// after the statement that started it has ended, reading whole nodes.
func TestDbRetrieveWherePagesCheckIt(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	for _, id := range []string{"a1", "a2", "a3", "b1", "b2"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "late fee clause", "group": id[:1]}})
		require.NoError(t, err)
	}
	service := search.NewService(store, search.DefaultVectorDimensions, nil)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)

	first, err := exec.Execute(ctx, "CALL db.retrieve({query: 'late fee clause', mode: 'id', n: 1, where: \"node.group = $g\"})", map[string]interface{}{"g": "a"})
	require.NoError(t, err)
	page := first.Rows[0][0].(map[string]interface{})
	seen := len(page["results"].([]interface{}))
	for page["has_more"] == true {
		next, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]interface{}{"request": map[string]interface{}{"qid": page["qid"], "n": int64(1)}})
		require.NoError(t, err)
		page = next.Rows[0][0].(map[string]interface{})
		seen += len(page["results"].([]interface{}))
	}
	require.Equal(t, 3, seen, "only group a, across pages")
}
