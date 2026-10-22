package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// EXISTS { } runs on the subquery evaluator COUNT and COLLECT use, so a body
// of several clauses (two MATCH, WITH, OPTIONAL MATCH, UNWIND) or a comma
// list of patterns has a row exactly when COUNT { } > 0. EXISTS ran such a
// body without a RETURN, so it never had a row, and a lone pattern list on a
// path matcher that read only its first pattern.
// Results are Neo4j 5.26.30's.
func TestExistsMultiClauseBodies(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, `CREATE (:Document {id: 'd1', current_version_id: 'v2'}), (:Document {id: 'd2', current_version_id: 'v1'}),
		(a:GraphNode {id: 'a', document_id: 'd1'}), (b:GraphNode {id: 'b', document_id: 'd9'}), (e:GraphNode {id: 'e', document_id: 'd2'}),
		(c:Correction {id: 'c', status: 'active', scope: ['all']}), (c2:Correction {id: 'c2', status: 'retired', scope: ['v1']}),
		(c)-[:CORRECTS]->(a), (c)-[:CORRECTS]->(b), (c2)-[:CORRECTS]->(e)`, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		body string
		want []string // the GraphNode ids for which EXISTS holds
	}{
		{"MATCH (c:Correction {status: 'active'})-[:CORRECTS]->(a) MATCH (d:Document {id: a.document_id})", []string{"a"}},
		{"MATCH (c:Correction {status: 'active'})-[:CORRECTS]->(a) MATCH (d:Document {id: a.document_id}) WHERE c.scope = ['all'] OR d.current_version_id IN c.scope", []string{"a"}},
		{"MATCH (c:Correction {status: 'active'})-[:CORRECTS]->(a), (d:Document {id: a.document_id})", []string{"a"}},
		{"MATCH (c:Correction)-[:CORRECTS]->(a) MATCH (d:Document) WHERE d.id = a.document_id", []string{"a", "e"}},
		{"MATCH (c:Correction)-[:CORRECTS]->(a) WITH c, a WHERE c.status = 'active' MATCH (d:Document {id: a.document_id})", []string{"a"}},
		{"MATCH (c:Correction)-[:CORRECTS]->(a) OPTIONAL MATCH (d:Document {id: a.document_id}) WITH c, d WHERE d IS NOT NULL", []string{"a", "e"}},
		{"UNWIND [1, 2] AS i MATCH (d:Document {id: a.document_id})", []string{"a", "e"}},
		{"MATCH (x:GraphNode) MATCH (y:Document {id: x.document_id})", []string{"a", "b", "e"}},
		{"MATCH (c:Correction {status: 'active'})-[:CORRECTS]->(a)", []string{"a", "b"}},
	} {
		ids := func(query string) []string {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			out := []string{}
			for _, row := range result.Rows {
				if len(row) == 1 || row[1] == true {
					out = append(out, row[0].(string))
				}
			}
			return out
		}
		require.Equal(t, tc.want, ids("MATCH (a:GraphNode) WHERE EXISTS { "+tc.body+" } RETURN a.id ORDER BY a.id"), tc.body)
		require.Equal(t, tc.want, ids("MATCH (a:GraphNode) RETURN a.id, EXISTS { "+tc.body+" } AS x ORDER BY a.id"), tc.body)
		require.Equal(t, tc.want, ids("MATCH (a:GraphNode) WITH a, COUNT { "+tc.body+" } AS n WHERE n > 0 RETURN a.id ORDER BY a.id"), tc.body)
	}
	// An empty body isn't an EXISTS subquery.
	fixtureAdmission340Context, fixtureAdmission340 := expressionTestContext(ctx, "EXISTS { }")
	_ = fixtureAdmission340
	_, recognized := exec.rowExistsSubqueryMatches(fixtureAdmission340, fixtureAdmission340Context, "EXISTS { }", nil)
	require.False(t, recognized)
}

// The bound-chain fast path: a chain of two hops, a second bound node and a
// bound relationship the path must agree with, and a WHERE reading the
// path's relationship. Results are Neo4j 5.26.30's.
func TestExistsBoundChainShapes(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(ctx, `CREATE (a:P {id: 'a'})-[:R {w: 1}]->(b:Q {id: 'b'})-[:S]->(:T), (c:P {id: 'c'})-[:R {w: 2}]->(d:Q {id: 'd'}),
		(a)-[:R {w: 3}]->(d)`, nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (p:P) WHERE EXISTS { (p)-[:R]->(:Q)-[:S]->(:T) } RETURN p.id ORDER BY p.id":               {{"a"}},
		"MATCH (p:P), (q:Q {id: 'd'}) WHERE EXISTS { (p)-[:R]->(q) } RETURN p.id ORDER BY p.id":          {{"a"}, {"c"}},
		"MATCH (p:P), (q:Q {id: 'b'}) WHERE EXISTS { (p)-[:R]->(q) } RETURN p.id ORDER BY p.id":          {{"a"}},
		"MATCH (p:P)-[r:R]->(q:Q {id: 'd'}) WHERE EXISTS { (p)-[r]->() } RETURN p.id, r.w ORDER BY p.id": {{"a", int64(3)}, {"c", int64(2)}},
		"MATCH (p:P) WHERE EXISTS { MATCH (p)-[r:R]->(q) WHERE r.w > 1 } RETURN p.id ORDER BY p.id":      {{"a"}, {"c"}},
		"MATCH (p:P) RETURN p.id, COUNT { MATCH (p)-[r:R]->(q) WHERE r.w > 1 } AS n ORDER BY p.id":       {{"a", int64(1)}, {"c", int64(1)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}
