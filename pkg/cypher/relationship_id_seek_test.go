package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A one-hop match whose relationship is pinned by elementId(r) or id(r)
// starts from that relationship's endpoints and
// returns what a scan of every node returns: each direction, undirected (two
// rows, one per orientation), a type that fits and one that doesn't, a
// missing relationship, a node's element ID, null, and endpoint labels the
// pattern requires.
func TestRelationshipIDSeekMatches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "UNWIND range(0, 49) AS i CREATE (:X {id: i})-[:R {id: i}]->(:Y {id: i})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:X {id: 7})-[r:R]->(b) RETURN elementId(r), id(r), elementId(a)", nil)
	require.NoError(t, err)
	elementID, id, nodeElementID := result.Rows[0][0], result.Rows[0][1], result.Rows[0][2]

	for query, want := range map[string][][]interface{}{
		"MATCH ()-[r]->() WHERE elementId(r) = $e RETURN r.id":                                {{int64(7)}},
		"MATCH ()-[r:R]->() WHERE elementId(r) = $e RETURN count(r)":                          {{int64(1)}},
		"MATCH (a)-[r]->(b) WHERE elementId(r) = $e RETURN a.id, b.id":                        {{int64(7), int64(7)}},
		"MATCH (b)<-[r]-(a) WHERE elementId(r) = $e RETURN a.id, b.id":                        {{int64(7), int64(7)}},
		"MATCH (a)-[r]-(b) WHERE elementId(r) = $e RETURN labels(a)[0] ORDER BY labels(a)[0]": {{"X"}, {"Y"}},
		"MATCH ()-[r:OTHER]->() WHERE elementId(r) = $e RETURN count(r)":                      {{int64(0)}},
		"MATCH (a:Y)-[r]->() WHERE elementId(r) = $e RETURN count(r)":                         {{int64(0)}},
		"MATCH (a:X {id: 7})-[r]->(b:Y) WHERE elementId(r) = $e RETURN count(r)":              {{int64(1)}},
		"MATCH (a:X {id: 8})-[r]->() WHERE elementId(r) = $e RETURN count(r)":                 {{int64(0)}},
		"MATCH (a:Y)-[r]-(b) WHERE elementId(r) = $e RETURN b.id":                             {{int64(7)}},
		"MATCH ()-[r]->() WHERE elementId(r) = $missing RETURN count(r)":                      {{int64(0)}},
		"MATCH ()-[r]->() WHERE elementId(r) = $node RETURN count(r)":                         {{int64(0)}},
		"MATCH ()-[r]->() WHERE elementId(r) = null RETURN count(r)":                          {{int64(0)}},
		"MATCH ()-[r]->() WHERE id(r) = $id RETURN r.id":                                      {{int64(7)}},
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"e": elementID, "id": id, "node": nodeElementID, "missing": fmt.Sprint(elementID) + "x"})
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}

	edge, none, ok := exec.relationshipIDSeedEdge(ctx, "(a)-[r]->(b)", fmt.Sprintf("elementId(r) = '%v'", elementID))
	require.True(t, ok)
	require.False(t, none)
	require.NotNil(t, edge)
	pinned := exec.parseTraversalPattern(ctx, "(a)-[r]->(b)")
	require.NotNil(t, pinned)
	require.Len(t, exec.relationshipIDSeedEndpointNodes(ctx, edge, pinned), 1, "the start node only")
	_, _, ok = exec.relationshipIDSeedEdge(ctx, "(a)-[r*1..2]->(b)", fmt.Sprintf("elementId(r) = '%v'", elementID))
	require.False(t, ok, "a variable-length pattern")
	_, _, ok = exec.relationshipIDSeedEdge(ctx, "(a)-[r]->(b)", "r.id = 7")
	require.False(t, ok)
}

// A match asks for its relationship pin once per row, and the pin is in the
// WHERE. Without a WHERE nothing is read for it, the pattern included; with
// one, an id comparison pins the relationship wherever it stands among the
// terms, and every other shape is declined as before.
func TestRelationshipIDSeedReadsTheWhereBeforeThePattern(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "UNWIND range(0, 49) AS i CREATE (:X {id: i})-[:R {id: i}]->(:Y {id: i})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:X {id: 7})-[r:R]->(b) RETURN elementId(r), elementId(a)", nil)
	require.NoError(t, err)
	elementID, nodeElementID := result.Rows[0][0], result.Rows[0][1]

	pinned := false
	allocations := testing.AllocsPerRun(100, func() {
		_, _, ok := exec.relationshipIDSeedEdge(ctx, "(a:X {id: 7})-[r:R]->(b:Y)", "")
		pinned = pinned || ok
	})
	require.False(t, pinned)
	require.Zero(t, allocations, "a match without a WHERE reads no pattern for a pin")

	pin := fmt.Sprintf("elementId(r) = '%v'", elementID)
	nodePin := fmt.Sprintf("elementId(a) = '%v'", nodeElementID)
	for _, shape := range []struct {
		pattern, where string
		ok, none       bool
	}{
		{"(a)-[r]->(b)", "a.id = 7", false, false},
		{"(a)-[r]->(b)", nodePin, false, false},
		{"(a)-[r]->(b)", pin + " OR a.id = 7", false, false},
		{"(a)-[r]->(b)-[s]->(c)", pin, false, false},
		{"(a)-[r]->(b)", pin, true, false},
		{"(a)-[r]->(b)", "(" + pin + ")", true, false},
		{"(a)-[r]->(b)", "a.id = 7 AND " + pin, true, false},
		{"(a)-[r]->(b)", pin + " AND b.id = 7", true, false},
		{"(a)-[r]->(b)", nodePin + " AND " + pin, true, false},
		{"(a)-[r:OTHER]->(b)", pin, true, true},
		{"(a)-[r]->(b)", fmt.Sprintf("elementId(r) = '%vx'", elementID), true, true},
		{"(a)-[r]->(b)", "elementId(r) = null", true, true},
	} {
		name := shape.pattern + " WHERE " + shape.where
		edge, none, ok := exec.relationshipIDSeedEdge(ctx, shape.pattern, shape.where)
		require.Equal(t, shape.ok, ok, name)
		require.Equal(t, shape.none, none, name)
		if shape.ok && !shape.none {
			require.NotNil(t, edge, name)
			require.EqualValues(t, 7, edge.Properties["id"], name)
		} else {
			require.Nil(t, edge, name)
		}
	}

	// The same through a match that runs once per row of an earlier clause.
	for query, want := range map[string][][]interface{}{
		"MATCH (a:X) WITH a MATCH (a)-[r:R]->(b) WHERE elementId(r) = $e RETURN a.id, b.id":         {{int64(7), int64(7)}},
		"MATCH (a:X) WITH a MATCH (a)-[r:R]->(b) WHERE b.id >= 0 AND elementId(r) = $e RETURN a.id": {{int64(7)}},
		"MATCH (a:X) WITH a MATCH (a)-[r:OTHER]->(b) WHERE elementId(r) = $e RETURN count(r)":       {{int64(0)}},
		"MATCH (a:X) WITH a MATCH (a)-[r:R]->(b) WHERE b.id = a.id RETURN count(r)":                 {{int64(50)}},
		"MATCH (a:X) WITH a MATCH (a)-[r:R]->(b) RETURN count(r)":                                   {{int64(50)}},
		"MATCH (a:X) WHERE EXISTS { MATCH (a)-[:R]->(:Y {id: a.id}) } RETURN count(a)":              {{int64(50)}},
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"e": elementID})
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}
