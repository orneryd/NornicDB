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

	// These direct kernel checks need the same admitted source as public
	// execution. Missing admission is tested separately as an invariant.
	seed := func(pattern, where string) ([]*storage.Node, bool, bool) {
		statement := "MATCH " + pattern + " WHERE " + where + " RETURN r"
		fixtureAdmission830Context, fixtureAdmission830 := compileRelationshipStatement(ctx, statement, nil)
		_ = fixtureAdmission830
		return exec.relationshipIDSeedNodes(fixtureAdmission830, fixtureAdmission830Context, pattern, where)
	}
	nodes, none, ok := seed("(a)-[r]->(b)", fmt.Sprintf("elementId(r) = '%v'", elementID))
	require.True(t, ok)
	require.False(t, none)
	require.Len(t, nodes, 1, "the start node only")
	_, _, ok = seed("(a)-[r*1..2]->(b)", fmt.Sprintf("elementId(r) = '%v'", elementID))
	require.False(t, ok, "a variable-length pattern")
	_, _, ok = seed("(a)-[r]->(b)", "r.id = 7")
	require.False(t, ok)
}
