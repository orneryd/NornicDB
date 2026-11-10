package cypher

// n.has_embedding is the stored truth on every read (NOR-407). It is no node
// property: it is read from the node's embedding metadata, which the embed
// worker keeps with the vectors, outside the node body. A read by id returned
// it; a label scan that asked storage for the property has_embedding got a
// body without that metadata and reported false.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// hasEmbeddingGraph holds five summaries: q1 and q2 are searchable and have
// managed vectors, q3 is searchable and has none, q4 is not searchable and
// has managed vectors, and v1 is searchable and has only a vector property.
type hasEmbeddingGraph struct {
	exec *StorageExecutor
}

var hasEmbeddingExpected = map[string]bool{"q1": true, "q2": true, "q3": false, "q4": true, "v1": false}

func newHasEmbeddingGraph(t *testing.T) *hasEmbeddingGraph {
	t.Helper()
	engine := newTestMemoryEngine(t).BadgerEngine
	g := &hasEmbeddingGraph{exec: NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"), QueryCachePolicy{MaxEntries: 0, TTL: 0})}
	for _, statement := range []string{
		"CREATE CONSTRAINT pd_id IF NOT EXISTS FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE",
		"CREATE INDEX pd_kind IF NOT EXISTS FOR (n:PDSummary) ON (n.kind)",
		"CREATE (:PDGraphNode:PDSummary:PDSearchable {id: 'q1', kind: 'summary', title: 't'})",
		"CREATE (:PDGraphNode:PDSummary:PDSearchable {id: 'q2', kind: 'summary', title: 't'})",
		"CREATE (:PDGraphNode:PDSummary:PDSearchable {id: 'q3', kind: 'summary', title: 't'})",
		"CREATE (:PDGraphNode:PDSummary {id: 'q4', kind: 'summary', title: 't'})",
		"CREATE (:PDGraphNode:PDSummary:PDSearchable {id: 'v1', kind: 'summary', title: 't'})",
		"CREATE (:PDGraphNode:PDEntity {id: 'e1', kind: 'entity', title: 't'})",
		"CREATE (:PDGraphNode:PDHub {id: 'hub'})",
		"MATCH (h:PDHub), (n:PDSummary) CREATE (h)-[:HAS]->(n)",
		"MATCH (n:PDGraphNode {id: 'v1'}) CALL db.create.setNodeVectorProperty(n, 'embedding', [0.1, 0.2, 0.3, 0.4]) RETURN n.id",
	} {
		g.run(t, statement, nil)
	}
	// The embed worker's final write: vectors and their metadata in the
	// embedding sidecar.
	for _, id := range []string{"q1", "q2", "q4"} {
		rows := g.run(t, "MATCH (n:PDGraphNode {id: $id}) RETURN elementId(n)", map[string]interface{}{"id": id}).Rows
		require.Len(t, rows, 1)
		elementID := fmt.Sprint(rows[0][0])
		nodeID := storage.NodeID("nornic:" + elementID[strings.LastIndex(elementID, ":")+1:])
		writeManagedTestChunks(t, engine, nodeID, [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}})
	}
	return g
}

func (g *hasEmbeddingGraph) run(t *testing.T, query string, params map[string]interface{}) *ExecuteResult {
	t.Helper()
	result, err := g.exec.Execute(t.Context(), query, params)
	require.NoError(t, err, query)
	return result
}

func TestHasEmbeddingIsTheStoredTruthOnEveryRead(t *testing.T) {
	ids := []interface{}{"q1", "q2", "q3", "q4", "v1"}
	// byID checks rows of (id, has_embedding) for the given summaries.
	byID := func(want ...string) func(*testing.T, [][]interface{}) {
		return func(t *testing.T, rows [][]interface{}) {
			got := map[string]interface{}{}
			for _, row := range rows {
				got[fmt.Sprint(row[0])] = row[1]
			}
			expected := map[string]interface{}{}
			for _, id := range want {
				expected[id] = hasEmbeddingExpected[id]
			}
			require.Equal(t, expected, got)
			require.Len(t, rows, len(want))
		}
	}
	all := byID("q1", "q2", "q3", "q4", "v1")
	shapes := []struct {
		name   string
		query  string
		params map[string]interface{}
		check  func(*testing.T, [][]interface{})
	}{
		{"by id", "UNWIND $ids AS i MATCH (n:PDGraphNode {id: i}) RETURN n.id AS id, n.has_embedding AS h", map[string]interface{}{"ids": ids}, all},
		{"by one id", "MATCH (n:PDGraphNode {id: 'q4'}) RETURN n.id AS id, n.has_embedding AS h", nil, byID("q4")},
		{"label scan", "MATCH (n:PDSummary) RETURN n.id AS id, n.has_embedding AS h", nil, all},
		{"label scan, ordered", "MATCH (n:PDSummary) RETURN n.id AS id, n.has_embedding AS h ORDER BY id LIMIT 10", nil, all},
		{"label scan, the flag alone", "MATCH (n:PDSummary) RETURN n.has_embedding AS h", nil, func(t *testing.T, rows [][]interface{}) {
			flags := make([]string, 0, len(rows))
			for _, row := range rows {
				flags = append(flags, fmt.Sprint(row[0]))
			}
			sort.Strings(flags)
			require.Equal(t, []string{"false", "false", "true", "true", "true"}, flags)
		}},
		{"label scan with IN", "MATCH (n:PDSummary) WHERE n.id IN $ids RETURN n.id AS id, n.has_embedding AS h", map[string]interface{}{"ids": ids}, all},
		{"label scan through WITH", "MATCH (n:PDSummary) WITH n.id AS id, n.has_embedding AS h RETURN id, h", nil, all},
		{"label scan with the node", "MATCH (n:PDSummary) RETURN n.id AS id, n.has_embedding AS h, n", nil, all},
		{"filtered by the flag", "MATCH (n:PDSummary) WHERE n.has_embedding = true RETURN n.id AS id, n.has_embedding AS h", nil, byID("q1", "q2", "q4")},
		{"counted by the flag", "MATCH (n:PDSummary) WHERE n.has_embedding = true RETURN count(n) AS c", nil, func(t *testing.T, rows [][]interface{}) {
			require.Equal(t, [][]interface{}{{int64(3)}}, rows)
		}},
		{"grouped by the flag", "MATCH (n:PDSummary) RETURN n:PDSearchable AS s, n.has_embedding AS h, count(*) AS c", nil, func(t *testing.T, rows [][]interface{}) {
			groups := make([]string, 0, len(rows))
			for _, row := range rows {
				groups = append(groups, fmt.Sprint(row...))
			}
			sort.Strings(groups)
			require.Equal(t, []string{"false true 1", "true false 2", "true true 2"}, groups)
		}},
		{"indexed property in braces", "MATCH (n:PDSummary {kind: 'summary'}) RETURN n.id AS id, n.has_embedding AS h", nil, all},
		{"unindexed property in braces", "MATCH (n:PDSummary {title: 't'}) RETURN n.id AS id, n.has_embedding AS h", nil, all},
		{"unindexed property in WHERE", "MATCH (n:PDSummary) WHERE n.title = 't' RETURN n.id AS id, n.has_embedding AS h", nil, all},
		{"through a relationship", "MATCH (:PDHub)-[:HAS]->(n:PDSummary) RETURN n.id AS id, n.has_embedding AS h", nil, all},
	}
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			g := newHasEmbeddingGraph(t)
			if explicit {
				g.run(t, "BEGIN", nil)
				t.Cleanup(func() { _, _ = g.exec.Execute(context.Background(), "ROLLBACK", nil) })
			}
			// Each shape runs three times: a scan that repeats reads through
			// the node read cache.
			for round := 1; round <= 3; round++ {
				for _, shape := range shapes {
					t.Run(fmt.Sprintf("%s/%d", shape.name, round), func(t *testing.T) {
						shape.check(t, g.run(t, shape.query, shape.params).Rows)
					})
				}
			}
		})
	}
}
