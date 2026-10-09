package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// MERGE takes any number of ON CREATE SET / ON MATCH SET clauses, each a SET
// of its own run in order, and a path of more than one relationship, matched
// or created as a whole (Neo4j 5.26.30, #907).
func TestMergeStructureMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_structure"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R {w: 1}]->(:Q {id: 2})-[:R {w: 2}]->(:Q {id: 3})", nil)
	require.NoError(t, err)
	inRolledBackTransaction := func(query string) (*ExecuteResult, error) {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		return exec.Execute(ctx, query, nil)
	}

	for _, testCase := range []struct {
		query string
		rows  [][]interface{}
		stats QueryStats
	}{
		{"MERGE (m:Q {id: 1}) ON MATCH SET m.x = 1 ON MATCH SET m.y = m.x RETURN m.x AS x, m.y AS y",
			[][]interface{}{{int64(1), int64(1)}}, QueryStats{PropertiesSet: 2}},
		{"MERGE (m:Q {id: 9}) ON CREATE SET m.x = 1 ON MATCH SET m.z = 1 ON CREATE SET m.y = m.x RETURN m.x AS x, m.y AS y, m.z AS z",
			[][]interface{}{{int64(1), int64(1), nil}}, QueryStats{NodesCreated: 1, PropertiesSet: 3, LabelsAdded: 1}},
		{"MERGE (m:Q {id: 1}) ON CREATE SET m.c = 1 ON MATCH SET m.x = 1 ON CREATE SET m.d = 1 ON MATCH SET m.y = 2 RETURN m.c AS c, m.d AS d, m.x AS x, m.y AS y",
			[][]interface{}{{nil, nil, int64(1), int64(2)}}, QueryStats{PropertiesSet: 2}},
		{"MERGE (m:Q {id: 1}) ON MATCH SET m:X ON MATCH SET m.x = 1 RETURN labels(m) AS l, m.x AS x",
			[][]interface{}{{[]interface{}{"Q", "X"}, int64(1)}}, QueryStats{PropertiesSet: 1, LabelsAdded: 1}},
		{"MERGE (a:Q {id: 1})-[:R]->(b:Q {id: 2})-[:R]->(c:Q {id: 3}) RETURN a.id AS a, c.id AS c",
			[][]interface{}{{int64(1), int64(3)}}, QueryStats{}},
		{"MERGE (a:W {k: 1})-[:T]->(b:W {k: 2})<-[:T]-(c:W {k: 3}) RETURN count(*) AS c",
			[][]interface{}{{int64(1)}}, QueryStats{NodesCreated: 3, RelationshipsCreated: 2, PropertiesSet: 3, LabelsAdded: 3}},
		{"MATCH (a:Q {id: 1}) MERGE (a)-[:R]->(b:Q {id: 2})-[:R]->(c:Q {id: 9}) RETURN c.id AS c",
			[][]interface{}{{int64(9)}}, QueryStats{NodesCreated: 2, RelationshipsCreated: 2, PropertiesSet: 2, LabelsAdded: 2}},
		{"MERGE p = (a:Q {id: 1})-[:R]->(b:Q {id: 2})-[:R]->(c:Q {id: 3}) RETURN length(p) AS l",
			[][]interface{}{{int64(2)}}, QueryStats{}},
		{"MERGE (a:Q {id: 1})-[r1:R]->(b:Q {id: 2})-[r2:R]->(c:Q {id: 3}) RETURN r1.w AS w1, r2.w AS w2",
			[][]interface{}{{int64(1), int64(2)}}, QueryStats{}},
		{"MERGE (a:Q {id: 1})-[:R]->(b:Q {id: 2})-[:R]->(c:Q {id: 3}) ON MATCH SET a.x = 1 RETURN a.x AS x",
			[][]interface{}{{int64(1)}}, QueryStats{PropertiesSet: 1}},
		{"MERGE (a:Q {id: 1})-[:R]->(b:Q {id: 2})-[:R]->(c:Q {id: 7}) ON CREATE SET c.new = true RETURN c.new AS n",
			[][]interface{}{{true}}, QueryStats{NodesCreated: 3, RelationshipsCreated: 2, PropertiesSet: 4, LabelsAdded: 3}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := inRolledBackTransaction(testCase.query)
			require.NoError(t, err)
			require.Equal(t, testCase.rows, result.Rows)
			require.NotNil(t, result.Stats)
			require.Equal(t, testCase.stats.NodesCreated, result.Stats.NodesCreated, "nodes created")
			require.Equal(t, testCase.stats.RelationshipsCreated, result.Stats.RelationshipsCreated, "relationships created")
			require.Equal(t, testCase.stats.PropertiesSet, result.Stats.PropertiesSet, "properties set")
			require.Equal(t, testCase.stats.LabelsAdded, result.Stats.LabelsAdded, "labels added")
		})
	}

	for _, query := range []string{
		"MERGE (a:W)-[:T]->(b:W {k: null})-[:T]->(c:W) RETURN 1 AS v",
		"MERGE (a:W)-[:T {w: null}]->(b:W)-[:T]->(c:W) RETURN 1 AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := inRolledBackTransaction(query)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SemanticError", code)
		})
	}
}

func TestSplitMergeClauseActions(t *testing.T) {
	parts := splitMergeClauseActions("(m:Q {s: 'ON MATCH SET'}) ON  CREATE   SET m.a = 1 ON MATCH SET m.b = 2, m.c = 3 ON CREATE SET m.d = 4")
	require.Equal(t, "(m:Q {s: 'ON MATCH SET'})", parts.pattern)
	require.Equal(t, 2, parts.onCreate.len())
	require.Equal(t, "m.d = 4", parts.onCreate.assignments(1))
	require.Equal(t, "SET m.a = 1 SET m.d = 4", parts.onCreate.setText())
	require.Equal(t, "SET m.b = 2, m.c = 3", parts.onMatch.setText())
	require.Equal(t, "", mergeActionClauses{}.setText())
	require.Equal(t, []string{"m.b = 2", "m.c = 3"}, mergeActionAssignments(parts.onMatch))
	require.Equal(t, mergeClauseActions{pattern: "(n)"}, splitMergeClauseActions(" (n) "))
	// More actions than the stack buffer holds.
	many := splitMergeClauseActions("(n) ON MATCH SET n.a = 1 ON MATCH SET n.b = 2 ON MATCH SET n.c = 3 ON MATCH SET n.d = 4 ON MATCH SET n.e = 5 ON MATCH SET n.f = 6 ON MATCH SET n.g = 7 ON MATCH SET n.h = 8 ON MATCH SET n.i = 9")
	require.Equal(t, 9, many.onMatch.len())
	require.Equal(t, "n.i = 9", many.onMatch.assignments(8))
	require.Equal(t, "SET n.c = 3", many.onMatch.set(2))
}

// A MERGE path reads each relationship as a single-relationship MERGE does:
// every direction, an undirected one created left to right, and every
// relationship of the path validated (Neo4j 5.26.30, #907).
func TestMergePathSegmentsMatchNeo4j(t *testing.T) {
	for _, testCase := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"MERGE (a:MA)-[:R]->(b:MB)-[:S]-(c:MC) WITH 1 AS one MERGE (a:MA)-[:R]->(b:MB)-[:S]-(c:MC) WITH 1 AS one MATCH p = (:MA)-[:R]->(:MB)-[:S]->(:MC) RETURN count(p) AS c", [][]interface{}{{int64(1)}}},
		{"MERGE (a:MA)-[:R]-(b:MB)-[:S]->(c:MC) WITH a MATCH (a)-[:R]->(x) RETURN labels(x) AS l", [][]interface{}{{[]interface{}{"MB"}}}},
		{"MERGE (a:MA)<-[:R]-(b:MB)-[:S]->(c:MC) WITH a MATCH (x)-[:R]->(a) RETURN labels(x) AS l", [][]interface{}{{[]interface{}{"MB"}}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_path_segments"))
			result, err := exec.Execute(context.Background(), testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_path_invalid"))
	for query, code := range map[string]string{
		"MERGE (a:MA)-[:R]->(b:MB)-[x]->(c:MC) RETURN 1 AS v":                 "Neo.ClientError.Statement.SyntaxError",
		"MERGE (a:MA)-[:R]->(b:MB)-[:S*2]->(c:MC) RETURN 1 AS v":              "Neo.ClientError.Statement.SyntaxError",
		"MERGE (a:MA)-[:R]->(b:MB)-[:S|T]->(c:MC) RETURN 1 AS v":              "Neo.ClientError.Statement.SyntaxError",
		"MERGE (a:MA)-[:R]->(b:MB {k: null})-[:S]->(c:MC) RETURN 1 AS v":      "Neo.ClientError.Statement.SemanticError",
		"MERGE (a:MA)-[:R {k: 0.0 / 0.0}]->(b:MB)-[:S]->(c:MC) RETURN 1 AS v": "Neo.ClientError.Statement.SemanticError",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, query)
	}
}

func TestMergePathHelpers(t *testing.T) {
	require.Equal(t, []string{"(a)-[:R]->(b {l: [1]})", "(b {l: [1]})<-[:S]-(c)"}, mergePathSegments("p = (a)-[:R]->(b {l: [1]})<-[:S]-(c)"))
	require.Nil(t, mergePathSegments("(a)-[:R]->(b"))
	require.Nil(t, mergePathSegments("(a)-[:R->(b)"))
	require.Nil(t, mergePathSegments("(a)"))
	require.Equal(t, "(a)-[:R]->(b)-[:S]->(c)", directedMergePath("(a)-[:R]-(b)-[:S]-(c)"))
	require.Equal(t, "(a)<-[:R]-(b)-[:`S]`]->(c {k: '-('})", directedMergePath("(a)<-[:R]-(b)-[:`S]`]-(c {k: '-('})"))
	require.Equal(t, "(a)-[:R]->(b)", directedMergePath("(a)-[:R]->(b)"))
	require.Equal(t, "(a)-[:R", directedMergePath("(a)-[:R"))
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	require.False(t, exec.isMultiRelationshipPattern("(a)-[:R]->(b)"))
	require.True(t, exec.isMultiRelationshipPattern("(a)-[:R]->(b)-[:S]-(c)"))
}
