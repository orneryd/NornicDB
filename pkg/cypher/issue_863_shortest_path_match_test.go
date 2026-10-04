package cypher

// NornicDB #863: a shortestPath MATCH ignored its WHERE and the properties of
// an unlabelled endpoint, built its rows by hand (so ORDER BY, LIMIT,
// aggregation and WITH didn't apply), resolved a patterned end to its first
// node, and filtered the one shortest path by a path predicate afterwards.
// It now runs as a pipeline MATCH step. #876: a label test in the WHERE of a
// MATCH joining a bound variable was read as text. Answers are Neo4j
// 5.26.30's.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

const issue863Graph = `CREATE (:Code {id: 'c1'})-[:R {w:1}]->(:Document {id: 'd1'}), (:Other {id: 'o1'})-[:S]->(:Code:Document {id: 'b'}), ({id:'u'}),
	(x:P {id:'x'})-[:T {ok:false}]->(y:P {id:'y'}), (x)-[:T {ok:true}]->(:P {id:'z'})-[:T {ok:true}]->(y)`

const shortestPathCommonEndNodesPrefix = "The shortest path algorithm does not work when the start and end nodes are the same."

func TestIssue863ShortestPathMatchIsAPipelineStep(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, issue863Graph, nil)
			require.NoError(t, err)
			run := func(query string) ([][]interface{}, error) {
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
				}
				res, err := exec.Execute(ctx, query, nil)
				if err != nil {
					return nil, err
				}
				return res.Rows, nil
			}
			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b:Document)) RETURN length(p) ORDER BY length(p) LIMIT 1", [][]interface{}{{int64(1)}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b:Document)) RETURN b.id, length(p) ORDER BY b.id", [][]interface{}{{"d1", int64(1)}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b)) WHERE b:Document RETURN length(p) ORDER BY length(p) LIMIT 1", [][]interface{}{{int64(1)}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b)) WHERE b.id = 'd1' RETURN length(p)", [][]interface{}{{int64(1)}}},
				{"MATCH p = shortestPath((a {id:'o1'})-[*]-(b)) WHERE b.id IN ['b', 'c1'] RETURN b.id, length(p) ORDER BY b.id", [][]interface{}{{"b", int64(1)}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b {id:'d1'})) RETURN length(p), [n IN nodes(p) | n.id]", [][]interface{}{{int64(1), []interface{}{"c1", "d1"}}}},
				{"MATCH p = shortestPath((:Code {id:'c1'})-[*]-(:Document)) RETURN length(p)", [][]interface{}{{int64(1)}}},
				{"MATCH shortestPath((a {id:'c1'})-[*]-(b:Document)) RETURN b.id", [][]interface{}{{"d1"}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b:Document)) RETURN count(*)", [][]interface{}{{int64(1)}}},
				{"MATCH p = allShortestPaths((a {id:'c1'})-[*]-(b:Document)) RETURN b.id, length(p) ORDER BY b.id", [][]interface{}{{"d1", int64(1)}}},
				{"MATCH (a {id:'c1'}) MATCH p = shortestPath((a)-[*]-(b:Document)) RETURN b.id, length(p) ORDER BY b.id", [][]interface{}{{"d1", int64(1)}}},
				{"MATCH (a {id:'c1'}) MATCH p = shortestPath((a)-[*]-(b)) WHERE b:Document RETURN b.id, length(p) ORDER BY b.id", [][]interface{}{{"d1", int64(1)}}},
				{"MATCH (a {id:'c1'}) OPTIONAL MATCH p = shortestPath((a)-[*]-(b)) WHERE b.id = 'zz' RETURN a.id, b, p", [][]interface{}{{"c1", nil, nil}}},
				{"MATCH (a {id:'u'}) OPTIONAL MATCH p = shortestPath((a)-[*]-(b:Document)) RETURN a.id, b.id, p", [][]interface{}{{"u", nil, nil}}},
				{"MATCH (a:Other) OPTIONAL MATCH p = shortestPath((a)-[*]-(b:Document)) RETURN a.id, b.id, length(p) ORDER BY b.id", [][]interface{}{{"o1", "b", int64(1)}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b:Document)) RETURN p IS NOT NULL AS x LIMIT 5", [][]interface{}{{true}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b:Document)) WITH b, p RETURN b.id ORDER BY b.id", [][]interface{}{{"d1"}}},
				{"MATCH p = shortestPath((a:Code)-[*0..]-(b:Document)) RETURN a.id, b.id, length(p) ORDER BY a.id, b.id", [][]interface{}{{"b", "b", int64(0)}, {"c1", "d1", int64(1)}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[r*]-(b:Document)) RETURN [x IN r | type(x)] AS t, size(r)", [][]interface{}{{[]interface{}{"R"}, int64(1)}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*..0]-(b:Document)) RETURN count(*)", [][]interface{}{{int64(0)}}},
				// Path predicates find the shortest path that satisfies them.
				{"MATCH p = shortestPath((a {id:'x'})-[*]-(b {id:'y'})) WHERE all(r IN relationships(p) WHERE r.ok) RETURN length(p)", [][]interface{}{{int64(2)}}},
				{"MATCH (a {id:'x'}), (b {id:'y'}) MATCH p = shortestPath((a)-[*]-(b)) WHERE all(r IN relationships(p) WHERE r.ok) RETURN length(p)", [][]interface{}{{int64(2)}}},
				{"MATCH p = shortestPath((a {id:'x'})-[*]-(b {id:'y'})) WHERE length(p) > 1 RETURN length(p)", [][]interface{}{{int64(2)}}},
				{"MATCH p = allShortestPaths((a {id:'x'})-[*]-(b {id:'y'})) WHERE length(p) >= 2 RETURN length(p)", [][]interface{}{{int64(2)}}},
				{"MATCH p = shortestPath((a {id:'x'})-[*]-(b {id:'y'})) WHERE none(n IN nodes(p) WHERE n.id = 'z') RETURN length(p)", [][]interface{}{{int64(1)}}},
				{"MATCH p = shortestPath((a {id:'x'})-[rs*]-(b {id:'y'})) WHERE all(r IN rs WHERE r.ok) RETURN length(p)", [][]interface{}{{int64(2)}}},
				{"MATCH p = shortestPath((a {id:'x'})-[*..1]-(b {id:'y'})) WHERE length(p) > 1 RETURN length(p)", nil},
				// The expression form takes bound endpoints; labels filter them.
				{"MATCH (a {id:'c1'}), (b {id:'d1'}) RETURN length(shortestPath((a)-[*]-(b:Document))) AS l", [][]interface{}{{int64(1)}}},
				{"MATCH (a {id:'c1'}), (b {id:'u'}) RETURN shortestPath((a)-[*]-(b:Document)) AS p", [][]interface{}{{nil}}},
				{"MATCH (a {id:'c1'}), (b {id:'d1'}) RETURN size(allShortestPaths((a)-[*]-(b))) AS l", [][]interface{}{{int64(1)}}},
				{"MATCH (a {id:'c1'}), (b {id:'d1'}) WHERE length(shortestPath((a)-[*]-(b))) = 1 RETURN b.id", [][]interface{}{{"d1"}}},
				{"MATCH (a {id:'c1'}), (b {id:'d1'}) WITH a, b WHERE shortestPath((a)-[*]-(b)) IS NOT NULL RETURN b.id", [][]interface{}{{"d1"}}},
				{"MATCH (a {id:'c1'}), (b) WHERE b.id IN ['d1','u'] AND shortestPath((a)-[*]-(b)) IS NULL RETURN b.id", [][]interface{}{{"u"}}},
				// The shortestPath part of a comma-separated pattern.
				{"MATCH (a {id:'c1'}), shortestPath((a)-[*]-(b:Document)) RETURN b.id", [][]interface{}{{"d1"}}},
				{"MATCH (a {id:'c1'}), (b:Document), p = shortestPath((a)-[*]-(b)) RETURN b.id", [][]interface{}{{"d1"}}},
				{"MATCH p = shortestPath((a {id:'c1'})-[*]-(b:Document)), (c {id:'u'}) RETURN b.id, c.id", [][]interface{}{{"d1", "u"}}},
				{"MATCH (a {id:'c1'}) OPTIONAL MATCH (c {id:'u'}), p = shortestPath((a)-[*]-(b:Document)) RETURN c.id, b.id", [][]interface{}{{"u", "d1"}}},
				{"MATCH (a {id:'c1'}) OPTIONAL MATCH (c {id:'zz'}), p = shortestPath((a)-[*]-(b:Document)) RETURN c.id, b.id", [][]interface{}{{nil, nil}}},
				{"MATCH ()-[:S]->(), p = shortestPath((a {id:'c1'})-[*]-(b:Document)) RETURN b.id", [][]interface{}{{"d1"}}},
			} {
				rows, err := run(tc.query)
				require.NoError(t, err, tc.query)
				if tc.rows == nil {
					require.Empty(t, rows, tc.query)
					continue
				}
				require.Equal(t, tc.rows, rows, tc.query)
			}
			for _, tc := range []struct{ query, message string }{
				{"MATCH p = shortestPath((a:Code)-[*]-(b:Document)) RETURN count(p)", shortestPathCommonEndNodesPrefix},
				{"MATCH (a:Code) OPTIONAL MATCH p = shortestPath((a)-[*]-(b:Document)) RETURN count(p)", shortestPathCommonEndNodesPrefix},
				{"MATCH (a {id:'c1'}) RETURN shortestPath((a)-[*]-(a)) AS p", shortestPathCommonEndNodesPrefix},
				{"MATCH p = shortestPath((a {id:'c1'})-[*2..]-(b:Document)) RETURN count(*)", "shortestPath(...) does not support a minimal length different from 0 or 1"},
				{"MATCH (a {id:'c1'}) RETURN length(shortestPath((a)-[*2..]-(a))) AS l", "shortestPath(...) does not support a minimal length different from 0 or 1"},
				{"MATCH (a {id:'c1'}) OPTIONAL MATCH p = shortestPath((a)-[*2..]-(b)) RETURN p", "shortestPath(...) does not support a minimal length different from 0 or 1"},
				{"MATCH p = shortestPath((a {id:'c1'})-->(b)-->(c)) RETURN p", "shortestPath(...) requires a pattern containing a single relationship"},
				{"MATCH p = allShortestPaths((a {id:'c1'})) RETURN p", "allShortestPaths(...) requires a pattern containing a single relationship"},
				{"MATCH (a {id:'c1'}), (b) WHERE shortestPath((a)-[*]-(:Document)) IS NULL RETURN b", "A shortestPath(...) requires bound nodes when not part of a MATCH clause."},
				{"MATCH p = shortestPath((a {id:'x'})-[:T* {ok:true}]-(b {id:'y'})) RETURN length(p)", "shortestPath(...) contains properties {ok:true}. This is currently not supported."},
				{"MATCH (a {id:'x'}), (b {id:'y'}) RETURN shortestPath((a)-[* {ok:true}]-(b))", "shortestPath(...) contains properties {ok:true}. This is currently not supported."},
				{"MATCH (a {id:'c1'}) RETURN length(shortestPath((a)-[*]-(:Document))) AS l", "A shortestPath(...) requires bound nodes when not part of a MATCH clause."},
				{"MATCH (a {id:'c1'}) RETURN size(allShortestPaths((a)-[*]-(:Document))) AS l", "A allShortestPaths(...) requires bound nodes when not part of a MATCH clause."},
			} {
				_, err := run(tc.query)
				require.Error(t, err, tc.query)
				require.Contains(t, err.Error(), tc.message, tc.query)
			}
		})
	}
}

func TestIssue876LabelTestsInMultiPatternWhere(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, issue863Graph, nil)
			require.NoError(t, err)
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, tc := range []struct {
				query string
				ids   []interface{}
			}{
				{"MATCH (a {id:'c1'}) MATCH (a), (b) WHERE b:Document RETURN b.id ORDER BY b.id", []interface{}{"b", "d1"}},
				{"MATCH (a {id:'c1'}) MATCH (b), (a) WHERE b:Document RETURN b.id ORDER BY b.id", []interface{}{"b", "d1"}},
				{"MATCH (a {id:'c1'}) MATCH (a), (b) WHERE b:Document OR b:Other RETURN b.id ORDER BY b.id", []interface{}{"b", "d1", "o1"}},
				{"MATCH (a {id:'c1'}) MATCH (a), (b) WHERE b:Document = true RETURN b.id ORDER BY b.id", []interface{}{"b", "d1"}},
				{"MATCH (a {id:'c1'}) MATCH (a), (b) WHERE NOT b:Document AND NOT b:P AND b.id <> 'c1' RETURN b.id ORDER BY b.id", []interface{}{"o1", "u"}},
			} {
				res, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				ids := make([]interface{}, 0, len(res.Rows))
				for _, row := range res.Rows {
					ids = append(ids, row[0])
				}
				require.Equal(t, tc.ids, ids, tc.query)
			}
		})
	}
}
