package cypher

// NornicDB #864: a quantifier after a relationship (-[:R]->{1,3}, -->+,
// <-[r]-*) was dropped, so the pattern matched single relationships. It now
// matches what Neo4j matches: the variable-length relationship with those
// bounds. Answers are Neo4j 5.26.30's.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue864RelationshipQuantifiers(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			for _, data := range []struct {
				graph string
				rows  []struct {
					query string
					rows  [][]interface{}
				}
				errors []struct{ query, message string }
			}{
				{
					graph: "CREATE (:Q {id:1})-[:R {w:1}]->(:Q {id:2})-[:R {w:2}]->(:Q {id:3})-[:R {w:3}]->(:Q {id:4})",
					rows: []struct {
						query string
						rows  [][]interface{}
					}{
						{"MATCH (a)-[:R]->{1,3}(b) RETURN count(*) AS c", [][]interface{}{{int64(6)}}},
						{"MATCH (a)-[:R]->{2}(b) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
						{"MATCH (a)-[:R]->+(b) RETURN count(*) AS c", [][]interface{}{{int64(6)}}},
						{"MATCH (a)-[:R]->{0,1}(b) RETURN count(*) AS c", [][]interface{}{{int64(7)}}},
						{"MATCH (a)-[:R]->{2,}(b) RETURN count(*) AS c", [][]interface{}{{int64(3)}}},
						{"MATCH (a)-[:R]->{,2}(b) RETURN count(*) AS c", [][]interface{}{{int64(9)}}},
						{"MATCH (a)-[:R]->*(b) RETURN count(*) AS c", [][]interface{}{{int64(10)}}},
						{"MATCH (a)-->{2}(b) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
						{"MATCH (a)<-[:R]-{2}(b) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
						{"MATCH (a)-[:R]-{2}(b) RETURN count(*) AS c", [][]interface{}{{int64(4)}}},
						{"MATCH (a {id:1})-[r:R]->{1,3}(b) RETURN b.id, size(r), [x IN r | x.w] ORDER BY b.id", [][]interface{}{
							{int64(2), int64(1), []interface{}{int64(1)}},
							{int64(3), int64(2), []interface{}{int64(1), int64(2)}},
							{int64(4), int64(3), []interface{}{int64(1), int64(2), int64(3)}},
						}},
						{"MATCH (a)-[:R {w:2}]->{1,2}(b) RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
						{"MATCH p = (a {id:1})-[:R]->{3}(b) RETURN length(p)", [][]interface{}{{int64(3)}}},
						{"MATCH (a)-[r:R]->{1,2}(b) WHERE all(x IN r WHERE x.w > 1) RETURN count(*) AS c", [][]interface{}{{int64(3)}}},
						{"MATCH (a)-[:R]->{1,2}(b)-[:R]->(c) RETURN count(*) AS c", [][]interface{}{{int64(3)}}},
						{"MATCH (a)-[:R]->{1}(b)-[:R]->{1}(c) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
						{"MATCH (a {id:1}) OPTIONAL MATCH (a)-[:R]->{4}(b) RETURN a.id, b", [][]interface{}{{int64(1), nil}}},
						{"MATCH (a {id:1}) WHERE EXISTS { (a)-[:R]->{3}() } RETURN a.id", [][]interface{}{{int64(1)}}},
						{"MATCH (a {id:1}) RETURN COUNT { (a)-[:R]->+() } AS n", [][]interface{}{{int64(3)}}},
						// A * in a backticked type is part of the name (#879).
						{"MATCH (a)-[:`R*`]->(b) RETURN count(*) AS c", [][]interface{}{{int64(0)}}},
						{"MATCH (a)-[:`R*`]->{1,2}(b) RETURN count(*) AS c", [][]interface{}{{int64(0)}}},
						{"MATCH (a)-[r:`R..S` {w: '*'}]->(b) RETURN count(*) AS c", [][]interface{}{{int64(0)}}},
					},
					errors: []struct{ query, message string }{
						{"MATCH (a)-[:R*]->{1,2}(b) RETURN count(*) AS c", "Variable length relationships cannot be part of a quantified path pattern."},
						{"CREATE (a)-[:R]->{2}(b)", "Quantified path patterns cannot be used in a CREATE clause, but only in a MATCH clause."},
						{"MERGE (a)-->+(b)", "Quantified path patterns cannot be used in a MERGE clause, but only in a MATCH clause."},
						{"MATCH (a) WHERE (a)-[:R]->{2}() RETURN a.id", "Invalid input '{'"},
						{"MATCH (a {id:1}) RETURN [(a)-[:R]->{1,2}(b) | b.id] AS ids", "Invalid input '{'"},
					},
				},
				{
					graph: "CREATE (:Q {id:1})-[:R {w:1}]->(:Q {id:2})-[:S {w:2}]->(:Q {id:3})-[:R {w:3}]->(:Q {id:4})",
					rows: []struct {
						query string
						rows  [][]interface{}
					}{
						{"MATCH (a)-[r:!S]->{1,2}(b) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
						{"MATCH (a)-[:!S]->{1,2}(b) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
						{"MATCH (a)-[:R|S]->{2}(b) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
						{"MATCH (a)-[r:%]->{3}(b) RETURN size(r) AS c", [][]interface{}{{int64(3)}}},
					},
				},
			} {
				exec := newAsyncStackTestExecutor(t)
				ctx := context.Background()
				_, err := exec.Execute(ctx, data.graph, nil)
				require.NoError(t, err)
				run := func(query string) (*ExecuteResult, error) {
					if mode == "explicit transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
					}
					return exec.Execute(ctx, query, nil)
				}
				for _, tc := range data.rows {
					res, err := run(tc.query)
					require.NoError(t, err, tc.query)
					require.Equal(t, tc.rows, res.Rows, tc.query)
				}
				for _, tc := range data.errors {
					_, err := run(tc.query)
					require.Error(t, err, tc.query)
					require.Contains(t, err.Error(), tc.message, tc.query)
				}
			}
		})
	}
}

// relationshipQuantifierAt reads only well-formed quantifiers; anything else
// after an arrow (a node, a map) is not one.
func TestRelationshipQuantifierAt(t *testing.T) {
	for text, want := range map[string]relationshipQuantifier{
		"+": {min: 1, max: -1, end: 1}, "*": {min: 0, max: -1, end: 1},
		"{3}": {min: 3, max: 3, end: 3}, "{ 1 , 4 }": {min: 1, max: 4, end: 9},
		"{2,}": {min: 2, max: -1, end: 4}, "{,5}": {min: 0, max: 5, end: 4},
	} {
		got, ok := relationshipQuantifierAt(text, 0, len(text))
		require.True(t, ok, text)
		require.Equal(t, want, got, text)
	}
	for _, text := range []string{"", "(", "{", "{1", "{}", "{x}", "{-1}", "{1,x}", "{x,1}", "{w: 1}"} {
		_, ok := relationshipQuantifierAt(text, 0, len(text))
		require.False(t, ok, text)
	}
}
