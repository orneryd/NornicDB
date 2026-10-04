package cypher

// NornicDB #878: a WHERE inside a node or relationship pattern is a predicate
// on that element, as if it were in the clause's WHERE; on a quantified
// relationship it applies to each relationship. Answers are Neo4j 5.26.30's.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue878PatternElementWhere(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:Q {id:1})-[:R {w:1}]->(:Q {id:2})-[:R {w:2}]->(:Q {id:3})-[:R {w:3}]->(:Q {id:4})", nil)
			require.NoError(t, err)
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				{"MATCH (a:Q WHERE a.id > 2)-[:R]->(b) RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
				{"MATCH (a)-[r:R WHERE r.w > 1]->(b) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
				{"MATCH (a)-[r WHERE r.w = 2]-(b) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
				{"MATCH (a)-[r:R WHERE r.w > 1]->{1,2}(b) RETURN count(*) AS c", [][]interface{}{{int64(3)}}},
				{"MATCH (a WHERE a.id = 1) RETURN a.id", [][]interface{}{{int64(1)}}},
				{"MATCH (a:Q {id: 1} WHERE a.id = 1) RETURN a.id", [][]interface{}{{int64(1)}}},
				{"OPTIONAL MATCH (a:Q WHERE a.id = 9) RETURN a", [][]interface{}{{nil}}},
				{"MATCH (a:Q WHERE a.id < 3)-[:R]->(b WHERE b.id > 1) RETURN a.id, b.id ORDER BY a.id", [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}}},
				{"MATCH (a:Q WHERE a.id = 1) WHERE a.id = 1 RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
				{"MATCH p = (a WHERE a.id = 1)-[:R]->(b) RETURN length(p)", [][]interface{}{{int64(1)}}},
				{"MATCH (a WHERE a.id = 1)-[r:R]->(b) WHERE r.w = 1 RETURN b.id", [][]interface{}{{int64(2)}}},
				{"RETURN EXISTS { MATCH (a:Q WHERE a.id = 4) } AS e", [][]interface{}{{true}}},
				{"RETURN [(a:Q WHERE a.id > 2)-[:R]->(b) | b.id] AS l", [][]interface{}{{[]interface{}{int64(4)}}}},
				{"MATCH (a:Q WHERE a.id IN [1, 2])-[r:R WHERE r.w <> 1]->(b:Q WHERE b.id = 3) RETURN a.id, r.w, b.id", [][]interface{}{{int64(2), int64(2), int64(3)}}},
				{"MATCH (a WHERE a.name = 'WHERE x') RETURN count(*) AS c", [][]interface{}{{int64(0)}}},
				{"MATCH (a:Q WHERE a.id > 1 AND a.id < 4) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
				{"MATCH (a:Q WHERE a.id = 2)<-[:R WHERE true]-(b) RETURN b.id", [][]interface{}{{int64(1)}}},
				{"MATCH (a)-[:R WHERE a.id = 1]->(b) RETURN b.id", [][]interface{}{{int64(2)}}},
				{"MATCH (a)-->{1,2}(b WHERE b.id = 4) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
				{"MATCH (a:Q WHERE a.id = 1) MATCH (a)-[:R]->(b WHERE b.id = 2) RETURN b.id", [][]interface{}{{int64(2)}}},
			} {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
			for _, tc := range []struct{ query, code, message string }{
				{"MATCH (a)-[r:R*1..2 WHERE r.w > 1]->(b) RETURN count(*) AS c", "Neo.ClientError.Statement.SyntaxError", "Relationship pattern predicates are not supported for variable-length relationships."},
				{"CREATE (a:Q WHERE a.id = 1)", "Neo.ClientError.Statement.SyntaxError", "Node pattern predicates are not allowed in a CREATE clause, but only in a MATCH clause or inside a pattern comprehension"},
				{"MERGE (a:Q WHERE a.id = 1)", "Neo.ClientError.Statement.SyntaxError", "Node pattern predicates are not allowed in a MERGE clause, but only in a MATCH clause or inside a pattern comprehension"},
				{"MATCH (a)-[r:R* WHERE r.w > 0]->(b) RETURN count(*) AS c", "Neo.ClientError.Statement.SyntaxError", "Relationship pattern predicates are not supported for variable-length relationships."},
			} {
				_, err := exec.Execute(ctx, tc.query, nil)
				require.Equal(t, tc.code+": "+tc.message, statusText(err), tc.query)
			}
		})
	}
}
