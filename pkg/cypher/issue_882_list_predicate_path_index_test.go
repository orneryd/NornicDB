package cypher

// NornicDB #882: in a traversal's WHERE, a list predicate (all, any, none,
// single) whose condition indexes the path or the relationship list (r[i],
// nodes(p)[i]) dropped every row: the condition was evaluated without the
// path variables. The path context now gives them to it, with a
// variable-length relationship variable as its list of relationships, as
// RETURN sees it. Answers are Neo4j 5.26.30's.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue882ListPredicateIndexesThePath(t *testing.T) {
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
				{"MATCH p=(s {id:1})-[r:R*2]->(t) WHERE all(i IN range(0,1) WHERE r[i].w < 3) RETURN count(*)", [][]interface{}{{int64(1)}}},
				{"MATCH p=(s {id:1})-[r:R*2]->(t) WHERE all(i IN range(0,1) WHERE all(x IN [r[i]] WHERE x.w < 3)) RETURN count(*)", [][]interface{}{{int64(1)}}},
				{"MATCH p = (s)-[r:R*1..3]->(t) WHERE all(i IN range(0, size(r)-1) WHERE all(b IN [nodes(p)[i+1]] WHERE b.id < 4)) RETURN count(*)", [][]interface{}{{int64(3)}}},
				{"MATCH p = (s)-[r:R*1..3]->(t) WHERE any(i IN range(0, size(r)-1) WHERE nodes(p)[i].id = 2) RETURN count(*)", [][]interface{}{{int64(4)}}},
				{"MATCH p=(s {id:1})-[r:R*2]->(t) RETURN all(i IN range(0,1) WHERE r[i].w < 3)", [][]interface{}{{true}}},
				{"MATCH (s {id:1})-[r:R*2]->(t) WHERE all(i IN [0] WHERE r[0].w = 1) RETURN count(*)", [][]interface{}{{int64(1)}}},
				{"MATCH (s)-[r:R*1..3]->(t) WHERE none(i IN range(0, size(r)-1) WHERE r[i].w = 2) RETURN s.id, t.id ORDER BY s.id, t.id", [][]interface{}{{int64(1), int64(2)}, {int64(3), int64(4)}}},
				{"MATCH (s)-[r:R*1..3]->(t) WHERE single(i IN range(0, size(r)-1) WHERE r[i].w > 1) RETURN s.id, t.id ORDER BY s.id, t.id", [][]interface{}{{int64(1), int64(3)}, {int64(2), int64(3)}, {int64(3), int64(4)}}},
				{"MATCH p = (s)-[r:R*2..3]->(t) WHERE all(i IN range(0, size(r)-2) WHERE r[i].w < r[i+1].w) RETURN s.id, t.id ORDER BY s.id, t.id", [][]interface{}{{int64(1), int64(3)}, {int64(1), int64(4)}, {int64(2), int64(4)}}},
				{"MATCH p = (s)-[:R*1..3]->(t) WHERE all(i IN range(0, length(p)-1) WHERE relationships(p)[i].w <= 2) RETURN s.id, t.id ORDER BY s.id, t.id", [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(3)}, {int64(2), int64(3)}}},
				{"MATCH p = (s)-[:R*1..3]->(t) WHERE any(n IN nodes(p) WHERE n.id = 3) AND all(i IN range(1, size(nodes(p))-1) WHERE nodes(p)[i].id > nodes(p)[i-1].id) RETURN s.id, t.id ORDER BY s.id, t.id", [][]interface{}{{int64(1), int64(3)}, {int64(1), int64(4)}, {int64(2), int64(3)}, {int64(2), int64(4)}, {int64(3), int64(4)}}},
				{"MATCH (s {id:1})-[r:R*2]->(t) WHERE EXISTS { MATCH (x:Q) WHERE x.id = size(r) } RETURN count(*)", [][]interface{}{{int64(1)}}},
				{"MATCH (s {id:1})-[r:R*2]->(t) WHERE EXISTS { MATCH (x:Q) WHERE x.id = r[1].w } RETURN count(*)", [][]interface{}{{int64(1)}}},
				{"MATCH p = (s {id:1})-[r:R*2]->(t) WHERE EXISTS { MATCH (x:Q) WHERE x.id = length(p) } RETURN count(*)", [][]interface{}{{int64(1)}}},
				{"MATCH (s {id:1}) OPTIONAL MATCH (s)-[r:R*2]->(t) RETURN size(r), [x IN r | x.w]", [][]interface{}{{int64(2), []interface{}{int64(1), int64(2)}}}},
				{"MATCH (s {id:4}) OPTIONAL MATCH (s)-[r:R*2]->(t) RETURN s.id, r", [][]interface{}{{int64(4), nil}}},
				{"MATCH (s {id:1}) OPTIONAL MATCH p = (s)-[r:R*2]->(t) RETURN length(p), [n IN nodes(p) | n.id]", [][]interface{}{{int64(2), []interface{}{int64(1), int64(2), int64(3)}}}},
				{"MATCH (s {id:1})-[r:R*1..2]->(t) WHERE all(i IN range(0, size(r)-1) WHERE r[i].w = i + 1) RETURN t.id ORDER BY t.id", [][]interface{}{{int64(2)}, {int64(3)}}},
				{"MATCH (s {id:1})-[r:R*1..3]->(t) WITH s, r, t WHERE all(i IN range(0, size(r)-1) WHERE r[i].w < 3) RETURN t.id ORDER BY t.id", [][]interface{}{{int64(2)}, {int64(3)}}},
			} {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
		})
	}
}
