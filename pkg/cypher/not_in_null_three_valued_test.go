package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOT over IN follows Cypher's three-valued logic on every WHERE route (#536):
// x IN null, a null x, and a list that holds null without a match are null;
// NOT null is null; WHERE keeps only true rows. `null IN []` is false.
//
// Data: (:T {id: 1}), (:T {id: 2}), (:T) - the last one has no id - each linked
// to one (:M) node. Expected counts are Neo4j 5's.
func TestNotInNullUsesThreeValuedLogic(t *testing.T) {
	predicates := []struct {
		where  string // uses X for the tested value
		params map[string]interface{}
		want   int64
	}{
		{"NOT X IN $l", map[string]interface{}{"l": nil}, 0},
		{"X IN $l", map[string]interface{}{"l": nil}, 0},
		{"NOT X IN null", nil, 0},
		{"NOT X IN [1]", nil, 1},
		{"NOT (X IN [1])", nil, 1},
		{"NOT X IN [1, null]", nil, 0},
		{"X IN [1, null]", nil, 1},
		{"X NOT IN [1, null]", nil, 0},
		{"NOT X IN $l", map[string]interface{}{"l": []interface{}{int64(1)}}, 1},
		{"NOT X IN $l", map[string]interface{}{"l": []interface{}{int64(1), nil}}, 0},
		{"NOT X IN []", nil, 3},
		{"NOT (X IN [1]) OR X = 1", nil, 2},
		{"NOT X IN [1] AND X > 0", nil, 1},
		{"NOT X IN [1] AND X > 5", nil, 0},
		{"X > 0 AND NOT X IN [1]", nil, 1},
	}
	shapes := []struct {
		name string
		stmt string // %s is the WHERE predicate
		x    string
	}{
		{"node count", "MATCH (n:T) WHERE %s RETURN count(n) AS c", "n.id"},
		{"node rows", "MATCH (n:T) WHERE %s RETURN n", "n.id"},
		{"multi-node match", "MATCH (n:T), (m:M) WHERE %s RETURN count(*) AS c", "n.id"},
		{"relationship traversal", "MATCH (n:T)-[:R]->(m:M) WHERE %s RETURN count(*) AS c", "n.id"},
		{"optional match", "MATCH (m:M) OPTIONAL MATCH (n:T)-[:R]->(m) WHERE %s RETURN count(n) AS c", "n.id"},
		{"with projection", "MATCH (n:T) WITH n.id AS id WHERE %s RETURN count(*) AS c", "id"},
		{"unwind values", "UNWIND [1, 2, null] AS id WITH id WHERE %s RETURN count(*) AS c", "id"},
		{"case when", "MATCH (n:T) RETURN sum(CASE WHEN %s THEN 1 ELSE 0 END) AS c", "n.id"},
		{"match set", "MATCH (n:T) WHERE %s SET n.seen = true RETURN n", "n.id"},
	}

	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newThreeValuedServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			t.Run(stack+"/"+mode, func(t *testing.T) {
				exec := build(t)
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (m:M) CREATE (:T {id: 1})-[:R]->(m), (:T {id: 2})-[:R]->(m), (:T)-[:R]->(m)", nil)
				require.NoError(t, err)
				if mode == "explicit transaction" {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				for _, shape := range shapes {
					for _, p := range predicates {
						where := strings.ReplaceAll(p.where, "X", shape.x)
						stmt := fmt.Sprintf(shape.stmt, where)
						res, err := exec.Execute(ctx, stmt, p.params)
						if !assert.NoError(t, err, "%s | %s", shape.name, stmt) {
							continue
						}
						got := int64(len(res.Rows))
						if strings.Contains(shape.stmt, " AS c") {
							require.Len(t, res.Rows, 1, stmt)
							got = res.Rows[0][0].(int64)
						}
						assert.Equal(t, p.want, got, "%s | %s | params %v", shape.name, stmt, p.params)
					}
				}
				if mode == "explicit transaction" {
					_, _ = exec.Execute(ctx, "ROLLBACK", nil)
				}
			})
		}
	}
}

// newThreeValuedServerStackExecutor builds the server's storage stack
// (Badger -> WAL -> Async -> Namespaced), whose auto-commit reads take the
// label-scan / compiled-predicate fast paths.
func newThreeValuedServerStackExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	dir := t.TempDir()
	badger, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	wal, err := storage.NewWAL(dir+"/wal", nil)
	require.NoError(t, err)
	async := storage.NewAsyncEngine(storage.NewWALEngine(badger, wal), nil)
	t.Cleanup(func() {
		_ = async.Close()
		_ = wal.Close()
		_ = badger.Close()
	})
	return NewStorageExecutor(storage.NewNamespacedEngine(async, "test"))
}
