package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RETURN items over a path variable are full expressions on every route that
// binds one: shortestPath, allShortestPaths and CREATE p = ... (#574). Expected
// values are Neo4j 5's.
func TestPathReturnItemsAreFullExpressions(t *testing.T) {
	sp := "MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 3})) RETURN "
	asp := "MATCH p = allShortestPaths((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 3})) RETURN "
	cp := "CREATE p = (:ZC {id: 7})-[:ZS]->(:ZC {id: 8})-[:ZS]->(:ZC {id: 9}) RETURN "
	cases := []struct {
		q    string
		want []interface{}
	}{
		{sp + "length(p) AS l", []interface{}{int64(2)}},
		{sp + "length(p) + 1 AS l", []interface{}{int64(3)}},
		{sp + "length(p) * 2 AS l", []interface{}{int64(4)}},
		{sp + "size(nodes(p)) AS n", []interface{}{int64(3)}},
		{sp + "size(relationships(p)) AS r", []interface{}{int64(2)}},
		{sp + "nodes(p)[0].id AS first", []interface{}{int64(1)}},
		{sp + "[x IN nodes(p) | x.id] AS ids", []interface{}{[]interface{}{int64(1), int64(2), int64(3)}}},
		{sp + "[r IN relationships(p) | type(r)] AS ts", []interface{}{[]interface{}{"ZS", "ZS"}}},
		{sp + "a.id + c.id AS s", []interface{}{int64(4)}},
		{sp + "'path' AS k, length(p) AS l", []interface{}{"path", int64(2)}},
		{sp + "length(p) + a.id AS s", []interface{}{int64(3)}},
		{asp + "length(p) + 1 AS l", []interface{}{int64(3)}},
		{asp + "size(nodes(p)) AS n", []interface{}{int64(3)}},
		{cp + "length(p) AS l", []interface{}{int64(2)}},
		{cp + "length(p) + 1 AS l", []interface{}{int64(3)}},
		{cp + "size(nodes(p)) AS n", []interface{}{int64(3)}},
		{cp + "nodes(p)[0].id AS first", []interface{}{int64(7)}},
		{cp + "[x IN nodes(p) | x.id] AS ids", []interface{}{[]interface{}{int64(7), int64(8), int64(9)}}},
		{"CREATE p = (a:ZC {id: 7})-[:ZS]->(:ZC {id: 8}) RETURN length(p) + a.id AS s", []interface{}{int64(8)}},
	}
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newPathReturnServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			t.Run(stack+"/"+mode, func(t *testing.T) {
				exec := build(t)
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (:ZSP {id: 1})-[:ZS]->(:ZSP {id: 2})-[:ZS]->(:ZSP {id: 3})", nil)
				require.NoError(t, err)
				if mode == "explicit transaction" {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				for _, tc := range cases {
					res, err := exec.Execute(ctx, tc.q, nil)
					if !assert.NoError(t, err, tc.q) {
						continue
					}
					if assert.Len(t, res.Rows, 1, tc.q) {
						assert.Equal(t, tc.want, res.Rows[0], tc.q)
					}
				}
				if mode == "explicit transaction" {
					_, _ = exec.Execute(ctx, "ROLLBACK", nil)
				}
			})
		}
	}
}

// newPathReturnServerStackExecutor builds the server's storage stack
// (Badger -> WAL -> Async -> Namespaced).
func newPathReturnServerStackExecutor(t *testing.T) *StorageExecutor {
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
