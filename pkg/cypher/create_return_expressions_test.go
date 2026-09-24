package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAsyncStackTestExecutor builds the server's storage stack
// (Badger -> WAL -> Async -> Namespaced), so auto-commit node-only CREATE
// statements take the async node-batch fast path (tryAsyncCreateNodeBatch).
func newAsyncStackTestExecutor(t *testing.T) *StorageExecutor {
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

// Every RETURN item after a plain CREATE is evaluated, not only items that
// reference a created variable, on every CREATE route: executeCreate, the
// multi-CREATE executor and the auto-commit node-only async fast path (#551).
func TestCreateReturnEvaluatesEveryItem(t *testing.T) {
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"async stack": newAsyncStackTestExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			t.Run(stack+"/"+mode, func(t *testing.T) {
				exec := build(t)
				ctx := context.Background()
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				params := map[string]interface{}{"a": int64(7)}
				for _, tc := range []struct {
					q    string
					want [][]interface{}
				}{
					{"CREATE (:P {v: 1}) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
					{"CREATE (p:P {v: 1}) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
					{"CREATE (p:P {v: 1}) RETURN p.v AS v, 1 AS ok", [][]interface{}{{int64(1), int64(1)}}},
					{"CREATE (p:P {v: 1}) RETURN 'x' AS s, 2 + 3 AS n, $a AS a", [][]interface{}{{"x", int64(5), int64(7)}}},
					{"CREATE (p:P {v: 1})-[:R]->(:Q) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
					{"CREATE (p:P {v: 2}) RETURN p.v + 1 AS v1, toString(p.v) AS s", [][]interface{}{{int64(3), "2"}}},
					{"CREATE (ab:P {v: 3}), (a:P {v: 4}) RETURN ab.v AS x, a.v AS y", [][]interface{}{{int64(3), int64(4)}}},
					{"CREATE (a:P {v: 5}) CREATE (:Q) RETURN 1 AS ok, a.v AS v", [][]interface{}{{int64(1), int64(5)}}},
					{"CREATE (a:P {v: 6}) RETURN count(*) AS c, count(a) AS ca", [][]interface{}{{int64(1), int64(1)}}},
				} {
					res, err := exec.Execute(ctx, tc.q, params)
					require.NoError(t, err, tc.q)
					assert.Equal(t, tc.want, res.Rows, tc.q)
				}
			})
		}
	}
}
