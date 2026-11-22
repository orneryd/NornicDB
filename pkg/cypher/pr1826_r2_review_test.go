package cypher

// Review lane R2 of PR #1826 (projected reads on by default). Test-only file:
// each test states the result a whole-node read gives, so it runs unchanged on
// the PR head and on its base.

import (
	"context"
	"fmt"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pr1826R2Executor(t *testing.T, namespace string) *StorageExecutor {
	t.Helper()
	engine := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(engine, namespace)
	return NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
}

// A node bound by a named single-node path is read through the path as well as
// through its variable. The property demand of the MATCH is computed for the
// node variable alone.
func TestPR1826R2NamedPathNodeKeepsItsProperties(t *testing.T) {
	for _, route := range []string{"scan", "indexed"} {
		t.Run(route, func(t *testing.T) {
			exec := pr1826R2Executor(t, "r2path"+route)
			ctx := t.Context()
			if route == "indexed" {
				_, err := exec.Execute(ctx, `CREATE CONSTRAINT r2path_id IF NOT EXISTS FOR (n:R2Path) REQUIRE n.id IS UNIQUE`, nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, `CREATE (:R2Path {id: 1, name: 'alice', secret: 's3cret'}), (:R2Path {id: 2, name: 'bob', secret: 'other'})`, nil)
			require.NoError(t, err)
			for _, test := range []struct {
				name, query string
				want        [][]any
			}{
				{"control variable read", `MATCH (n:R2Path {id: 1}) RETURN n.name AS name`, [][]any{{"alice"}}},
				{"control path with whole node", `MATCH p = (n:R2Path {id: 1}) RETURN nodes(p)[0].name AS name, n AS n`, nil},
				{"path element property", `MATCH p = (n:R2Path {id: 1}) RETURN nodes(p)[0].name AS name`, [][]any{{"alice"}}},
				{"path element property count", `MATCH p = (n:R2Path {id: 1}) RETURN size(keys(nodes(p)[0])) AS properties`, [][]any{{int64(3)}}},
				{"path element other property", `MATCH p = (n:R2Path {id: 1}) RETURN [x IN nodes(p) | x.secret] AS secrets`, [][]any{{[]any{"s3cret"}}}},
				{"path and variable property", `MATCH p = (n:R2Path {id: 1}) RETURN n.id AS id, nodes(p)[0].name AS name`, [][]any{{int64(1), "alice"}}},
				{"where reads path and property", `MATCH p = (n:R2Path {id: 1}) WHERE length(p) = 0 AND n.name = 'alice' RETURN n.id AS id`, [][]any{{int64(1)}}},
				{"where reads path and property, no inline", `MATCH p = (n:R2Path) WHERE length(p) = 0 AND n.name = 'alice' RETURN n.id AS id`, [][]any{{int64(1)}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, test.query, nil)
					if !assert.NoError(t, err) {
						return
					}
					t.Logf("R2ROWS route=%s case=%q rows=%v", route, test.name, pr1826R2Rows(result))
					if test.want == nil {
						if assert.Len(t, result.Rows, 1) {
							assert.Equal(t, "alice", result.Rows[0][0])
						}
						return
					}
					assert.Equal(t, test.want, pr1826R2Rows(result))
				})
			}
			t.Run("returned path", func(t *testing.T) {
				result, err := exec.Execute(ctx, `MATCH p = (n:R2Path {id: 1}) RETURN p`, nil)
				if !assert.NoError(t, err) || !assert.Len(t, result.Rows, 1) {
					return
				}
				rendered := fmt.Sprintf("%v", result.Rows[0][0])
				t.Logf("R2PATH route=%s path=%s", route, rendered)
				assert.Contains(t, rendered, "alice", "the returned path's node must carry name")
				assert.Contains(t, rendered, "s3cret", "the returned path's node must carry secret")
			})
		})
	}
}

func pr1826R2Rows(result *ExecuteResult) [][]any {
	rows := make([][]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		out := make([]any, len(row))
		for i, value := range row {
			if list, ok := value.([]interface{}); ok {
				value = append([]any{}, list...)
			}
			out[i] = value
		}
		rows = append(rows, out)
	}
	return rows
}

// The registry documents that a registration can be replaced and that a
// statement planned after the replacement uses the new declaration. The name
// here is the test's own, so no built-in is replaced.
func TestPR1826R2PropertyDemandFollowsReplacedRegistration(t *testing.T) {
	exec := pr1826R2Executor(t, "r2reg")
	ctx := t.Context()
	_, err := exec.Execute(ctx, `CREATE (:R2Reg {key: 'one', secret: 'value'})`, nil)
	require.NoError(t, err)
	argument := func(scope cypherfn.Context, args []string) (*storage.Node, error) {
		value, err := scope.Eval(args[0])
		if err != nil {
			return nil, err
		}
		node, _ := value.(*storage.Node)
		return node, nil
	}
	cypherfn.Register("r2probe", func(_ context.Context, scope cypherfn.Context, args []string) (interface{}, error) {
		node, err := argument(scope, args)
		if err != nil || node == nil {
			return nil, err
		}
		return int64(len(node.Labels)), nil
	}, cypherfn.OpaqueReads, cypherfn.HeaderInputImage)
	const query = `MATCH (n:R2Reg) RETURN r2probe(n) AS v`
	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(1)}}, pr1826R2Rows(first), "the header-only registration reads the label count")

	cypherfn.Register("r2probe", func(_ context.Context, scope cypherfn.Context, args []string) (interface{}, error) {
		node, err := argument(scope, args)
		if err != nil || node == nil {
			return nil, err
		}
		return node.Properties["secret"], nil
	}, cypherfn.OpaqueReads, cypherfn.ArgumentInputImage)

	// Another variable name is analysed for the first time after the
	// replacement: it shows the replacement is installed and reads its argument.
	fresh, err := exec.Execute(ctx, `MATCH (m:R2Reg) RETURN r2probe(m) AS v`, nil)
	require.NoError(t, err)
	t.Logf("R2REG fresh=%v", pr1826R2Rows(fresh))
	assert.Equal(t, [][]any{{"value"}}, pr1826R2Rows(fresh), "the replacement reads its node argument under a new variable name")

	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	t.Logf("R2REG repeated=%v", pr1826R2Rows(second))
	assert.Equal(t, [][]any{{"value"}}, pr1826R2Rows(second), "the statement run before the replacement must use the replacement's declaration afterwards")
}

// Narrow reads, wider reads and writes of the same node through one executor:
// every read must show the last committed write.
func TestPR1826R2IndexedReadsFollowWrites(t *testing.T) {
	exec := pr1826R2Executor(t, "r2writes")
	ctx := t.Context()
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT r2w_id IF NOT EXISTS FOR (n:R2W) REQUIRE n.id IS UNIQUE`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:R2W {id: 1, name: 'v1', note: 'n1', tags: ['a', 'b']})`, nil)
	require.NoError(t, err)
	read := func(step, query string, want [][]any) {
		t.Helper()
		for attempt := 0; attempt < 3; attempt++ {
			result, err := exec.Execute(ctx, query, nil)
			if !assert.NoError(t, err, step) {
				return
			}
			assert.Equal(t, want, pr1826R2Rows(result), "%s (read %d)", step, attempt)
		}
	}
	write := func(query string) {
		t.Helper()
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	read("initial narrow", `MATCH (n:R2W {id: 1}) RETURN n.name AS name`, [][]any{{"v1"}})
	read("absent property", `MATCH (n:R2W {id: 1}) RETURN n.missing AS missing`, [][]any{{nil}})
	read("widened", `MATCH (n:R2W {id: 1}) RETURN n.name AS name, n.note AS note, size(n.tags) AS tags`, [][]any{{"v1", "n1", int64(2)}})
	write(`MATCH (n:R2W {id: 1}) SET n.name = 'v2', n.missing = 'now'`)
	read("after set", `MATCH (n:R2W {id: 1}) RETURN n.name AS name, n.missing AS missing`, [][]any{{"v2", "now"}})
	write(`MATCH (n:R2W {id: 1}) REMOVE n.note`)
	read("after remove", `MATCH (n:R2W {id: 1}) RETURN n.name AS name, n.note AS note`, [][]any{{"v2", nil}})
	write(`MATCH (n:R2W {id: 1}) SET n:Extra`)
	read("after label add", `MATCH (n:R2W {id: 1}) RETURN n.name AS name, size(labels(n)) AS labels`, [][]any{{"v2", int64(2)}})
	write(`MATCH (n:R2W {id: 1}) SET n += {name: 'v3'}`)
	read("after map set", `MATCH (n:R2W {id: 1}) WHERE n.name = 'v3' RETURN n.missing AS missing`, [][]any{{"now"}})
	write(`MATCH (n:R2W {id: 1}) DETACH DELETE n`)
	read("after delete", `MATCH (n:R2W {id: 1}) RETURN n.name AS name`, [][]any{})
	write(`CREATE (:R2W {id: 1, name: 'again'})`)
	read("after recreate", `MATCH (n:R2W {id: 1}) RETURN n.name AS name, n.missing AS missing`, [][]any{{"again", nil}})
}
