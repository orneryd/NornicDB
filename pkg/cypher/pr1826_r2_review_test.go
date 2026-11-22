package cypher

// Review lane R2 of PR #1826 (projected reads on by default). Test-only file:
// each test states the result a whole-node read gives, so it runs unchanged on
// the PR head and on its base.

import (
	"context"
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
				properties := pr1826R2PathNodeProperties(result.Rows[0][0])
				t.Logf("R2PATH route=%s type=%T node properties=%v", route, result.Rows[0][0], properties)
				if assert.Len(t, properties, 1, "one node in the returned path") {
					assert.Equal(t, map[string]any{"id": int64(1), "name": "alice", "secret": "s3cret"}, properties[0])
				}
			})
			t.Run("control relationship path", func(t *testing.T) {
				_, err := exec.Execute(ctx, `MATCH (a:R2Path {id: 1}), (b:R2Path {id: 2}) CREATE (a)-[:R2REL]->(b)`, nil)
				require.NoError(t, err)
				result, err := exec.Execute(ctx, `MATCH p = (n:R2Path {id: 1})-[:R2REL]->(m:R2Path) RETURN nodes(p)[0].name AS first, nodes(p)[1].secret AS second`, nil)
				if assert.NoError(t, err) {
					t.Logf("R2ROWS route=%s case=%q rows=%v", route, "control relationship path", pr1826R2Rows(result))
					assert.Equal(t, [][]any{{"alice", "other"}}, pr1826R2Rows(result))
				}
			})
		})
	}
}

func pr1826R2PathNodeProperties(path any) []map[string]any {
	value, ok := path.(map[string]interface{})
	if !ok {
		return nil
	}
	var out []map[string]any
	switch nodes := value["nodes"].(type) {
	case []*storage.Node:
		for _, node := range nodes {
			out = append(out, map[string]any(node.Properties))
		}
	case []interface{}:
		for _, element := range nodes {
			switch node := element.(type) {
			case *storage.Node:
				out = append(out, map[string]any(node.Properties))
			case map[string]interface{}:
				if properties, ok := node["properties"].(map[string]interface{}); ok {
					out = append(out, properties)
				} else {
					out = append(out, node)
				}
			}
		}
	}
	return out
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
// statement planned after the replacement uses the new declaration
// (pkg/cypher/fn/registry.go, Register and InputImageOf). tail() is a registered
// built-in with the structural declaration; the replacement declares that it
// reads its arguments' values, and returns the property names of the first
// element. The built-in is restored when the test ends.
func TestPR1826R2PropertyDemandFollowsReplacedRegistration(t *testing.T) {
	exec := pr1826R2Executor(t, "r2reg")
	ctx := t.Context()
	_, err := exec.Execute(ctx, `CREATE (:R2Reg {key: 'one', secret: 'value'})`, nil)
	require.NoError(t, err)
	const query = `MATCH (n:R2Reg) RETURN size(tail([n, n])) AS v`
	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(1)}}, pr1826R2Rows(first), "the built-in tail of a two-element list has one element")

	t.Cleanup(func() {
		cypherfn.Register("tail", fnTail, cypherfn.CanonicalReads, cypherfn.StructuralInputImage)
	})
	calls := 0
	cypherfn.Register("tail", func(_ context.Context, scope cypherfn.Context, args []string) (interface{}, error) {
		calls++
		value, err := scope.Eval(args[0])
		if err != nil {
			return nil, err
		}
		list, _ := value.([]interface{})
		if len(list) == 0 {
			return []interface{}{}, nil
		}
		node, _ := list[0].(*storage.Node)
		if node == nil {
			return []interface{}{}, nil
		}
		names := make([]interface{}, 0, len(node.Properties))
		for name := range node.Properties {
			names = append(names, name)
		}
		return names, nil
	}, cypherfn.OpaqueReads, cypherfn.ArgumentInputImage)

	// Another variable name is analysed for the first time after the
	// replacement: it shows the replacement is installed and receives the node.
	fresh, err := exec.Execute(ctx, `MATCH (m:R2Reg) RETURN size(tail([m, m])) AS v`, nil)
	require.NoError(t, err)
	t.Logf("R2REG fresh=%v calls=%d", pr1826R2Rows(fresh), calls)
	assert.Equal(t, [][]any{{int64(2)}}, pr1826R2Rows(fresh), "under a new variable name the replacement sees both properties of its node argument")

	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	t.Logf("R2REG repeated=%v calls=%d", pr1826R2Rows(second), calls)
	assert.Equal(t, [][]any{{int64(2)}}, pr1826R2Rows(second), "the statement first run before the replacement must give the replacement its whole node argument")
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

// Two executors over one store act as two sessions: one holds an explicit
// transaction, the other reads the same node through the unique index.
func TestPR1826R2TransactionsAndIndexedReads(t *testing.T) {
	engine := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(engine, "r2tx")
	writer := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	reader := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	ctx := t.Context()
	run := func(exec *StorageExecutor, query string) {
		t.Helper()
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	see := func(step string, exec *StorageExecutor, query string, want [][]any) {
		t.Helper()
		for attempt := 0; attempt < 3; attempt++ {
			result, err := exec.Execute(ctx, query, nil)
			if !assert.NoError(t, err, step) {
				return
			}
			assert.Equal(t, want, pr1826R2Rows(result), "%s (read %d)", step, attempt)
		}
	}
	const name = `MATCH (n:R2T {id: 1}) RETURN n.name AS name`
	const wide = `MATCH (n:R2T {id: 1}) RETURN n.name AS name, n.note AS note, n.extra AS extra`
	run(writer, `CREATE CONSTRAINT r2t_id IF NOT EXISTS FOR (n:R2T) REQUIRE n.id IS UNIQUE`)
	run(writer, `CREATE (:R2T {id: 1, name: 'v1', note: 'n1'})`)
	see("reader warm narrow", reader, name, [][]any{{"v1"}})
	see("reader warm wide", reader, wide, [][]any{{"v1", "n1", nil}})

	run(writer, "BEGIN")
	run(writer, `MATCH (n:R2T {id: 1}) SET n.name = 'tx', n.extra = 'e' REMOVE n.note`)
	see("writer reads its own narrow write", writer, name, [][]any{{"tx"}})
	see("writer reads its own wide write", writer, wide, [][]any{{"tx", nil, "e"}})
	see("reader does not see the open transaction, narrow", reader, name, [][]any{{"v1"}})
	see("reader does not see the open transaction, wide", reader, wide, [][]any{{"v1", "n1", nil}})
	run(writer, "ROLLBACK")
	see("writer after rollback", writer, wide, [][]any{{"v1", "n1", nil}})
	see("reader after rollback", reader, wide, [][]any{{"v1", "n1", nil}})

	run(writer, "BEGIN")
	run(writer, `MATCH (n:R2T {id: 1}) SET n.name = 'c'`)
	run(writer, `CREATE (:R2T {id: 2, name: 'new'})`)
	see("writer sees the node it created", writer, `MATCH (n:R2T {id: 2}) RETURN n.name AS name`, [][]any{{"new"}})
	see("reader does not see the created node", reader, `MATCH (n:R2T {id: 2}) RETURN n.name AS name`, [][]any{})
	run(writer, "COMMIT")
	see("reader after commit", reader, name, [][]any{{"c"}})
	see("reader sees the committed node", reader, `MATCH (n:R2T {id: 2}) RETURN n.name AS name`, [][]any{{"new"}})

	run(writer, "BEGIN")
	run(writer, `MATCH (n:R2T {id: 1}) DETACH DELETE n`)
	see("writer does not see the node it deleted", writer, name, [][]any{})
	see("reader still sees the node", reader, name, [][]any{{"c"}})
	run(writer, "ROLLBACK")
	see("writer after rolled back delete", writer, name, [][]any{{"c"}})
	see("reader after rolled back delete", reader, wide, [][]any{{"c", "n1", nil}})
}
