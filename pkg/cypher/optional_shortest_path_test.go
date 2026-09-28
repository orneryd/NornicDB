package cypher

// optional_shortest_path_test.go — regression tests for #581 (OPTIONAL MATCH
// with shortestPath) and #721 (shortestPath as a projected value).
//
// Pinned against Neo4j 5.26 behavior:
//
//	OPTIONAL MATCH p = shortestPath(...) RETURN length(p) AS l
//	  → one row {l: <length>} when a path exists, {l: null} when none.
//	Anchored form MATCH (a) OPTIONAL MATCH p = shortestPath((a)-...)
//	  → left outer join: one row per seed, nulls when the BFS finds no path.
//	length(shortestPath(...)) as a RETURN/WITH value
//	  → evaluates per row through the same BFS machinery.
//
// Every form is exercised in auto-commit and explicit-transaction modes, and
// run twice back-to-back to pin deterministic, idempotent behavior.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// setupOptionalShortestPathGraph builds a 1->2->3 chain under label ZSP, plus
// two disconnected nodes (id 4 and id 5) for null-path cases.
func setupOptionalShortestPathGraph(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	_, err := exec.Execute(ctx, "CREATE (:ZSP {id: 1})-[:ZS {w: 5}]->(:ZSP {id: 2})-[:ZS {w: 7}]->(:ZSP {id: 3}), (:ZSP {id: 4}), (:ZSP {id: 5})", nil)
	require.NoError(t, err)
}

func newOptionalShortestPathExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "opt-sp")
	return NewStorageExecutor(store)
}

// rowValues flattens rows into comparable [][]interface{} and asserts the
// expected column names at the same time.
func requireRowsEqual(t *testing.T, result *ExecuteResult, columns []string, rows [][]interface{}) {
	t.Helper()
	require.Equal(t, columns, result.Columns, "column names")
	require.Equal(t, len(rows), len(result.Rows), "row count")
	for i, row := range rows {
		require.Equal(t, row, result.Rows[i], "row %d", i)
	}
}

func TestBug581OptionalMatchShortestPath(t *testing.T) {
	exec := newOptionalShortestPathExecutor(t)
	ctx := context.Background()
	setupOptionalShortestPathGraph(t, exec, ctx)

	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		run := func(t *testing.T, query string) *ExecuteResult {
			t.Helper()
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}()
			}
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, "query failed: %s", query)
			return result
		}

		t.Run(mode, func(t *testing.T) {
			t.Run("clause-only path exists", func(t *testing.T) {
				query := "OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 3})) RETURN length(p) AS l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{int64(2)}})
				}
			})

			t.Run("clause-only path with arithmetic", func(t *testing.T) {
				query := "OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 3})) RETURN length(p) + 1 AS l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{int64(3)}})
				}
			})

			t.Run("clause-only path IS NULL false", func(t *testing.T) {
				query := "OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 3})) RETURN p IS NULL AS missing, 1 AS one"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"missing", "one"}, [][]interface{}{{false, int64(1)}})
				}
			})

			t.Run("clause-only no path yields null", func(t *testing.T) {
				query := "OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 99})) RETURN length(p) AS l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{nil}})
				}
			})

			t.Run("clause-only no path IS NULL true", func(t *testing.T) {
				query := "OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 99})) RETURN p IS NULL AS missing, 7 AS seven"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"missing", "seven"}, [][]interface{}{{true, int64(7)}})
				}
			})

			t.Run("anchored path exists", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}) OPTIONAL MATCH p = shortestPath((a)-[:ZS*]->(c:ZSP {id: 3})) RETURN length(p) + 1 AS l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{int64(3)}})
				}
			})

			t.Run("anchored no path yields null", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}) OPTIONAL MATCH p = shortestPath((a)-[:ZS*]->(c:ZSP {id: 99})) RETURN length(p) AS l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{nil}})
				}
			})

			t.Run("anchored both ends bound", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}), (b:ZSP {id: 3}) OPTIONAL MATCH p = shortestPath((a)-[:ZS*]->(b)) RETURN length(p) AS l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{int64(2)}})
				}
			})

			t.Run("anchored seeds with mixed reachability", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}) OPTIONAL MATCH p = shortestPath((a)-[:ZS*]->(c:ZSP {id: 3})) RETURN a.id AS seed, length(p) AS l"
				result := run(t, query)
				require.Equal(t, []string{"seed", "l"}, result.Columns)
				require.Equal(t, 1, len(result.Rows))
				require.Equal(t, []interface{}{int64(1), int64(2)}, result.Rows[0])
			})
		})
	}
}

func TestBug721ShortestPathAsValue(t *testing.T) {
	exec := newOptionalShortestPathExecutor(t)
	ctx := context.Background()
	setupOptionalShortestPathGraph(t, exec, ctx)

	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		run := func(t *testing.T, query string) *ExecuteResult {
			t.Helper()
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}()
			}
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, "query failed: %s", query)
			return result
		}

		t.Run(mode, func(t *testing.T) {
			t.Run("length in RETURN", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}), (b:ZSP {id: 3}) RETURN length(shortestPath((a)-[:ZS*]->(b))) AS l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{int64(2)}})
				}
			})

			t.Run("length in WITH", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}), (b:ZSP {id: 3}) WITH a, b WITH length(shortestPath((a)-[:ZS*]->(b))) AS l RETURN l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{int64(2)}})
				}
			})

			t.Run("raw path value in RETURN", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}), (b:ZSP {id: 3}) RETURN shortestPath((a)-[:ZS*]->(b)) AS p"
				result := run(t, query)
				require.Equal(t, []string{"p"}, result.Columns)
				require.Equal(t, 1, len(result.Rows))
				path, ok := result.Rows[0][0].(map[string]interface{})
				require.True(t, ok, "expected path map, got %T", result.Rows[0][0])
				require.Equal(t, int64(2), path["length"])
			})

			t.Run("no path yields null", func(t *testing.T) {
				// A disconnected ZSP node (id 5): both endpoints exist so the
				// row survives, but the BFS finds no path → length is null.
				query := "MATCH (a:ZSP {id: 1}), (b:ZSP {id: 5}) RETURN length(shortestPath((a)-[:ZS*]->(b))) AS l"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"l"}, [][]interface{}{{nil}})
				}
			})

			t.Run("nodes of path", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}), (b:ZSP {id: 3}) RETURN size(nodes(shortestPath((a)-[:ZS*]->(b)))) AS n"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"n"}, [][]interface{}{{int64(3)}})
				}
			})

			t.Run("allShortestPaths as list value", func(t *testing.T) {
				query := "MATCH (a:ZSP {id: 1}), (b:ZSP {id: 3}) RETURN size(allShortestPaths((a)-[:ZS*]->(b))) AS n"
				for attempt := 0; attempt < 2; attempt++ {
					requireRowsEqual(t, run(t, query), []string{"n"}, [][]interface{}{{int64(1)}})
				}
			})
		})
	}
}

// TestBug581OptionalShortestPathKeepsErrors exercises the no-swallow rule:
// malformed traversal patterns inside OPTIONAL MATCH must raise, never come
// back as a fabricated {result: null} row.
func TestBug581OptionalShortestPathKeepsErrors(t *testing.T) {
	exec := newOptionalShortestPathExecutor(t)
	ctx := context.Background()
	setupOptionalShortestPathGraph(t, exec, ctx)

	_, err := exec.Execute(ctx, "OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-->(c:ZSP {id: 3})) RETURN length(p) AS l", nil)
	require.Error(t, err)
}
