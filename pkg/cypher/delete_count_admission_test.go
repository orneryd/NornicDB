package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These are exactly NOR-80's three recovered probes, whose Neo4j 5.26.30
// reference verdicts are accepted. Sibling ticket probes remain out of scope.
func TestDeleteCountSubqueryAdmission(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, query := range []string{
			"UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { MATCH (m) } > 0 RETURN label",
			"UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { CALL { MATCH (m) RETURN m } RETURN m } > 0 RETURN label",
			"UNWIND [] AS s MATCH (n) DELETE CASE WHEN COUNT { MATCH (m) } > 0 THEN n END",
		} {
			for _, mode := range []string{"auto", "tx"} {
				for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
					t.Run(mode+"/"+prefix+query, func(t *testing.T) {
						exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_count_admission"))
						ctx := context.Background()
						if mode == "tx" {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
						}
						result, err := exec.Execute(ctx, prefix+query, nil)
						require.NoError(t, err)
						require.NotNil(t, result)
						if prefix == "" {
							require.Empty(t, result.Rows)
						}
					})
				}
			}
		}
	})
}

func TestDeleteCountSubqueryCaseTargets(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, target string
			deleted      int
		}{
			{"selected node", "CASE WHEN COUNT { MATCH (m:Item) } > 0 THEN n END", 1},
			{"unchosen node", "CASE WHEN COUNT { MATCH (m:Item) } < 0 THEN n END", 0},
			{"selected null", "CASE WHEN COUNT { MATCH (m:Item) } > 0 THEN null ELSE n END", 0},
			{"all null", "CASE WHEN COUNT { MATCH (m:Item) } > 0 THEN null ELSE null END", 0},
			{"selected else", "CASE WHEN COUNT { MATCH (m:Item) } < 0 THEN null ELSE n END", 1},
			{"correlated count", "CASE WHEN COUNT { MATCH (m:Item) WHERE m.id = n.id } > 0 THEN n END", 1},
			{"count call", "CASE WHEN COUNT { CALL { MATCH (m:Item) RETURN m } RETURN m } > 0 THEN n END", 1},
		} {
			for _, mode := range []string{"auto", "tx"} {
				t.Run(tc.name+"/"+mode, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_count_targets"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:Item {id: 1}), (:Item {id: 2})", nil)
					require.NoError(t, err)
					if mode == "tx" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					result, err := exec.Execute(ctx, "MATCH (n:Item {id: 1}) DELETE "+tc.target, nil)
					require.NoError(t, err)
					require.Equal(t, tc.deleted, result.Stats.NodesDeleted)
					if mode == "tx" {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
					remaining, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n.id ORDER BY n.id", nil)
					require.NoError(t, err)
					want := [][]interface{}{{int64(1)}, {int64(2)}}
					if tc.deleted == 1 {
						want = [][]interface{}{{int64(2)}}
					}
					require.Equal(t, want, remaining.Rows)
				})
			}
		}
	})
}

func TestDeleteCountSubqueryUndefinedReferences(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct{ target, missing string }{
			{"missing", "missing"},
			{"CASE WHEN COUNT { MATCH (m:Item) } > 0 THEN missing END", "missing"},
			{"CASE WHEN COUNT { MATCH (m:Item) WHERE m.id = missing } > 0 THEN n END", "missing"},
			{"CASE WHEN COUNT { MATCH (m:Item) } > 0 THEN m END", "m"},
			{"n.items[missing]", "missing"},
		} {
			for _, mode := range []string{"auto", "tx"} {
				for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
					t.Run(mode+"/"+prefix+tc.target, func(t *testing.T) {
						exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_count_missing"))
						ctx := context.Background()
						_, err := exec.Execute(ctx, "CREATE (:Item {id: 1}), (:Item {id: 2})", nil)
						require.NoError(t, err)
						if mode == "tx" {
							_, err = exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						requireStatus(t, exec, prefix+"MATCH (n:Item) DELETE "+tc.target, nil,
							"Neo.ClientError.Statement.SyntaxError", "Variable `"+tc.missing+"` not defined")
						if mode == "tx" {
							_, err = exec.Execute(ctx, "ROLLBACK", nil)
							require.NoError(t, err)
						}
						remaining, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n.id ORDER BY n.id", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, remaining.Rows, "rejection must preserve every node")
					})
				}
			}
		}
	})
}

// COUNT bodies execute only for reached DELETE rows, and their original errors
// must stop the collect phase before any target can be mutated.
func TestDeleteCountSubqueryRuntimeErrorBoundary(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, empty := range []bool{false, true} {
			for _, mode := range []string{"auto", "tx"} {
				name := "reached error/" + mode
				if empty {
					name = "empty all-null/" + mode
				}
				t.Run(name, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_count_error"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:Item {id: 1}), (:Item {id: 2})", nil)
					require.NoError(t, err)
					if mode == "tx" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					query := "MATCH (n:Item) DELETE CASE WHEN COUNT { MATCH (m:Item) WHERE m.id / 0 > 0 } > 0 THEN n END"
					if empty {
						query = "MATCH (n:Absent) DELETE CASE WHEN COUNT { MATCH (m:Item) WHERE m.id / 0 > 0 } > 0 THEN null ELSE null END"
						result, err := exec.Execute(ctx, query, nil)
						require.NoError(t, err, "zero input rows must not execute the COUNT body")
						require.Zero(t, result.Stats.NodesDeleted)
					} else {
						requireStatus(t, exec, query, nil, "Neo.ClientError.Statement.ArithmeticError", "/ by zero")
					}
					if mode == "tx" {
						_, err = exec.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}
					result, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n.id ORDER BY n.id", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
				})
			}
		}
	})
}
