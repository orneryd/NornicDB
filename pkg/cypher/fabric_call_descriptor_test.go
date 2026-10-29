package cypher

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// fabricCallCaseDeadline bounds one statement of fabricCallDescriptorRun.
const fabricCallCaseDeadline = 10 * time.Second

// fabricCallDescriptorRun runs query in autocommit and in an explicit
// composite transaction, and with --ref neo-ref also on Neo4j 5.26.30 with the
// USE selectors removed: these bodies read no constituent data.
func fabricCallDescriptorRun(t *testing.T, exec *StorageExecutor, driver neo4j.DriverWithContext, query string, params map[string]interface{}, want string) {
	t.Helper()
	ctx := context.Background()
	if driver != nil {
		reference := strings.NewReplacer("USE comp.one ", "", "USE comp.two ", "", "USE comp ", "").Replace(query)
		ref, err := neo4j.ExecuteQuery(ctx, driver, reference, params, neo4j.EagerResultTransformer)
		require.NoError(t, err, "Neo4j query: %s", reference)
		rows := make([][]interface{}, 0, len(ref.Records))
		for _, record := range ref.Records {
			rows = append(rows, record.Values)
		}
		encoded, err := json.Marshal(rows)
		require.NoError(t, err)
		require.JSONEq(t, want, string(encoded), "Neo4j reference")
	}
	for _, mode := range []string{"autocommit", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			runExec := exec
			completed := true
			if mode == "transaction" {
				runExec = fabricCallRowsCompositeExecutor(t, exec)
				_, err := runExec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() {
					if !completed {
						return // the statement still holds the transaction
					}
					_, err := runExec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}()
			}
			// A statement that never completes (a plan re-entering its
			// composite) fails its case within the deadline instead of
			// stalling the package.
			runCtx, cancel := context.WithTimeout(ctx, fabricCallCaseDeadline)
			defer cancel()
			type outcome struct {
				result *ExecuteResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				result, err := runExec.Execute(runCtx, query, params)
				done <- outcome{result, err}
			}()
			var out outcome
			select {
			case out = <-done:
			case <-time.After(fabricCallCaseDeadline):
				completed = false
				t.Fatalf("statement did not complete within %v: %s", fabricCallCaseDeadline, query)
			}
			result, err := out.result, out.err
			require.NoError(t, err)
			encoded, err := json.Marshal(append(make([][]interface{}, 0, len(result.Rows)), result.Rows...))
			require.NoError(t, err)
			require.JSONEq(t, want, string(encoded))
		})
	}
}

// OPTIONAL CALL keeps an input row whose body returns no rows, with null for
// the body's columns; a plain CALL drops it. Names spelled like the keywords
// are names, as in a statement Fabric doesn't plan.
func TestFabricOptionalCall(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	driver := fabricCallRowsReference(t)
	for _, tt := range []struct{ name, query, want string }{
		{"empty_body", "USE comp WITH 1 AS x OPTIONAL CALL { USE comp.one UNWIND [] AS n RETURN n } RETURN x, n", "[[1,null]]"},
		{"every_row_empty", "USE comp UNWIND [1,2] AS x OPTIONAL CALL { WITH x USE comp.one UNWIND [] AS y RETURN y } RETURN x, y ORDER BY x", "[[1,null],[2,null]]"},
		{"partial", "USE comp UNWIND [1,2] AS x OPTIONAL CALL { WITH x USE comp.one WITH x WHERE x = 2 RETURN x * 10 AS y } RETURN x, y ORDER BY x", "[[1,null],[2,20]]"},
		{"several_rows", "USE comp UNWIND [1,2] AS x OPTIONAL CALL { WITH x USE comp.one WITH x WHERE x = 2 UNWIND [1,2] AS z RETURN z } RETURN x, z ORDER BY x, z", "[[1,null],[2,1],[2,2]]"},
		{"plain_call_control", "USE comp UNWIND [1,2] AS x CALL { WITH x USE comp.one WITH x WHERE x = 2 RETURN x * 10 AS y } RETURN x, y ORDER BY x", "[[2,20]]"},
		{"union_body", "USE comp WITH 1 AS x OPTIONAL CALL { USE comp.one UNWIND [] AS k RETURN k UNION ALL USE comp.two UNWIND [] AS k RETURN k } RETURN x, k", "[[1,null]]"},
		{"aggregate_body", "USE comp WITH 1 AS x OPTIONAL CALL { USE comp.one UNWIND [] AS k RETURN count(k) AS c } RETURN x, c", "[[1,0]]"},
		{"commented", "USE comp WITH 1 AS x OPTIONAL /* call */ CALL { USE comp.one UNWIND [] AS n RETURN n } RETURN x, n", "[[1,null]]"},
		{"variable_named_optional", "USE comp WITH 1 AS optional CALL { USE comp.one RETURN 2 AS k } RETURN optional, k", "[[1,2]]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fabricCallDescriptorRun(t, exec, driver, tt.query, nil, tt.want)
		})
	}
}

// A CALL body without RETURN is a unit subquery: it keeps every input row
// and runs its updates once per row.
func TestFabricUnitCallKeepsRows(t *testing.T) {
	ctx := context.Background()
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
	store, err := mgr.GetStorage("one")
	require.NoError(t, err)
	one := NewStorageExecutor(store)
	count := func(t *testing.T) int64 {
		t.Helper()
		result, err := one.Execute(ctx, "MATCH (n:FabricUnitCall) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return result.Rows[0][0].(int64)
	}
	for _, tt := range []struct{ name, query, want string }{
		{"create", "USE comp UNWIND [1,2] AS x CALL { WITH x USE comp.one CREATE (:FabricUnitCall {x: x}) } RETURN x ORDER BY x", "[[1],[2]]"},
		{"create_several", "USE comp UNWIND [1,2] AS x CALL { WITH x USE comp.one UNWIND [1,2] AS y CREATE (:FabricUnitCall {x: x, y: y}) } RETURN x ORDER BY x", "[[1],[2]]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, explicit := range []bool{false, true} {
				before := count(t)
				runExec := exec
				if explicit {
					runExec = fabricCallRowsCompositeExecutor(t, exec)
					_, err := runExec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, err := runExec.Execute(ctx, tt.query, nil)
				require.NoError(t, err)
				encoded, err := json.Marshal(result.Rows)
				require.NoError(t, err)
				require.JSONEq(t, tt.want, string(encoded), "explicit=%v", explicit)
				if explicit {
					_, err := runExec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				created := int64(2)
				if tt.name == "create_several" {
					created = 4
				}
				require.Equal(t, before+created, count(t), "explicit=%v", explicit)
			}
		})
	}
}

// Splitting a CALL body into UNION branches must read a name spelled UNION
// as the name: parameters, variables, keys, properties and labels.
func TestFabricCallBodyUnionNames(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	driver := fabricCallRowsReference(t)
	params := map[string]interface{}{"union": int64(7)}
	for _, tt := range []struct{ name, query, want string }{
		{"parameter", "USE comp CALL { USE comp.one RETURN $union AS k } RETURN k", "[[7]]"},
		{"parameter_branches", "USE comp CALL { USE comp.one RETURN $union AS k UNION ALL USE comp.two RETURN $union + 1 AS k } RETURN k ORDER BY k", "[[7],[8]]"},
		{"variable", "USE comp CALL { USE comp.one WITH 7 AS union RETURN union AS k } RETURN k", "[[7]]"},
		{"variable_branches", "USE comp CALL { USE comp.one WITH 7 AS union RETURN union AS k UNION ALL USE comp.two WITH 8 AS union RETURN union AS k } RETURN k ORDER BY k", "[[7],[8]]"},
		{"distinct_variable", "USE comp CALL { USE comp.one WITH 7 AS union WITH DISTINCT union RETURN union AS k } RETURN k", "[[7]]"},
		{"map_key", "USE comp CALL { USE comp.one WITH {union: 7} AS m RETURN m.union AS k } RETURN k", "[[7]]"},
		{"top_level_parameter", "USE comp.one RETURN $union AS k UNION ALL USE comp.two RETURN $union + 1 AS k", "[[7],[8]]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fabricCallDescriptorRun(t, exec, driver, tt.query, params, tt.want)
		})
	}
}

// A FOREACH update is a write: a second constituent's write in the same
// transaction is rejected, also between UNION branches.
func TestFabricForeachWriteOccupiesWriteShard(t *testing.T) {
	ctx := context.Background()
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	const second = "Writing to more than one database per transaction is not allowed"
	for _, action := range []string{"CREATE (:FabricForeachWrite)", "MERGE (:FabricForeachWrite)", "SET n.v = 1", "REMOVE n.v", "DELETE n"} {
		body := "FOREACH (x IN [1] | " + action + ")"
		if !strings.HasPrefix(action, "CREATE") && !strings.HasPrefix(action, "MERGE") {
			body = "MATCH (n:FabricForeachTarget {action: $action}) FOREACH (x IN [1] | " + action + ")"
		}
		t.Run(action, func(t *testing.T) {
			params := map[string]interface{}{"action": action}
			if strings.HasPrefix(action, "SET") || strings.HasPrefix(action, "REMOVE") || strings.HasPrefix(action, "DELETE") {
				// Exercise a real mutation. A zero-row UPDATE correctly leaves
				// the write slot available and is covered by the atomicity AC.
				_, err := exec.Execute(ctx, "USE comp.one CREATE (:FabricForeachTarget {action:$action,v:0})", params)
				require.NoError(t, err)
			}
			runExec := fabricCallRowsCompositeExecutor(t, exec)
			_, err := runExec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer func() { _, _ = runExec.Execute(ctx, "ROLLBACK", nil) }()
			_, err = runExec.Execute(ctx, "USE comp.one "+body, params)
			require.NoError(t, err)
			_, err = runExec.Execute(ctx, "USE comp.two CREATE (:FabricForeachWrite)", nil)
			require.ErrorContains(t, err, second)
		})
	}
	t.Run("union_branches", func(t *testing.T) {
		query := "USE comp.one FOREACH (x IN [1] | CREATE (:FabricForeachWrite)) RETURN 1 AS k UNION ALL USE comp.two FOREACH (x IN [1] | CREATE (:FabricForeachWrite)) RETURN 2 AS k"
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, second)
	})
}

// A Fabric UNION, top-level or a CALL body split into branches, has the
// semantics of the same UNION on one graph: columns match by name, other
// names are a SyntaxError, and DISTINCT compares rows by equivalence.
func TestFabricUnionMatchesSingleGraphUnion(t *testing.T) {
	ctx := context.Background()
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	driver := fabricCallRowsReference(t)
	for _, tt := range []struct{ name, query, want string }{
		{"order_top", "USE comp.one RETURN 1 AS a, 2 AS b UNION ALL USE comp.two RETURN 2 AS b, 1 AS a", "[[1,2],[1,2]]"},
		{"order_call", "USE comp CALL { USE comp.one RETURN 1 AS a, 2 AS b UNION ALL USE comp.two RETURN 2 AS b, 1 AS a } RETURN a, b", "[[1,2],[1,2]]"},
		{"distinct_number_top", "USE comp.one RETURN 1 AS v UNION USE comp.two RETURN 1.0 AS v", "[[1]]"},
		{"distinct_number_call", "USE comp CALL { USE comp.one RETURN 1 AS v UNION USE comp.two RETURN 1.0 AS v } RETURN v", "[[1]]"},
		{"distinct_list_call", "USE comp CALL { USE comp.one RETURN [1] AS v UNION USE comp.two RETURN [1.0] AS v } RETURN v", "[[[1]]]"},
		{"distinct_map_call", "USE comp CALL { USE comp.one RETURN {k: 1} AS v UNION USE comp.two RETURN {k: 1.0} AS v } RETURN v", "[[{\"k\":1}]]"},
		{"distinct_keeps_different", "USE comp CALL { USE comp.one RETURN 1 AS v UNION USE comp.two RETURN 2 AS v } RETURN v ORDER BY v", "[[1],[2]]"},
		{"after_outer_rows", "USE comp WITH 1 AS x CALL { USE comp.one RETURN 1 AS k UNION ALL USE comp.two RETURN 2 AS k } RETURN x, k ORDER BY k", "[[1,1],[1,2]]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fabricCallDescriptorRun(t, exec, driver, tt.query, nil, tt.want)
		})
	}
	for _, tt := range []struct{ name, query string }{
		{"mismatch_top", "USE comp.one RETURN 1 AS a UNION USE comp.two RETURN 2 AS b"},
		{"mismatch_call", "USE comp CALL { USE comp.one RETURN 1 AS a UNION USE comp.two RETURN 2 AS b } RETURN a"},
		{"mismatch_count_call", "USE comp CALL { USE comp.one RETURN 1 AS a UNION ALL USE comp.two RETURN 1 AS a, 2 AS b } RETURN a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if driver != nil {
				reference := strings.NewReplacer("USE comp.one ", "", "USE comp.two ", "", "USE comp ", "").Replace(tt.query)
				_, err := neo4j.ExecuteQuery(ctx, driver, reference, nil, neo4j.EagerResultTransformer)
				require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError", "Neo4j query: %s", reference)
			}
			for _, explicit := range []bool{false, true} {
				runExec := exec
				if explicit {
					runExec = fabricCallRowsCompositeExecutor(t, exec)
					_, err := runExec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err := runExec.Execute(ctx, tt.query, nil)
				if explicit {
					_, _ = runExec.Execute(ctx, "ROLLBACK", nil)
				}
				require.Error(t, err, "explicit=%v", explicit)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "explicit=%v: %v", explicit, err)
			}
		})
	}
}

// A unit CALL keeps exactly one unchanged input row however many rows its
// body produces internally, a nested unit or returning CALL included, and
// runs every body effect. OPTIONAL's null extension is the separate rule
// for a returning body without rows.
func TestFabricNestedUnitCall(t *testing.T) {
	for _, normalization := range []string{"on", "off"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			if normalization == "off" {
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
			}
			exec := newFabricCallRowsExecutor(t)
			exec.cache = nil
			driver := fabricCallRowsReference(t)
			for _, tt := range []struct{ name, query, want string }{
				{"unit_in_unit", "USE comp UNWIND [1,2] AS x CALL { WITH x UNWIND [1,2] AS y CALL { WITH x, y USE comp.one CREATE (:FabricNestedUnit {x: x, y: y}) } } RETURN x ORDER BY x", "[[1],[2]]"},
				{"unit_in_optional_unit", "USE comp UNWIND [1,2] AS x OPTIONAL CALL { WITH x UNWIND [1,2] AS y CALL { WITH x, y USE comp.one CREATE (:FabricNestedUnit {x: x, y: y}) } } RETURN x ORDER BY x", "[[1],[2]]"},
				{"returning_in_unit", "USE comp UNWIND [1,2] AS x CALL { WITH x CALL { WITH x USE comp.one UNWIND [1,2] AS z RETURN z } CALL { WITH x, z USE comp.one CREATE (:FabricNestedUnit {x: x, z: z}) } } RETURN x ORDER BY x", "[[1],[2]]"},
				{"unit_in_returning", "USE comp UNWIND [1,2] AS x CALL { WITH x UNWIND [1,2] AS y CALL { WITH x, y USE comp.one CREATE (:FabricNestedUnit {x: x, y: y}) } RETURN y } RETURN x, y ORDER BY x, y", "[[1,1],[1,2],[2,1],[2,2]]"},
				{"unit_in_empty_optional", "USE comp UNWIND [1,2] AS x OPTIONAL CALL { WITH x UNWIND [] AS y CALL { WITH x, y USE comp.one CREATE (:FabricNestedUnit {x: x, y: y}) } RETURN y } RETURN x, y ORDER BY x", "[[1,null],[2,null]]"},
				{"unit_then_aggregate", "USE comp UNWIND [1,2] AS x CALL { WITH x UNWIND [1,2] AS y CALL { WITH x, y USE comp.one CREATE (:FabricNestedUnit {x: x, y: y}) } } RETURN count(*) AS rows", "[[2]]"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					fabricCallDescriptorRun(t, exec, driver, tt.query, nil, tt.want)
				})
			}
			// Every body effect runs: four nodes per committed statement.
			mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
			store, err := mgr.GetStorage("one")
			require.NoError(t, err)
			one := NewStorageExecutor(store)
			count := func() int64 {
				result, err := one.Execute(context.Background(), "MATCH (n:FabricNestedUnitCount) RETURN count(n) AS c", nil)
				require.NoError(t, err)
				return result.Rows[0][0].(int64)
			}
			before := count()
			_, err = exec.Execute(context.Background(), "USE comp UNWIND [1,2] AS x CALL { WITH x UNWIND [1,2] AS y CALL { WITH x, y USE comp.one CREATE (:FabricNestedUnitCount {x: x, y: y}) } } RETURN x", nil)
			require.NoError(t, err)
			require.Equal(t, before+4, count())
		})
	}
}

// A composite UNION that mixes UNION and UNION ALL is rejected before any
// branch runs, with the single-graph path's error, at the top level and in
// a CALL body; a branch's write never happens.
func TestFabricMixedUnionRejected(t *testing.T) {
	ctx := context.Background()
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	driver := fabricCallRowsReference(t)
	_, native := exec.Execute(ctx, "RETURN 1 AS x UNION ALL RETURN 2 AS x UNION RETURN 3 AS x", nil)
	require.Error(t, native)
	nativeCode, _ := nornicerrors.Neo4jStatus(native)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", nativeCode)
	mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
	store, err := mgr.GetStorage("one")
	require.NoError(t, err)
	one := NewStorageExecutor(store)
	written := func() int64 {
		result, err := one.Execute(ctx, "MATCH (n:FabricMixedUnion) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return result.Rows[0][0].(int64)
	}
	for _, tt := range []struct{ name, query string }{
		{"top_level", "USE comp.one RETURN 1 AS x UNION ALL USE comp.two RETURN 2 AS x UNION USE comp.one RETURN 3 AS x"},
		{"top_level_write", "USE comp.one CREATE (:FabricMixedUnion) RETURN 1 AS x UNION ALL USE comp.two RETURN 2 AS x UNION USE comp.two RETURN 3 AS x"},
		{"call_body", "USE comp CALL { USE comp.one RETURN 1 AS x UNION ALL USE comp.two RETURN 2 AS x UNION USE comp.one RETURN 3 AS x } RETURN x"},
		{"call_body_write", "USE comp CALL { USE comp.one CREATE (:FabricMixedUnion) RETURN 1 AS x UNION ALL USE comp.two RETURN 2 AS x UNION USE comp.two RETURN 3 AS x } RETURN x"},
		{"distinct_first", "USE comp CALL { USE comp.one RETURN 1 AS x UNION USE comp.two RETURN 2 AS x UNION ALL USE comp.one RETURN 3 AS x } RETURN x"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if driver != nil {
				reference := strings.NewReplacer("USE comp.one ", "", "USE comp.two ", "", "USE comp ", "").Replace(tt.query)
				_, err := neo4j.ExecuteQuery(ctx, driver, reference, nil, neo4j.EagerResultTransformer)
				require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError", "Neo4j query: %s", reference)
			}
			for _, explicit := range []bool{false, true} {
				before := written()
				runExec := exec
				if explicit {
					runExec = fabricCallRowsCompositeExecutor(t, exec)
					_, err := runExec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err := runExec.Execute(ctx, tt.query, nil)
				if explicit {
					_, _ = runExec.Execute(ctx, "ROLLBACK", nil)
				}
				require.Error(t, err, "explicit=%v", explicit)
				require.Equal(t, native.Error(), err.Error(), "explicit=%v", explicit)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, nativeCode, code, "explicit=%v", explicit)
				require.Equal(t, before, written(), "explicit=%v: a branch ran", explicit)
			}
		})
	}
}
