package cypher

// H2 variant of fcd53ab65 "Honor periodic iteration parameters and report
// failed batches" (units of pkg/cypher/call_apoc_periodic.go). The fix's own
// assertions (periodic_iterate_parameters_test.go: configured parameters reach
// the inner statements; a failed batch is counted and its message reported)
// applied to the sibling entry points of the same file: the alias
// apoc.periodic.rock_n_roll, apoc.periodic.commit with its params argument,
// apoc.periodic.commit with a failing statement, and iterate with a failing
// action of another error class and with a parameter-only iterate statement.

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestHuntH2PeriodicSiblings(t *testing.T) {
	cases, leads := 0, 0
	type check struct {
		name   string
		setup  []string
		call   string
		outer  map[string]interface{}
		stored string
		want   [][]interface{}
		// columns that must hold the given value in the call's single row
		columns map[string]interface{}
		// failure: the call's row must report a failure (any non-zero
		// failure count, or a non-empty error map) or the call must fail
		failure bool
	}
	checks := []check{
		{name: "rock_n_roll-configured-params",
			call:   "CALL apoc.periodic.rock_n_roll('UNWIND [1,2,3] AS i RETURN i', 'CREATE (:ProcessedItem {i: i, v: $v})', {batchSize: 1, params: {v: 7}}) YIELD failedBatches RETURN failedBatches",
			stored: "MATCH (n:ProcessedItem) RETURN n.v AS v ORDER BY n.i", want: [][]interface{}{{int64(7)}, {int64(7)}, {int64(7)}},
			columns: map[string]interface{}{"failedBatches": int64(0)}},
		{name: "rock_n_roll-params-reach-iterate",
			call:   "CALL apoc.periodic.rock_n_roll('UNWIND [1,2,3] AS i RETURN i + $offset AS i', 'CREATE (:ProcessedItem {i: i})', {batchSize: 2, params: {offset: 10}}) YIELD failedBatches RETURN failedBatches",
			stored: "MATCH (n:ProcessedItem) RETURN n.i AS v ORDER BY v", want: [][]interface{}{{int64(11)}, {int64(12)}, {int64(13)}},
			columns: map[string]interface{}{"failedBatches": int64(0)}},
		{name: "rock_n_roll-failed-batch-messages",
			setup: []string{"CREATE CONSTRAINT h2piu IF NOT EXISTS FOR (u:PIU) REQUIRE u.k IS UNIQUE", "UNWIND range(1,4) AS i CREATE (:PI {i:i})"},
			call:  "CALL apoc.periodic.rock_n_roll('MATCH (n:PI) RETURN n ORDER BY n.i', 'CREATE (:PIU {k: n.i % 2})', {batchSize:1}) YIELD failedBatches, errorMessages RETURN failedBatches, size(keys(errorMessages)) > 0 AS hasMessages",
			stored: "MATCH (u:PIU) RETURN count(u) AS c", want: [][]interface{}{{int64(2)}},
			columns: map[string]interface{}{"failedBatches": int64(2), "hasMessages": true}},
		{name: "commit-params-argument",
			setup:  []string{"UNWIND range(1,5) AS i CREATE (:Todo {i: i})"},
			call:   "CALL apoc.periodic.commit('MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT $limit SET n.done = $mark RETURN count(*)', {limit: 2, mark: 7}) YIELD updates RETURN updates",
			stored: "MATCH (n:Todo) RETURN n.done AS v ORDER BY n.i", want: [][]interface{}{{int64(7)}, {int64(7)}, {int64(7)}, {int64(7)}, {int64(7)}},
			columns: map[string]interface{}{"updates": int64(5)}},
		{name: "commit-params-outer-expression",
			setup:  []string{"UNWIND range(1,3) AS i CREATE (:Todo {i: i})"},
			outer:  map[string]interface{}{"m": int64(3)},
			call:   "CALL apoc.periodic.commit('MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT $limit SET n.done = $mark RETURN count(*)', {limit: 1, mark: $m + 1}) YIELD updates RETURN updates",
			stored: "MATCH (n:Todo) RETURN n.done AS v ORDER BY n.i", want: [][]interface{}{{int64(4)}, {int64(4)}, {int64(4)}},
			columns: map[string]interface{}{"updates": int64(3)}},
		{name: "commit-failing-statement-is-reported",
			setup:   []string{"UNWIND range(1,3) AS i CREATE (:Todo {i: i})"},
			call:    "CALL apoc.periodic.commit('MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT $limit SET n.done = 1 / (n.i - n.i) RETURN count(*)', {limit: 1}) YIELD updates, executions RETURN updates, executions",
			stored:  "MATCH (n:Todo) WHERE n.done IS NOT NULL RETURN count(n) AS c", want: [][]interface{}{{int64(0)}},
			failure: true},
		{name: "commit-constraint-failure-is-reported",
			setup:   []string{"CREATE CONSTRAINT h2cu IF NOT EXISTS FOR (u:CU) REQUIRE u.k IS UNIQUE", "CREATE (:CU {k: 1})", "UNWIND range(1,3) AS i CREATE (:Todo {i: i})"},
			call:    "CALL apoc.periodic.commit('MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT $limit SET n.done = true CREATE (:CU {k: 1}) RETURN count(*)', {limit: 1}) YIELD updates, executions RETURN updates, executions",
			stored:  "MATCH (n:Todo) WHERE n.done IS NOT NULL RETURN count(n) AS c", want: [][]interface{}{{int64(0)}},
			failure: true},
		{name: "iterate-type-error-batch-message",
			call:    "CALL apoc.periodic.iterate('UNWIND [1,2] AS i RETURN i', 'CREATE (:ProcessedItem {v: i / 0})', {batchSize:1}) YIELD failedBatches, errorMessages RETURN failedBatches, size(keys(errorMessages)) > 0 AS hasMessages",
			stored:  "MATCH (n:ProcessedItem) RETURN count(n) AS c", want: [][]interface{}{{int64(0)}},
			columns: map[string]interface{}{"failedBatches": int64(2), "hasMessages": true}},
		{name: "iterate-params-only-in-action-with-config-map-param",
			outer:  map[string]interface{}{"cfg": map[string]interface{}{"batchSize": int64(1), "params": map[string]interface{}{"v": int64(9)}}},
			call:   "CALL apoc.periodic.iterate('UNWIND [1,2] AS i RETURN i', 'CREATE (:ProcessedItem {i: i, v: $v})', $cfg) YIELD failedBatches RETURN failedBatches",
			stored: "MATCH (n:ProcessedItem) RETURN n.v AS v ORDER BY n.i", want: [][]interface{}{{int64(9)}, {int64(9)}},
			columns: map[string]interface{}{"failedBatches": int64(0)}},
		{name: "iterate-nested-map-param",
			call:   "CALL apoc.periodic.iterate('UNWIND $rows AS r RETURN r', 'CREATE (:ProcessedItem {i: r.i, v: r.v * $k})', {batchSize: 2, params: {rows: [{i:1, v:2}, {i:2, v:3}], k: 10}}) YIELD failedBatches RETURN failedBatches",
			stored: "MATCH (n:ProcessedItem) RETURN n.v AS v ORDER BY n.i", want: [][]interface{}{{int64(20)}, {int64(30)}},
			columns: map[string]interface{}{"failedBatches": int64(0)}},
	}
	forEachParser(t, func(t *testing.T) {
		for _, c := range checks {
			t.Run(c.name, func(t *testing.T) {
				cases++
				ctx := context.Background()
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "h2periodic"))
				for _, statement := range c.setup {
					if _, err := exec.Execute(ctx, statement, nil); err != nil {
						t.Fatalf("setup %q: %v", statement, err)
					}
				}
				lead := map[string]any{"harness": "periodic", "unit": "fcd53ab65", "case": c.name, "setup": c.setup, "call": c.call, "outer": fmt.Sprint(c.outer)}
				result, err := exec.Execute(ctx, c.call, c.outer)
				stored, storedErr := h2Exec(exec, ctx, c.stored, nil)
				if storedErr != nil {
					lead["kind"], lead["error"] = "stored-read-error", storedErr.Error()
					leads++
					h2Lead(lead)
					return
				}
				if stored == nil {
					stored = [][]interface{}{}
				}
				wrong := !reflect.DeepEqual(stored, c.want)
				row := map[string]interface{}{}
				if err == nil && len(result.Rows) == 1 {
					for i, column := range result.Columns {
						row[column] = result.Rows[0][i]
					}
				}
				if c.failure {
					reported := err != nil
					// apoc.periodic.commit's row has no failure column at the
					// pin: a failure is reported only by an error.
					if !reported {
						wrong = true
						lead["note"] = "statement failure not reported (no error, no failure column)"
					}
				} else {
					if err != nil {
						lead["kind"], lead["error"] = "error", err.Error()
						leads++
						h2Lead(lead)
						return
					}
					for column, want := range c.columns {
						if !reflect.DeepEqual(row[column], want) {
							wrong = true
						}
					}
				}
				if wrong {
					lead["kind"] = "wrong-result"
					lead["row"], lead["want_columns"] = fmt.Sprint(row), fmt.Sprint(c.columns)
					lead["call_error"] = errString(err)
					lead["stored_query"], lead["stored"], lead["want_stored"] = c.stored, stored, c.want
					leads++
					h2Lead(lead)
				}
			})
		}
	})
	h2Coverage(map[string]any{"harness": "periodic", "commit": "fcd53ab65", "cases": cases, "leads": leads})
}
