//go:build closeout_l2_e53_1

package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E53.1: apoc.periodic.iterate counts the batches a unique constraint fails
// but returns empty errorMessages / batch.errors. The parser is the
// process's (NORNICDB_PARSER).

// l2E53_1Iterate is the entry's step 3 with the inner CREATE's divisor as a
// parameter (2 in the entry).
func l2E53_1Iterate(modulo int) string {
	return fmt.Sprintf("CALL apoc.periodic.iterate('MATCH (n:PI) RETURN n ORDER BY n.i', 'CREATE (:PIU {k: n.i %% %d})', {batchSize:1}) ", modulo) +
		"YIELD batches, total, committedOperations, failedOperations, failedBatches, errorMessages, batch " +
		"RETURN batches, total, committedOperations, failedOperations, failedBatches, errorMessages, batch"
}

type l2E53_1Result struct {
	err     error
	fields  map[string]interface{}
	stored  interface{}
	rawRows [][]interface{}
}

func l2E53_1Exec(t *testing.T) *StorageExecutor {
	t.Helper()
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	return NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
}

// l2E53_1Run runs the entry's four statements; modulo is the inner CREATE's
// divisor (2: two batches violate the constraint; 1000: none does).
func l2E53_1Run(t *testing.T, modulo int) l2E53_1Result {
	t.Helper()
	ctx := context.Background()
	exec := l2E53_1Exec(t)
	for _, q := range []string{
		"CREATE CONSTRAINT piu_k IF NOT EXISTS FOR (u:PIU) REQUIRE u.k IS UNIQUE",
		"UNWIND range(1,4) AS i CREATE (:PI {i:i})",
	} {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	iterate := l2E53_1Iterate(modulo)
	out := l2E53_1Result{fields: map[string]interface{}{}}
	res, err := exec.Execute(ctx, iterate, nil)
	out.err = err
	if err == nil {
		out.rawRows = res.Rows
		if len(res.Rows) == 1 {
			for i, col := range res.Columns {
				if i < len(res.Rows[0]) {
					out.fields[col] = res.Rows[0][i]
				}
			}
		}
	}
	count, err := exec.Execute(ctx, "MATCH (u:PIU) RETURN count(u) AS n", nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if len(count.Rows) == 1 {
		out.stored = count.Rows[0][0]
	}
	return out
}

// l2E53_1Len is the number of entries of a map or list value; -1 otherwise.
func l2E53_1Len(v interface{}) int {
	if v == nil {
		return 0
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		return rv.Len()
	}
	return -1
}

func l2E53_1Num(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return -1
}

func l2E53_1BatchErrors(batch interface{}) interface{} {
	if m, ok := batch.(map[string]interface{}); ok {
		return m["errors"]
	}
	return nil
}

func TestL2_e53_1_Repro(t *testing.T) {
	r := l2E53_1Run(t, 2)
	failedBatches := l2E53_1Num(r.fields["failedBatches"])
	msgs := l2E53_1Len(r.fields["errorMessages"])
	batchErrs := l2E53_1Len(l2E53_1BatchErrors(r.fields["batch"]))
	ok := r.err == nil && failedBatches == 2 && msgs > 0 && batchErrs > 0
	verdict := "OK"
	if !ok {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E53.1 %s parser=%s err=%v failedBatches=%d errorMessages=%v(len %d) batch=%v(errors len %d) stored=%v",
		verdict, config.GetParserType(), r.err, failedBatches, r.fields["errorMessages"], msgs, r.fields["batch"], batchErrs, r.stored)
	if r.err != nil {
		t.Fatalf("iterate failed as a whole: %v (stored=%v)", r.err, r.stored)
	}
	if failedBatches != 2 {
		t.Errorf("failedBatches=%d, want 2 (row %v)", failedBatches, r.rawRows)
	}
	if msgs <= 0 {
		t.Errorf("errorMessages=%v is empty; want the two failed batches' constraint messages", r.fields["errorMessages"])
	}
	if batchErrs <= 0 {
		t.Errorf("batch.errors=%v is empty", l2E53_1BatchErrors(r.fields["batch"]))
	}
	if l2E53_1Num(r.stored) != 2 {
		t.Errorf("stored PIU=%v, want 2", r.stored)
	}
}

// Control: no batch violates the constraint (k = i % 1000 is unique): four
// committed operations, no failed batch, no error message, four nodes stored.
func TestL2_e53_1_Control(t *testing.T) {
	r := l2E53_1Run(t, 1000)
	t.Logf("control err=%v fields=%v stored=%v", r.err, r.fields, r.stored)
	if r.err != nil {
		t.Fatalf("control iterate: %v", r.err)
	}
	if l2E53_1Num(r.fields["committedOperations"]) != 4 || l2E53_1Num(r.fields["failedBatches"]) != 0 || l2E53_1Len(r.fields["errorMessages"]) > 0 || l2E53_1Num(r.stored) != 4 {
		t.Errorf("control: fields=%v stored=%v, want 4 committed, 0 failed batches, no messages, 4 stored", r.fields, r.stored)
	}
}

// Cause: the owner is executePeriodicIterate's batch loop
// (call_apoc_periodic.go:~73-118), which keys errorMessages by the failed
// batch's error text. This runs each batch's action by hand in its own
// explicit transaction on the same setup and logs the error text each one
// produces at statement and at COMMIT, i.e. what errorMessages should carry;
// an empty or missing text at the failing step locates the loss.
func TestL2_e53_1_Cause(t *testing.T) {
	ctx := context.Background()
	exec := l2E53_1Exec(t)
	for _, q := range []string{
		"CREATE CONSTRAINT piu_k IF NOT EXISTS FOR (u:PIU) REQUIRE u.k IS UNIQUE",
		"UNWIND range(1,4) AS i CREATE (:PI {i:i})",
	} {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for i := int64(1); i <= 4; i++ {
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			t.Fatalf("BEGIN: %v", err)
		}
		_, stmtErr := exec.Execute(ctx, "MATCH (n:PI {i: $i}) CREATE (:PIU {k: n.i % 2})", map[string]interface{}{"i": i})
		var commitErr error
		if stmtErr != nil {
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
		} else {
			_, commitErr = exec.Execute(ctx, "COMMIT", nil)
		}
		t.Logf("cause parser=%s batch i=%d statementErr=%q commitErr=%q", config.GetParserType(), i, fmt.Sprint(stmtErr), fmt.Sprint(commitErr))
	}
}
