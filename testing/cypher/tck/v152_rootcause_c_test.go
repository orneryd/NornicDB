package tck

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV152RCCSharedReferenceReset is a minimal reproduction of the
// differential corpus's isolation step, against an in-process server so no
// shared reference is disturbed. runDifferentialCases gives every execution a
// backend on the same database name and calls resetDifferentialBackend
// (differential_test.go:488-489, :804-817) before each case; Reset deletes
// every node and drops every index (bolt_backend.go:63-76). Two executions A
// and B with no lease: B's per-case reset lands between A's setup and A's
// read. Control: the same A sequence with no second execution.
func TestV152RCCSharedReferenceReset(t *testing.T) {
	driver, shutdown := startConformanceServer(t)
	defer shutdown()
	ctx := context.Background()
	count := func(b *BoltBackend) string {
		r, err := b.Execute(ctx, "MATCH (n:RCCMarkerA) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return fmt.Sprint(r.Rows)
	}
	indexes := func(b *BoltBackend) int {
		r, err := b.Execute(ctx, "SHOW INDEXES", nil)
		require.NoError(t, err)
		col, n := -1, 0
		for i, c := range r.Columns {
			if c == "name" {
				col = i
			}
		}
		require.GreaterOrEqual(t, col, 0)
		for _, row := range r.Rows {
			if fmt.Sprint(row[col]) == "rcc_case_a" {
				n++
			}
		}
		return n
	}
	a := newDifferentialBackend(t, driver, "nornic", AutocommitMode)
	b := newDifferentialBackend(t, driver, "nornic", AutocommitMode)

	resetDifferentialBackend(t, ctx, a, "A")
	executeDifferentialSetup(t, ctx, a, "A", "CREATE INDEX rcc_case_a FOR (n:RCCMarkerA) ON (n.i)")
	executeDifferentialSetup(t, ctx, a, "A", "CREATE (:RCCMarkerA {i: 1})")
	control, controlIdx := count(a), indexes(a)
	t.Logf("RCC_TCK_CONTROL owner=testing/cypher/tck/differential_test.go:488-489 rows=%s case_index=%d", control, controlIdx)
	require.Equal(t, "[[1]]", control)
	require.Equal(t, 1, controlIdx)

	resetDifferentialBackend(t, ctx, b, "B") // the other execution's next case
	trigger, triggerIdx := count(a), indexes(a)
	t.Logf("RCC_TCK_TRIGGER owner=testing/cypher/tck/bolt_backend.go:63-76 database=nornic rows_after_other_reset=%s case_index_after_other_reset=%d", trigger, triggerIdx)
	assert.Equal(t, "[[1]]", trigger, "another execution's reset must not remove this execution's case data")
	assert.Equal(t, 1, triggerIdx, "another execution's reset must not drop this execution's case index")
}
