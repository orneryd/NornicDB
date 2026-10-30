// Package cypher — Plan 04-03-03 GREEN tests for the three observation
// chokepoints that emit nornicdb_cypher_* metrics from StorageExecutor.
//
// Three observation sites under test (per RISK-1 corrected mapping):
//
//	Site 1 — admin dispatch (isSystemCommandNoGraph branch)
//	Site 2 — parse-error (validateSyntax err return)
//	Site 3 — normal-path-after-Analyze (read/write/schema/fabric)
//
// Plus D-16 transaction_conflicts wiring (storage detects, Cypher counts)
// and gauge balance for active_transactions / slow_queries threshold gate.
package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCypherMetricsForTest(t *testing.T) (*observability.CypherMetrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	bag := observability.NewCypherMetrics(reg, false /* tenant flag off */, func() float64 { return 1.0 })
	return bag, reg
}

// TestObserveQuery_NormalPath drives a MATCH and asserts queries_total
// observes op_type="read" + duration histogram observed.
func TestObserveQuery_NormalPath(t *testing.T) {
	exec, store := newTestExecutor(t)
	bag, reg := newCypherMetricsForTest(t)
	exec.SetCypherMetrics(bag, "")

	_, _ = store.CreateNode(&storage.Node{
		ID:     "n1",
		Labels: []string{"User"},
	})

	_, err := exec.Execute(context.Background(), "MATCH (n:User) RETURN n", nil)
	require.NoError(t, err)

	// queries_total{op_type="read"} should be 1.
	got := testutil.ToFloat64(bag.Queries.WithLabelValues("read"))
	assert.Equal(t, 1.0, got, "Site 3: read op_type increments on MATCH")

	// query_duration_seconds{op_type="read"} should have one observation.
	cnt, err := testutil.GatherAndCount(reg, "nornicdb_cypher_query_duration_seconds")
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "Site 3: duration histogram observed")
}

// TestObserveQuery_NormalPath_Write drives a CREATE and asserts op_type="write".
func TestObserveQuery_NormalPath_Write(t *testing.T) {
	exec, _ := newTestExecutor(t)
	bag, _ := newCypherMetricsForTest(t)
	exec.SetCypherMetrics(bag, "")

	_, err := exec.Execute(context.Background(), "CREATE (n:User {name: 'Alice'})", nil)
	require.NoError(t, err)

	got := testutil.ToFloat64(bag.Queries.WithLabelValues("write"))
	assert.Equal(t, 1.0, got, "Site 3: write op_type increments on CREATE")
}

// TestObserveQuery_AdminDispatch drives SHOW DATABASES (without dbManager
// configured the executor returns an error, but the metric still observes
// at Site 1 BEFORE Analyze runs).
func TestObserveQuery_AdminDispatch(t *testing.T) {
	exec, _ := newTestExecutor(t)
	bag, _ := newCypherMetricsForTest(t)
	exec.SetCypherMetrics(bag, "")

	// SHOW DATABASES routes through isSystemCommandNoGraph → executeWithoutTransaction.
	// We're not asserting query success here (no dbManager wired); we're
	// asserting the Site-1 observation fires regardless of execution outcome.
	_, _ = exec.Execute(context.Background(), "SHOW DATABASES", nil)

	got := testutil.ToFloat64(bag.Queries.WithLabelValues("admin"))
	assert.Equal(t, 1.0, got,
		"Site 1: admin op_type increments BEFORE Analyze on SHOW DATABASES")
}

// TestObserveQuery_ParseError drives invalid syntax; the validateSyntax
// path emits op_type="parse_error" (Site 2) WITHOUT a duration observation
// (parse cost is sub-microsecond — not meaningful to bucket).
func TestObserveQuery_ParseError(t *testing.T) {
	exec, _ := newTestExecutor(t)
	bag, reg := newCypherMetricsForTest(t)
	exec.SetCypherMetrics(bag, "")

	// Garbage that fails validateSyntax.
	_, err := exec.Execute(context.Background(), "INVALID NOT_A_QUERY ((", nil)
	require.Error(t, err, "validateSyntax must reject invalid syntax")

	got := testutil.ToFloat64(bag.Queries.WithLabelValues("parse_error"))
	assert.Equal(t, 1.0, got, "Site 2: parse_error op_type increments at validateSyntax err")

	// Site 2 does NOT observe duration (no meaningful timing for a parse error).
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_cypher_query_duration_seconds" {
			continue
		}
		// If duration histogram surfaced, it MUST NOT have a parse_error series.
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == "op_type" {
					assert.NotEqual(t, "parse_error", lp.GetValue(),
						"Site 2: parse_error MUST NOT emit duration observation")
				}
			}
		}
	}
}

// TestObserveQuery_NilBag_NoOp asserts the executor tolerates an unset
// metrics bag (forward-compat: Plan 04-03 wires the bag at startup but
// older test fixtures or alternate constructors may not).
func TestObserveQuery_NilBag_NoOp(t *testing.T) {
	exec, _ := newTestExecutor(t)
	// no SetCypherMetrics call — metrics is nil

	_, err := exec.Execute(context.Background(), "MATCH (n) RETURN n", nil)
	require.NoError(t, err, "executor without metrics bag must not panic")
}

// TestTransactionConflicts_Increment (D-16) — drive a synthetic ErrConflict
// observation through the cypher transaction wrapper site and assert the
// counter increments. Storage layer never imports observability; the
// observation site lives in pkg/cypher.
func TestTransactionConflicts_Increment(t *testing.T) {
	exec, _ := newTestExecutor(t)
	bag, _ := newCypherMetricsForTest(t)
	exec.SetCypherMetrics(bag, "")

	// Direct call into the helper — D-16 chokepoint surface. The executor
	// exposes observeTransactionConflict for the storage-conflict-aware
	// transaction wrapper sites that surface ErrConflict to the caller.
	exec.observeTransactionConflict(storage.ErrConflict)

	got := testutil.ToFloat64(bag.TransactionConflicts.WithLabelValues())
	assert.Equal(t, 1.0, got, "D-16: storage conflict increments cypher counter")

	// Non-conflict errors must not increment.
	exec.observeTransactionConflict(errors.New("not a conflict"))
	got = testutil.ToFloat64(bag.TransactionConflicts.WithLabelValues())
	assert.Equal(t, 1.0, got, "D-16: non-conflict errors must NOT increment")

	// Nil error must not increment.
	exec.observeTransactionConflict(nil)
	got = testutil.ToFloat64(bag.TransactionConflicts.WithLabelValues())
	assert.Equal(t, 1.0, got, "D-16: nil error must NOT increment")
}

// TestActiveTransactions_GaugeBalance asserts transaction Begin/Commit
// pairs balance the active_transactions gauge to 0 (Inc on Begin; Dec on
// Commit/Rollback).
func TestActiveTransactions_GaugeBalance(t *testing.T) {
	exec, _ := newTestExecutor(t)
	bag, _ := newCypherMetricsForTest(t)
	exec.SetCypherMetrics(bag, "")

	// Use the small chokepoint helpers directly so the test stays focused
	// on gauge balance semantics, not transaction-engine plumbing.
	exec.observeTransactionBegin()
	exec.observeTransactionBegin()
	assert.Equal(t, 2.0, testutil.ToFloat64(bag.ActiveTransactions),
		"two Begin calls → gauge=2")

	exec.observeTransactionEnd()
	assert.Equal(t, 1.0, testutil.ToFloat64(bag.ActiveTransactions),
		"one End → gauge=1")

	exec.observeTransactionEnd()
	assert.Equal(t, 0.0, testutil.ToFloat64(bag.ActiveTransactions),
		"second End → gauge=0 (balanced)")
}

// TestSlowQueries_ThresholdedIncrement asserts the slow_queries counter
// increments only when duration meets the configured threshold (matches
// Phase 2 D-04c slow-query log emission gate semantics).
func TestSlowQueries_ThresholdedIncrement(t *testing.T) {
	exec, _ := newTestExecutor(t)
	bag, _ := newCypherMetricsForTest(t)
	exec.SetCypherMetrics(bag, "")
	exec.SetSlowQueryThreshold(50 * time.Millisecond)

	// Below threshold → no increment.
	exec.observeSlowQueryIfThresholded(10 * time.Millisecond)
	assert.Equal(t, 0.0, testutil.ToFloat64(bag.SlowQueries.WithLabelValues()),
		"duration < threshold → no increment")

	// At/above threshold → increment.
	exec.observeSlowQueryIfThresholded(100 * time.Millisecond)
	assert.Equal(t, 1.0, testutil.ToFloat64(bag.SlowQueries.WithLabelValues()),
		"duration ≥ threshold → 1 increment")

	// Threshold of 0 disables (matches emitSlowQueryLog semantics).
	exec.SetSlowQueryThreshold(0)
	exec.observeSlowQueryIfThresholded(1 * time.Hour)
	assert.Equal(t, 1.0, testutil.ToFloat64(bag.SlowQueries.WithLabelValues()),
		"threshold=0 disables emission (Phase 2 D-04c semantics)")
}
