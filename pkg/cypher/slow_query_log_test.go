// Wave-0 RED test for the cypher slow-query slog emission schema (LOG-07 + D-04c).
//
// References (*StorageExecutor).SetLogger and (*StorageExecutor).SetSlowQueryThreshold
// which do not yet exist; package fails to compile until the GREEN task ships them.
package cypher

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestExecutor_SlowQueryLog_Schema asserts the LOG-07 schema:
//
//	level=WARN
//	msg="slow query"
//	event="slow_query"
//	plan_hash matches ^[0-9a-f]{16}$
//	cypher.duration_ms is a number >= 0
//	query is a string of <= 500 chars containing "<REDACTED>" (input has a string literal).
func TestExecutor_SlowQueryLog_Schema(t *testing.T) {
	te := observability.NewTestEnv(t)
	te.CaptureRecords()

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "slow_query_test")
	exec := NewStorageExecutor(store)
	exec.SetLogger(te.Logger)
	// Force every query into the slow-query path.
	exec.SetSlowQueryThreshold(1 * time.Nanosecond)

	ctx := context.Background()
	// Query MUST contain a string literal so the redacted attr provably contains "<REDACTED>".
	if _, err := exec.Execute(ctx, `MATCH (n {name: "alice"}) RETURN n`, nil); err != nil {
		// Execution may fail for other reasons (no nodes, etc.); that's OK — we only
		// care that the slow-query log fired before the return.
		t.Logf("execute returned err=%v (acceptable for slow-query schema test)", err)
	}

	records := te.LoggedRecords()
	if len(records) == 0 {
		t.Fatalf("no slow-query records captured")
	}

	hexRe := regexp.MustCompile(`^[0-9a-f]{16}$`)
	var found bool
	for _, rec := range records {
		if rec["level"] != "WARN" {
			continue
		}
		if rec["msg"] != "slow query" {
			continue
		}
		if rec["event"] != "slow_query" {
			t.Errorf("expected event=slow_query, got %v", rec["event"])
			continue
		}
		ph, ok := rec["plan_hash"].(string)
		if !ok || !hexRe.MatchString(ph) {
			t.Errorf("plan_hash %v not valid 16-char hex", rec["plan_hash"])
			continue
		}
		dur, ok := rec["cypher.duration_ms"].(float64) // JSON numbers decode as float64
		if !ok || dur < 0 {
			t.Errorf("cypher.duration_ms %v invalid", rec["cypher.duration_ms"])
			continue
		}
		q, ok := rec["query"].(string)
		if !ok {
			t.Errorf("query attr not a string: %T", rec["query"])
			continue
		}
		if len(q) > 500 {
			t.Errorf("query attr exceeds 500 chars: len=%d", len(q))
			continue
		}
		if !strings.Contains(q, RedactedPlaceholder) {
			t.Errorf("query attr missing redaction marker: %q", q)
			continue
		}
		if strings.Contains(q, "alice") {
			t.Errorf("LOG-08 violation: 'alice' literal leaked into log: %q", q)
		}
		found = true
	}
	if !found {
		t.Fatalf("no record matched the LOG-07 schema; records=%v", records)
	}
}

// TestExecutor_SlowQueryLog_TruncatesAt500 — query attr is exactly <= 500 chars
// even when the redacted query is longer.
func TestExecutor_SlowQueryLog_TruncatesAt500(t *testing.T) {
	te := observability.NewTestEnv(t)
	te.CaptureRecords()

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "slow_query_test_trunc")
	exec := NewStorageExecutor(store)
	exec.SetLogger(te.Logger)
	exec.SetSlowQueryThreshold(1 * time.Nanosecond)

	// Build a query whose redacted form will be > 500 chars. WHERE n.k0 = 0 OR n.k1 = 1 ...
	// Each clause adds ~20 chars (after redaction); 30 clauses ~ 600+ chars.
	var b strings.Builder
	b.WriteString("MATCH (n) WHERE ")
	for i := 0; i < 30; i++ {
		if i > 0 {
			b.WriteString(" OR ")
		}
		// fixed-width identifier so the test is deterministic
		b.WriteString("n.k")
		b.WriteString(string(rune('a' + (i % 26))))
		b.WriteString(" = ")
		b.WriteString("12345")
	}
	b.WriteString(" RETURN n")

	ctx := context.Background()
	if _, err := exec.Execute(ctx, b.String(), nil); err != nil {
		t.Logf("execute returned err=%v (acceptable)", err)
	}

	for _, rec := range te.LoggedRecords() {
		if rec["msg"] != "slow query" {
			continue
		}
		q, ok := rec["query"].(string)
		if !ok {
			continue
		}
		if len(q) > 500 {
			t.Fatalf("expected truncation at 500, got len=%d", len(q))
		}
		return // success
	}
	t.Fatalf("no slow-query record observed for truncation test")
}
