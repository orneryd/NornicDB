//go:build closeout_l2_e38_20

package cypher

import (
	"context"
	"runtime"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// E38.20: the first statement after a BEGIN in a fresh process allocates
// 8.4-11.9 MB on 2dbc67e96 against 0.15-0.39 MB on b6d8e762a; the profile's
// nine largest allocating functions are in the ANTLR runtime (cold grammar
// prediction state), also under the default parser.
//
// FRESH PROCESS: the Repro must be the first statement this test process
// executes. Run it alone or first (Go runs the selected tests in file order,
// and the Repro is declared first); the Cause needs its own process too (see
// the notes). The measured statement is `RETURN 1 AS one` (8,352,232 bytes as
// a first statement on 2dbc67e96, 149,712 on b6d8e762a).
//
// Threshold 2 MB: 4x under the smallest defect figure (8.35 MB) and 5x over
// the b6d8e762a figures (0.15-0.39 MB). The parser is the process's
// (NORNICDB_PARSER).

const (
	l2E38_20Bound     = 2 << 20
	l2E38_20Statement = "RETURN 1 AS one"
)

func l2E38_20Allocated(t *testing.T, exec *StorageExecutor, ctx context.Context, statement string) uint64 {
	t.Helper()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	res, err := exec.Execute(ctx, statement, nil)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%s returned %v", statement, res.Rows)
	}
	return after.TotalAlloc - before.TotalAlloc
}

func TestL2_e38_20_Repro(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := WithTransactionControl(context.Background())
	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	first := l2E38_20Allocated(t, exec, ctx, l2E38_20Statement)
	second := l2E38_20Allocated(t, exec, ctx, l2E38_20Statement)
	if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}
	if first > l2E38_20Bound {
		t.Logf("L2RESULT E38.20 DEFECT first=%dB second=%dB bound=%dB", first, second, l2E38_20Bound)
		t.Errorf("the first statement after BEGIN in this process allocated %d bytes (bound %d); the same statement next allocated %d", first, l2E38_20Bound, second)
		return
	}
	t.Logf("L2RESULT E38.20 OK first=%dB second=%dB bound=%dB", first, second, l2E38_20Bound)
}

// TestL2_e38_20_Control: once the process has run a statement (a warm-up on
// this executor), the same statement after BEGIN allocates far under the
// bound: the harness measures the statement's own work.
func TestL2_e38_20_Control(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := WithTransactionControl(context.Background())
	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	warm := l2E38_20Allocated(t, exec, ctx, l2E38_20Statement)
	measured := l2E38_20Allocated(t, exec, ctx, l2E38_20Statement)
	if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}
	t.Logf("control: warm-up %d B, measured %d B", warm, measured)
	if measured > l2E38_20Bound {
		t.Errorf("control: a warm statement after BEGIN allocated %d bytes", measured)
	}
}

// TestL2_e38_20_Cause (own fresh process): the ANTLR grammar's first
// validation of the statement is measured before anything else runs
// (cypherantlr.Validate, pkg/cypher/antlr/parse.go:50), then BEGIN and the
// first statement. If the first Validate allocates about the Repro's figure
// and the statement after it is small, the first statement's cost is the
// cold initialisation of the ANTLR prediction state.
func TestL2_e38_20_Cause(t *testing.T) {
	for _, phase := range []string{"first", "warm"} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		err := cypherantlr.Validate(l2E38_20Statement)
		runtime.ReadMemStats(&after)
		t.Logf("L2CAUSE E38.20 %s cypherantlr.Validate: %d B err=%v", phase, after.TotalAlloc-before.TotalAlloc, err)
	}
	exec := NewStorageExecutor(newTestMemoryEngine(t), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := WithTransactionControl(context.Background())
	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	first := l2E38_20Allocated(t, exec, ctx, l2E38_20Statement)
	if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}
	t.Logf("L2CAUSE E38.20 first statement after BEGIN, after grammar preparation: %d B", first)
}
