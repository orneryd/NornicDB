package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A baseline and ONE last-observed-poll replay exercise publication/unwind.
// This reuses the original sampled context and statement probe. It does not
// sweep phases/stops, implement a sampler, or assert that a source site was hit.
func stageBContractInvocation[T any](t *testing.T, stop int, run func(*pipelineCancellation) T) (T, int, *pipelineCancellation, context.Context, time.Duration) {
	t.Helper()
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	sampled := &queryCacheSampleContext{Context: base, cancel: cancel, stop: stop}
	ctx := withExpressionFailureSlot(sampled)
	probe := &pipelineCancellation{ctx: ctx, remaining: 128}
	ctx = context.WithValue(ctx, pipelineCancellationKey{}, probe)
	t.Logf("CONTRACT BEFORE phase=128 stop=%d calls=0 remaining=%d", stop, probe.remaining)
	value := run(probe)
	calls, cancelled := sampled.receipt()
	afterCancel := time.Duration(0)
	if !cancelled.IsZero() {
		afterCancel = time.Since(cancelled)
	}
	t.Logf("CONTRACT AFTER phase=128 stop=%d calls=%d remaining=%d latch=%v expressionFailure=%v cancelled=%s afterCancel=%s", stop, calls, probe.remaining, probe.err, getExpressionFailure(ctx), cancelled.Format(time.RFC3339Nano), afterCancel)
	if stop == 0 {
		require.True(t, cancelled.IsZero(), "NONACCEPTANCE baseline cancelled")
		require.NoError(t, probe.err)
		require.NoError(t, getExpressionFailure(ctx))
	} else {
		if calls < stop || cancelled.IsZero() || probe.err == nil {
			t.Fatalf("NONACCEPTANCE late replay did not reach stop/latch: stop=%d calls=%d latch=%v", stop, calls, probe.err)
		}
		require.ErrorIs(t, probe.err, context.Canceled)
		require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
		require.Less(t, afterCancel, 250*time.Millisecond)
		// A latched failure must not consult the context on the parent's next
		// check. Read receipt rather than consuming another sampled Err call.
		require.False(t, probe.check())
		postCalls, _ := sampled.receipt()
		require.Equal(t, calls, postCalls)
	}
	return value, calls, probe, ctx, afterCancel
}

func stageBRequireInnerReplay(t *testing.T, calls int) {
	t.Helper()
	if calls < 2 {
		t.Fatalf("NONACCEPTANCE baseline has no sampled child work: calls=%d", calls)
	}
}

func TestStageBYieldClauseChildCancellation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  *yieldClause
	}{
		{"where_return", "CALL db.labels() YIELD label WHERE label = 'Memory' RETURN label", &yieldClause{items: []yieldItem{{name: "label"}}, where: "label = 'Memory'"}},
		{"star_where", "CALL db.index.fulltext.queryNodes('idx', 'search') YIELD * WHERE score > 0.8", &yieldClause{items: []yieldItem{}, yieldAll: true, where: "score > 0.8"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("query=%q owner=parseYieldClauseWithCancellation", tc.query)
			require.Equal(t, tc.want, parseYieldClause(tc.query))
			run := func(p *pipelineCancellation) *yieldClause { return parseYieldClause(tc.query) }
			got, calls, _, _, _ := stageBContractInvocation(t, 0, run)
			require.Equal(t, tc.want, got)
			stageBRequireInnerReplay(t, calls)
			got, _, _, _, _ = stageBContractInvocation(t, calls, run)
			require.Nil(t, got, "cancelled YIELD must not publish partial metadata")
		})
	}
}

func TestStageBDeclaredMetadataChildCancellation(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "stageb_declared_metadata"))
	for _, tc := range []struct {
		name   string
		query  string
		want   []string
		fabric bool
	}{
		{"yield_names", "CALL db.info() YIELD name, version", []string{"name", "version"}, false},
		{"yield_return_star", "CALL db.info() YIELD name AS dbName RETURN *", []string{"dbName"}, false},
		{"yield_return_modifiers", "CALL db.info() YIELD name, version RETURN name AS dbName ORDER BY dbName SKIP 1 LIMIT 2", []string{"dbName"}, false},
		{"fabric_return_modifiers", "MATCH (n) RETURN n, count(*) AS c ORDER BY c DESC LIMIT 2", []string{"n", "c"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("query=%q fabric=%t", tc.query, tc.fabric)
			run := func(p *pipelineCancellation) []string {
				if tc.fabric {
					return e.inferTopLevelReturnColumns(tc.query)
				}
				return e.StatementColumns(tc.query)
			}
			require.Equal(t, tc.want, run(nil))
			got, calls, _, _, _ := stageBContractInvocation(t, 0, run)
			require.Equal(t, tc.want, got)
			stageBRequireInnerReplay(t, calls)
			got, _, _, _, _ = stageBContractInvocation(t, calls, run)
			require.Nil(t, got, "cancelled child must unwind without publishing metadata")
		})
	}
}

func TestStageBSemicolonChildCancellation(t *testing.T) {
	e := &StorageExecutor{}
	comment := "/*" + strings.Repeat(" ; ", 129) + "*/"
	query := "RETURN `semi;colon`, ';' " + comment + "; RETURN 2 // ;\n"
	want := []string{"RETURN `semi;colon`, ';' " + comment, " RETURN 2 // ;\n"}
	require.Equal(t, want, e.splitBySemicolon(query))
	run := func(p *pipelineCancellation) []string { return e.splitBySemicolon(query) }
	got, calls, _, _, _ := stageBContractInvocation(t, 0, run)
	require.Equal(t, want, got)
	stageBRequireInnerReplay(t, calls)
	got, _, _, _, _ = stageBContractInvocation(t, calls, run)
	require.Nil(t, got, "cancelled lexical child must not publish a partial statement list")
}

func TestStageBFramingCancellationErrorPrecedence(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "stageb_framing_precedence"))
	for _, tc := range []struct {
		name  string
		query string
		code  string
	}{
		{"multiple_statements", "RETURN 1 AS x;;", "Neo.ClientError.Statement.SyntaxError"},
		{"conflicting_modes", "EXPLAIN CYPHER 5 PROFILE RETURN 1", "Neo.ClientError.Statement.ArgumentError"},
		{"union_finish", "RETURN 1 AS x UNION FINISH", "Neo.ClientError.Statement.SyntaxError"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Padding a comment gives the shared child reader real sampled work;
			// the original malformed statement and its existing status remain.
			query := tc.query + " /*" + strings.Repeat(" ", 257) + "*/"
			t.Logf("original=%q checked=%q", tc.query, query)
			want := e.validateStatementFraming(tc.query)
			require.Error(t, want, "NONACCEPTANCE original error control")
			code, _ := nornicerrors.Neo4jStatus(want)
			require.Equal(t, tc.code, code)
			run := func(p *pipelineCancellation) error { return e.validateStatementFraming(query) }
			got, calls, _, _, _ := stageBContractInvocation(t, 0, run)
			require.Error(t, got, "NONACCEPTANCE padded error control")
			code, _ = nornicerrors.Neo4jStatus(got)
			require.Equal(t, tc.code, code)
			stageBRequireInnerReplay(t, calls)
			got, _, _, _, _ = stageBContractInvocation(t, calls, run)
			require.ErrorIs(t, got, context.Canceled, "latched child cancellation takes precedence during framing unwind")
		})
	}
}

func TestStageBPreambleAndRecursiveFinishCancellation(t *testing.T) {
	t.Run("preamble", func(t *testing.T) {
		query := "EXPLAIN CYPHER 5" + strings.Repeat(" ", 257) + "runtime=slotted RETURN 1 AS x"
		want, changed := stripCypherPreamble(query)
		require.True(t, changed)
		require.Equal(t, "EXPLAIN RETURN 1 AS x", want)
		run := func(p *pipelineCancellation) string {
			value, _ := stripCypherPreamble(query)
			return value
		}
		got, calls, _, _, _ := stageBContractInvocation(t, 0, run)
		require.Equal(t, want, got)
		stageBRequireInnerReplay(t, calls)
		// Text is internal scratch here; the parent's error is the latch and
		// existing expression slot, not an invented empty-text contract.
		stageBContractInvocation(t, calls, run)
	})
	t.Run("recursive_union_finish", func(t *testing.T) {
		query := "FINISH UNION ALL FINISH UNION ALL FINISH"
		want, changed := stripUnionBranchFinishes(query)
		require.True(t, changed)
		require.Empty(t, want)
		type result struct {
			text    string
			changed bool
		}
		run := func(p *pipelineCancellation) result {
			text, changed := stripUnionBranchFinishes(query)
			return result{text, changed}
		}
		got, calls, _, _, _ := stageBContractInvocation(t, 0, run)
		require.Equal(t, result{want, changed}, got)
		stageBRequireInnerReplay(t, calls)
		got, _, _, _, _ = stageBContractInvocation(t, calls, run)
		require.Equal(t, result{query, false}, got, "recursive parent must not publish a partially stripped statement")
	})
}
