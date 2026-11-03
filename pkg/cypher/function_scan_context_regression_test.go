package cypher

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var functionScanContextTestSequence atomic.Uint64

// The semantic function scan belongs to the caller's request. It visits the
// statement's calls under the caller's context, and it reports the caller's
// cancellation instead of reading the rest of the statement: a scan that ran
// on a background context would return nil here and keep visiting.
func TestFunctionScansHonorCallerCancellation(t *testing.T) {
	const query = "RETURN toLower(toString(1)) AS a, abs(-1) AS b"
	for _, scan := range []struct {
		name string
		run  func(context.Context, func(string) error) error
	}{
		{"call", func(ctx context.Context, visit func(string) error) error {
			return forEachFunctionCall(ctx, query, func(_, normalized string) error { return visit(normalized) })
		}},
		{"invocation", func(ctx context.Context, visit func(string) error) error {
			return forEachFunctionInvocation(ctx, query, func(_, normalized string, _ int) error { return visit(normalized) })
		}},
	} {
		t.Run(scan.name, func(t *testing.T) {
			var names []string
			require.NoError(t, scan.run(t.Context(), func(name string) error {
				names = append(names, name)
				return nil
			}))
			require.Equal(t, []string{"tolower", "tostring", "abs"}, names, "healthy scanning is unchanged")

			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			visited := 0
			require.ErrorIs(t, scan.run(canceled, func(string) error {
				visited++
				return nil
			}), context.Canceled, "a canceled scan reports the caller's cancellation")
			require.Zero(t, visited, "a scan already canceled performs no visit")

			// Cancellation raised at a scan checkpoint (a real cancel func on a
			// live context, not a deadline or a canned error) stops the scan.
			checkpoint := fixtureowner.NewCheckpointContext(1, nil)
			defer checkpoint.Cancel()
			visited = 0
			require.ErrorIs(t, scan.run(checkpoint, func(string) error {
				visited++
				return nil
			}), context.Canceled)
			require.Positive(t, checkpoint.Checks())
			require.Zero(t, visited)
		})
	}
}

// A scan canceled before it reads the whole expression has not reached a
// verdict. The row shape and predicate readers must keep the caller's
// statement context for that read and must not publish the partial verdict in
// their text-keyed caches; a live read still publishes and is reused.
func TestCanceledShapeReadsDoNotPublishVerdicts(t *testing.T) {
	nonce := strconv.FormatUint(functionScanContextTestSequence.Add(1), 10)
	shapeText := "zzshapeProbe" + nonce + "(n.keep) = 1 AND n.other = 2"
	predicateText := "zzpredicateProbe" + nonce + "(n.keep) = 1 OR n.other = 2"
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	shape := rowContextShapeOf(canceled, shapeText)
	require.True(t, shape.statementContext, "a canceled read retains the statement context instead of dropping it")
	_, published := rowContextShapes.get(shapeText)
	require.False(t, published, "a verdict read under cancellation is not published")

	require.True(t, rowPredicateNeedsStatementContext(canceled, predicateText))
	_, published = rowPredicateStatementContextNeeds.get(predicateText)
	require.False(t, published)

	require.Nil(t, rowPredicateProgramFor(canceled, predicateText))
	_, published = rowPredicatePrograms.get(predicateText)
	require.False(t, published)

	healthy := rowContextShapeOf(t.Context(), shapeText)
	require.True(t, healthy.statementContext)
	cachedShape, published := rowContextShapes.get(shapeText)
	require.True(t, published, "a live read publishes its verdict")
	require.Same(t, healthy, cachedShape)

	require.True(t, rowPredicateNeedsStatementContext(t.Context(), predicateText))
	needs, published := rowPredicateStatementContextNeeds.get(predicateText)
	require.True(t, published, "a live read publishes its verdict")
	require.True(t, needs)

	require.Nil(t, rowPredicateProgramFor(t.Context(), predicateText), "the first live read marks the predicate")
	_, published = rowPredicatePrograms.get(predicateText)
	require.True(t, published)
	program := rowPredicateProgramFor(t.Context(), predicateText)
	require.NotNil(t, program)
	require.True(t, program.statementContext)
	cachedProgram, published := rowPredicatePrograms.get(predicateText)
	require.True(t, published, "a live read publishes its program")
	require.Same(t, program, cachedProgram)

	// A later canceled read reuses the live verdicts rather than replacing them.
	require.Same(t, healthy, rowContextShapeOf(canceled, shapeText))
	require.True(t, rowPredicateNeedsStatementContext(canceled, predicateText))
	require.Same(t, program, rowPredicateProgramFor(canceled, predicateText))
}

// A public Execute canceled while its own function scan looks a plugin name up
// returns the caller's cancellation as the statement's error, performs no
// plugin work, and a later live Execute runs the same statement normally.
func TestPublicExecuteCancellationDuringFunctionScan(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scan_cancel"))
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	canceled, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var invocations atomic.Int64
	PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
		if name != "zzscan.cancel" {
			return nil, false
		}
		// The scan's own name lookup cancels the running request.
		cancel()
		return func() int64 { invocations.Add(1); return 7 }, true
	})

	_, err := executor.Execute(canceled, "RETURN zzscan.cancel() AS value", nil)
	require.ErrorIs(t, err, context.Canceled, "the caller's cancellation is the statement's error")
	require.Zero(t, invocations.Load(), "a canceled statement does not reach the plugin")

	result, err := executor.Execute(t.Context(), "RETURN zzscan.cancel() AS value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
	require.Positive(t, invocations.Load())
}

// Canceled public executions across the statement's work boundaries leave the
// result cache live: canceled runs publish no new result, and subsequent live
// runs return the result with the expected warm/cold cache hits. A canceled verdict
// that was published in place of a live one would fail one of these.
func TestPublicExecuteScanCancellationRecoversCache(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scan_recovery"), QueryCachePolicy{MaxEntries: 10, TTL: time.Minute})
	const query = "RETURN toLower('SCAN_RECOVERY') AS scan_recovery"
	for _, budget := range []int{1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64} {
		checkpoint := fixtureowner.NewCheckpointContext(budget, nil)
		_, _, entriesBefore, _, _ := executor.cache.Stats()
		_, err := executor.Execute(checkpoint, query, nil)
		if checkpoint.Context.Err() != nil {
			require.ErrorIs(t, err, context.Canceled, "budget %d", budget)
			_, _, entriesAfter, _, _ := executor.cache.Stats()
			require.Equal(t, entriesBefore, entriesAfter, "canceled budget %d published a result", budget)
		} else {
			require.NoError(t, err, "budget %d", budget)
		}
		checkpoint.Cancel()
	}
	hitsBefore, _, entriesBefore, _, _ := executor.cache.Stats()
	require.LessOrEqual(t, entriesBefore, 1)
	for range 2 {
		result, err := executor.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"scan_recovery"}, result.Columns)
		require.Equal(t, [][]interface{}{{"scan_recovery"}}, result.Rows)
	}
	hits, _, _, _, _ := executor.cache.Stats()
	expectedHits := int64(1)
	if entriesBefore != 0 {
		expectedHits = 2 // An earlier uncanceled budget may have warmed this result.
	}
	require.Equal(t, expectedHits, hits-hitsBefore, "a canceled classification cannot disable a later live cache entry")
}

func TestFunctionScanBoundaryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, text := range []string{"", "  ", "'only a literal'", "/* only a comment */"} {
		err := forEachFunctionInvocation(ctx, text, func(string, string, int) error {
			t.Fatal("a canceled scanner visited a function")
			return nil
		})
		require.ErrorIs(t, err, context.Canceled, "text %q", text)
	}
	live, stop := context.WithCancel(t.Context())
	defer stop()
	visits := 0
	err := forEachFunctionInvocation(live, "abs(1)", func(string, string, int) error {
		visits++
		stop()
		return nil
	})
	require.Equal(t, 1, visits)
	require.ErrorIs(t, err, context.Canceled, "cancellation during the last callback is not a successful scan")
}
