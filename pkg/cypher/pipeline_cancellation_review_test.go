package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

func TestPipelineSingleRowExpressionDeadlines(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	items := make([]interface{}, 1000000)
	words := make([]interface{}, 1000000)
	for index := range items {
		items[index] = int64(index)
		words[index] = "text"
	}
	values := pipelineRow{"items": items, "words": words, "text": strings.Repeat("é", 16<<20)}
	for _, expression := range []string{
		"range(1,1000000000)", "[x IN items | x+1]", "[x IN items | [y IN items | x+y]]",
		"reduce(s=0,x IN items | s+x)", "all(x IN items WHERE x>=0)", "reverse(text)",
		"lpad('',1000000000,'x')", "rpad('',1000000000,'x')", "apoc.text.join(words,',')",
	} {
		t.Run(expression, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(withExpressionFailureSlot(context.Background()), 20*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			var id atomic.Uint64
			done := make(chan bool, 1)
			go func() {
				id.Store(pipelineCurrentGoroutine())
				_, ok := executor.evaluateRowExpressionWithContext(ctx, expression, values)
				done <- ok
			}()
			select {
			case ok := <-done:
				require.False(t, ok, "fixture must still be doing work at its deadline")
				require.ErrorIs(t, getExpressionFailure(ctx), context.DeadlineExceeded)
				require.Less(t, time.Since(deadline), 250*time.Millisecond)
				t.Logf("deadline=%s overdue=%s", deadline.Format(time.RFC3339Nano), time.Since(deadline))
			case <-time.After(time.Second):
				t.Fatal("one row continued past its deadline")
			}
			assertPipelineQueryExited(t, id.Load())
		})
	}
}

func TestPipelineSingleRowValidationDeadline(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	items := make([]interface{}, 1000000)
	for index := range items {
		items[index] = int64(index)
	}
	for _, stage := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"property_index_parameter", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_, _, _ = executor.parseSimpleIndexedInParamWithCancellation("n", "n.x IN $items", map[string]interface{}{"items": items}, &probe)
			return probe.err
		}}, {"membership_index", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			cache := bindingParamMembershipCache{}
			_, _, _ = cache.getValidatedWithCancellation(items, &probe)
			require.Nil(t, cache.snapshot, "a cancelled index must not be published")
			return probe.err
		}},
	} {
		t.Run(stage.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(withExpressionFailureSlot(context.Background()), 20*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			err := stage.run(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Less(t, time.Since(deadline), 250*time.Millisecond)
			t.Logf("deadline=%s overdue=%s", deadline.Format(time.RFC3339Nano), time.Since(deadline))
		})
	}
}

func TestPipelineComparisonStopsAtCancelledSample(t *testing.T) {
	list := make([]interface{}, 1000000)
	for index := range list {
		list[index] = int64(0)
	}
	for _, compare := range []func(interface{}, interface{}, *pipelineCancellation){
		func(left, right interface{}, probe *pipelineCancellation) {
			_ = compareValuesForSortWithCancellation(left, right, probe)
		},
		func(left, right interface{}, probe *pipelineCancellation) {
			_ = cypherEqualityWithCancellation(left, right, probe)
		},
		func(left, right interface{}, probe *pipelineCancellation) {
			_, _ = compareCypherOrderedValuesWithCancellation(left, right, probe)
		},
	} {
		ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
		probe := pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
		compare(list, list, &probe)
		require.ErrorIs(t, probe.err, context.Canceled)
		require.Zero(t, probe.remaining, "no further comparisons may consume the latched probe after cancellation")
		require.Equal(t, 2, ctx.calls)
	}
}

func TestPipelineMaterializationSamplingAndResults(t *testing.T) {
	for _, count := range []int{1, 100000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			rows := make([]pipelineRow, count)
			for index := range rows {
				rows[index] = pipelineRow{"x": int64(index)}
			}
			ctx := &pipelineCountingContext{Context: context.Background()}
			result, ok := materializePipelineSource(ctx, pipelineRowsSource(rows))
			require.True(t, ok)
			require.Equal(t, rows, result)
			if count == 1 {
				require.Equal(t, 1, ctx.calls)
			} else {
				// Materialization checks rows, their fields and bounded output
				// copies. The shared samples remain amortized over all work;
				// they are not one ctx.Err call for each row or copied slot.
				require.GreaterOrEqual(t, ctx.calls, 1+(count-1)/pipelineCancellationInterval)
				require.LessOrEqual(t, ctx.calls, 1+(5*count-1)/pipelineCancellationInterval)
			}
			t.Logf("rows=%d context_samples=%d", count, ctx.calls)
		})
	}
}

func TestPipelineLargeKeyCopyStopsDuringUnwind(t *testing.T) {
	leaf := strings.Repeat("large-value", 1<<20)
	for _, value := range []interface{}{leaf, []byte(leaf), []interface{}{leaf}, map[string]interface{}{"prefix": leaf, "tail": []interface{}{leaf}}} {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			healthy := pipelineCancellation{ctx: context.Background()}
			require.Equal(t, cypherEquivalenceKey(value), cypherEquivalenceKeyWithCancellation(value, &healthy))
			ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
			probe := pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
			_ = cypherEquivalenceKeyWithCancellation([]interface{}{[]interface{}{value}}, &probe)
			require.ErrorIs(t, probe.err, context.Canceled)
			require.Zero(t, probe.remaining, "cancelled nested prefixes must not be copied while unwinding")
		})
	}
}

func TestPipelineSlowDiagnosticDoesNotDelayDeadline(t *testing.T) {
	environment := observability.NewTestEnv(t)
	environment.CaptureRecords()
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	executor.SetLogger(environment.Logger)
	executor.SetSlowQueryThreshold(time.Nanosecond)
	query := "RETURN 'private-literal' AS value " + strings.Repeat("UNION ALL RETURN 'private-literal' AS value ", 20000)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	<-ctx.Done()
	deadline, _ := ctx.Deadline()
	_, err := executor.Execute(ctx, query, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(deadline), 250*time.Millisecond)
	found := false
	for _, record := range environment.LoggedRecords() {
		if record["event"] == "slow_query" {
			found = true
			require.Equal(t, RedactedPlaceholder, record["query"])
		}
	}
	require.True(t, found, "the deferred diagnostic must still emit its safe event")
}

func TestPipelineDiagnosticRedactionFailsClosed(t *testing.T) {
	for _, query := range []string{
		"RETURN 'private-literal' AS value // private-comment",
		"RETURN \"private-literal\" AS value /* private-comment */",
		"MATCH (n {name: 'private-literal'}) RETURN n",
		"RETURN 'private-literal",
		"RETURN ['private-literal', 123456789]",
		strings.Repeat("RETURN 'private-literal' ", 1000),
	} {
		redacted := RedactLiterals(StripComments(query))
		require.NotContains(t, redacted, "private-literal")
		require.NotContains(t, redacted, "private-comment")
		require.NotContains(t, redacted, "123456789")
	}
}

func TestPipelineFallbackKeyPreservesGoSyntax(t *testing.T) {
	// Paths have their own identity key (cypherEquivalenceKey); the fallback
	// covers the remaining Go values.
	for _, value := range []interface{}{CypherVector{Type: VectorFloat64, Floats: []float64{1.25, -2.5}}, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), struct {
		Text  string
		Items []interface{}
		Map   map[int]string
	}{"a\"é\n", []interface{}{1, nil, "x"}, map[int]string{10: "b", 2: "a"}}} {
		probe := pipelineCancellation{ctx: context.Background()}
		want := "T" + fmt.Sprintf("%T:%#v", value, value)
		require.Equal(t, want, cypherEquivalenceKeyWithCancellation(value, &probe))
	}
	// Composite map keys need a deterministic order even when their printed
	// scalar payloads match across types.
	value := struct{ Map map[interface{}]interface{} }{map[interface{}]interface{}{int64(1): "integer", float64(1): "float", [2]int{1, 2}: "array", struct{ X int }{1}: "struct", false: "boolean"}}
	want := cypherEquivalenceKey(value)
	for attempt := 0; attempt < 50; attempt++ {
		probe := pipelineCancellation{ctx: context.Background()}
		require.Equal(t, want, cypherEquivalenceKeyWithCancellation(value, &probe))
	}
	for _, pair := range [][2]interface{}{{int(2), int(10)}, {uint(2), uint(10)}, {1.5, 2.5}, {complex(1, 2), complex(1, 3)}, {false, true}, {"a", "b"}, {[2]int{1, 2}, [2]int{1, 3}}, {struct{ X int }{1}, struct{ X int }{2}}} {
		probe := pipelineCancellation{ctx: context.Background()}
		require.Negative(t, compareEquivalenceGoMapKeys(reflect.ValueOf(pair[0]), reflect.ValueOf(pair[1]), &probe))
	}
}
