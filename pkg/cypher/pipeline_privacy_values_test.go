package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
)

func TestPipelineCollectionFunctionCancellation(t *testing.T) {
	const count = 257
	items := make([]interface{}, count)
	pairs := make([]interface{}, count)
	fields := make(map[string]interface{}, count)
	copyItems := make([]interface{}, 128*count)
	words := make([]string, count)
	for i := range items {
		items[i] = int64(i)
		words[i] = fmt.Sprint(i)
		pairs[i] = []interface{}{fmt.Sprint(i), int64(i)}
		fields[fmt.Sprint(i)] = int64(i)
	}
	cases := []struct {
		name string
		run  func(*pipelineCancellation) interface{}
	}{
		{"flatten", func(p *pipelineCancellation) interface{} {
			return flattenListWithCancellation([]interface{}{items}, true, p)
		}},
		{"set", func(p *pipelineCancellation) interface{} { return toSetWithCancellation(items, p) }},
		{"flatten_strings", func(p *pipelineCancellation) interface{} { return flattenListWithCancellation(words, true, p) }},
		{"set_strings", func(p *pipelineCancellation) interface{} { return toSetWithCancellation(words, p) }},
		{"join", func(p *pipelineCancellation) interface{} {
			return joinPipelineValues(words, strings.Repeat(",", 8192), p)
		}},
		{"sum", func(p *pipelineCancellation) interface{} { return apocCollSumWithCancellation(items, p) }},
		{"avg", func(p *pipelineCancellation) interface{} { return apocCollAvgWithCancellation(items, p) }},
		{"min", func(p *pipelineCancellation) interface{} { return apocCollMinWithCancellation(items, p) }},
		{"max", func(p *pipelineCancellation) interface{} { return apocCollMaxWithCancellation(items, p) }},
		{"sort", func(p *pipelineCancellation) interface{} { return apocCollSortWithCancellation(items, p) }},
		{"sort_nodes", func(p *pipelineCancellation) interface{} { return apocCollSortNodesWithCancellation(pairs, "x", p) }},
		{"reverse", func(p *pipelineCancellation) interface{} { return apocCollReverseWithCancellation(items, p) }},
		{"union", func(p *pipelineCancellation) interface{} { return apocCollUnionWithCancellation(items, items, p) }},
		{"union_all", func(p *pipelineCancellation) interface{} {
			return apocCollUnionAllWithCancellation(copyItems, copyItems, p)
		}},
		{"intersection", func(p *pipelineCancellation) interface{} {
			return apocCollIntersectionWithCancellation(items, items, p)
		}},
		{"subtract", func(p *pipelineCancellation) interface{} { return apocCollSubtractWithCancellation(items, items, p) }},
		{"contains", func(p *pipelineCancellation) interface{} {
			return apocCollContainsWithCancellation(items, int64(-1), p)
		}},
		{"contains_all", func(p *pipelineCancellation) interface{} { return apocCollContainsAllWithCancellation(items, items, p) }},
		{"contains_any", func(p *pipelineCancellation) interface{} {
			return apocCollContainsAnyWithCancellation(items, []interface{}{int64(-1)}, p)
		}},
		{"index_of", func(p *pipelineCancellation) interface{} { return apocCollIndexOfWithCancellation(items, int64(-1), p) }},
		{"split", func(p *pipelineCancellation) interface{} { return apocCollSplitWithCancellation(items, int64(-1), p) }},
		{"partition", func(p *pipelineCancellation) interface{} {
			return apocCollPartitionWithCancellation(items, int64(1), p)
		}},
		{"pairs", func(p *pipelineCancellation) interface{} { return apocCollPairsWithCancellation(items, p) }},
		{"zip", func(p *pipelineCancellation) interface{} { return apocCollZipWithCancellation(items, items, p) }},
		{"frequencies", func(p *pipelineCancellation) interface{} { return apocCollFrequenciesWithCancellation(items, p) }},
		{"occurrences", func(p *pipelineCancellation) interface{} {
			return apocCollOccurrencesWithCancellation(items, int64(-1), p)
		}},
		{"merge", func(p *pipelineCancellation) interface{} { return mergeMapsWithCancellation(fields, fields, p) }},
		{"clean_keys", func(p *pipelineCancellation) interface{} { return cleanMapWithCancellation(fields, items, nil, p) }},
		{"clean_values", func(p *pipelineCancellation) interface{} { return cleanMapWithCancellation(fields, nil, items, p) }},
		{"from_pairs", func(p *pipelineCancellation) interface{} { return fromPairsWithCancellation(pairs, p) }},
		{"from_lists", func(p *pipelineCancellation) interface{} { return fromListsWithCancellation(items, items, p) }},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			healthy := pipelineCancellation{ctx: context.Background()}
			require.Equal(t, entry.run(nil), entry.run(&healthy))
			runPipelineCancelPointStage(t, func(ctx context.Context) error {
				probe := pipelineCancellation{ctx: ctx}
				_ = entry.run(&probe)
				return probe.err
			})
			runPrivacyReaderSamples(t, entry.run)
		})
	}
}

func TestPipelineRegistryReadersShareCancellation(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	fields := map[string]interface{}{}
	for i := 0; i < 1000; i++ {
		fields[fmt.Sprint(i)] = i
	}
	text := strings.Repeat("ΟΣßİé", 1<<17)
	for _, expression := range []string{"keys(fields)", "toUpper(text)", "toLower(text)", "apoc.text.join([text],'')", "apoc.text.join([[fields,text]],',')"} {
		t.Run(expression, func(t *testing.T) {
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &pipelineCountingContext{Context: base, stop: 2}
			probe := &pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
			row := pipelineRow{"fields": fields, "text": text, rowStatementContextKey: context.WithValue(probe.ctx, pipelineCancellationKey{}, probe)}
			_, ok, err := e.evaluateRowValueWithCancellation(expression, row, probe)
			require.False(t, ok)
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, probe.err, context.Canceled)
		})
	}
	// Stateful case conversion must preserve context across chunk boundaries.
	for _, function := range []string{"toUpper", "toLower"} {
		ctx := cypherfn.Context{Eval: func(string) (interface{}, error) { return text, nil }}
		want, found, err := cypherfn.EvaluateFunction(function, []string{"text"}, ctx)
		require.True(t, found)
		require.NoError(t, err)
		probe := pipelineCancellation{ctx: context.Background()}
		ctx.Cancellation = &probe
		got, _, err := cypherfn.EvaluateFunction(function, []string{"text"}, ctx)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

type pipelineLargeGoString string

func (value pipelineLargeGoString) GoString() string { return string(value) }

type pipelineLargeDisplayString string

func (value pipelineLargeDisplayString) String() string { return string(value) }

func TestPipelineDisplayFormattingAndLongKeys(t *testing.T) {
	for _, value := range []interface{}{nil, "text", (*int)(nil), new(int), pipelineLargeDisplayString("display"), []interface{}{"text", int64(2), nil}, []int{1, 2}, map[string]interface{}{"b": []int{2}, "a": "x"}, &struct{ Values []int }{[]int{1, 2}}, struct {
		A string
		B int
	}{"x", 1}} {
		probe := pipelineCancellation{ctx: context.Background()}
		require.Equal(t, fmt.Sprint(value), pipelineDisplayString(value, &probe))
	}
	large := strings.Repeat("x", 1<<20)
	for _, value := range []interface{}{large, []interface{}{large}, map[string]interface{}{"x": large}, pipelineLargeGoString(large)} {
		ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
		probe := pipelineCancellation{ctx: ctx}
		_ = cypherEquivalenceKeyWithCancellation(value, &probe)
		require.ErrorIs(t, probe.err, context.Canceled)
	}
	for _, phase := range []string{"hash", "collision"} {
		t.Run(phase, func(t *testing.T) {
			probe := pipelineCancellation{ctx: context.Background()}
			set := pipelineStringSet{probe: &probe}
			require.False(t, set.add(large))
			require.True(t, set.add(large))
			ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
			probe.ctx = ctx
			probe.remaining = 0
			if phase == "collision" {
				probe.remaining = 0
				ctx.stop = 4
			}
			_ = set.add(large)
			require.ErrorIs(t, probe.err, context.Canceled)
			require.Equal(t, 1, set.next)
		})
	}
}

func TestPipelineCancelledReadersAvoidWholeInputAllocation(t *testing.T) {
	typed := make([]int, 1<<20)
	items := make([]interface{}, 1<<20)
	words := make([]string, 1<<20)
	large := pipelineLargeGoString(strings.Repeat("x", 1<<20))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, run := range []func(*pipelineCancellation){
		func(p *pipelineCancellation) { _ = toAnySliceWithCancellation(typed, p) },
		func(p *pipelineCancellation) { _, _ = evaluateListAccessFunctionWithCancellation("reverse", items, p) },
		func(p *pipelineCancellation) { _ = apocCollReverseWithCancellation(items, p) },
		func(p *pipelineCancellation) { _ = joinPipelineValues(words, ",", p) },
		func(p *pipelineCancellation) { _ = cypherEquivalenceKeyWithCancellation(large, p) },
	} {
		probe := pipelineCancellation{ctx: ctx}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		run(&probe)
		runtime.ReadMemStats(&after)
		require.ErrorIs(t, probe.err, context.Canceled)
		require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "cancelled reader allocated an input-sized scratch buffer")
	}
}

func TestPipelineWholeValueFunctionDeadlines(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	items := make([]interface{}, 16<<20)
	fields := make(map[string]interface{}, 200000)
	for i := 0; i < 200000; i++ {
		fields[fmt.Sprint(i)] = i
	}
	row := pipelineRow{"items": items, "fields": fields, "text": strings.Repeat("ΟΣßİé ", 1<<21)}
	for _, expression := range []string{
		"apoc.coll.reverse(items)", "apoc.coll.flatten([items],true)", "apoc.coll.toSet(items)",
		"apoc.coll.sum(items)", "apoc.coll.avg(items)", "apoc.coll.min(items)", "apoc.coll.max(items)",
		"keys(fields)", "toUpper(text)", "toLower(text)", "apoc.text.join([items],'')",
	} {
		t.Run(expression, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(withExpressionFailureSlot(context.Background()), 20*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			done := make(chan bool, 1)
			var id atomic.Uint64
			go func() {
				id.Store(pipelineCurrentGoroutine())
				_, ok := e.evaluateRowExpressionWithContext(ctx, expression, row)
				done <- ok
			}()
			select {
			case ok := <-done:
				require.False(t, ok, "fixture must reach its deadline inside the large value reader")
				require.ErrorIs(t, getExpressionFailure(ctx), context.DeadlineExceeded)
				require.Less(t, time.Since(deadline), 250*time.Millisecond)
				t.Logf("expression=%s deadline=%s overdue=%s", expression, deadline.Format(time.RFC3339Nano), time.Since(deadline))
			case <-time.After(time.Second):
				t.Fatal("value function did not observe deadline")
			}
			assertPipelineQueryExited(t, id.Load())
		})
	}
}
