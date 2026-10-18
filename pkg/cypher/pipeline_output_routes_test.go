package cypher

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
)

// A one-byte value must not copy a native, accumulated megabyte prefix
// before observing the statement's existing cancellation latch.
func TestPipelineOutputFormatterOldPrefixCancellation(t *testing.T) {
	for _, value := range []interface{}{"x", nil, true, int64(1), []int{1}, map[string]int{"x": 1}, struct{ X int }{1}, (*int)(nil)} {
		for _, goSyntax := range []bool{false, true} {
			var out strings.Builder
			out.Grow(1 << 20)
			out.WriteString(strings.Repeat("p", out.Cap()))
			ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
			// Leave enough work for container-key preparation, then stop
			// during the substantially larger old output-prefix copy.
			p := &pipelineCancellation{ctx: withExpressionFailureSlot(ctx), remaining: 64}
			appendPipelineFormatValue(&out, reflect.ValueOf(value), p, false, goSyntax)
			require.ErrorIs(t, p.err, context.Canceled)
			require.ErrorIs(t, getExpressionFailure(p.ctx), context.Canceled)
			require.LessOrEqual(t, out.Len(), 63*4096, "cancel inside old-prefix copy: %T / goSyntax=%v", value, goSyntax)
		}
	}
	old := make([]interface{}, 1<<18)
	ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
	p := &pipelineCancellation{ctx: ctx, remaining: 8}
	require.Nil(t, collFlattenWithCancellation(old, []interface{}{int64(1)}, 0, p))
	require.ErrorIs(t, p.err, context.Canceled)
	require.Nil(t, old[len(old)-1], "cancelled growth must not mutate the input prefix")
}

func TestPipelineOutputFormatterReplay(t *testing.T) {
	integer := 1
	values := []interface{}{
		nil, "\"é\n\xff", true, int64(-1), uint64(2), 1.25, complex(1, 2),
		(*int)(nil), &integer, &struct{ X []int }{[]int{1, 2}},
		[]interface{}{nil, "x", []int{1, 2}}, []int(nil), map[string]interface{}(nil),
		map[string]interface{}{"b": []int{1}, "a": "x"},
		struct {
			A string
			b int
			c uint
			d float32
			e complex64
			f bool
			g chan int
			h func()
		}{"x", -1, 2, 1.5, complex(1, 2), true, nil, nil},
	}
	for _, value := range values {
		for _, goSyntax := range []bool{false, true} {
			run := func(p *pipelineCancellation) string {
				var out strings.Builder
				appendPipelineFormatValue(&out, reflect.ValueOf(value), p, false, goSyntax)
				return out.String()
			}
			want := fmt.Sprint(value)
			if goSyntax {
				want = fmt.Sprintf("%#v", value)
			}
			p := &pipelineCancellation{ctx: context.Background()}
			require.Equal(t, want, run(p), "%T / goSyntax=%v", value, goSyntax)
			replayPrepassWork(t, func(p *pipelineCancellation) { _ = run(p) })
		}
	}
	for _, value := range []interface{}{true, false, []byte{1, 2}, int32(-1), uint32(2), 1.5, math.Inf(1), 2.0, math.MaxFloat64, math.NaN(), []interface{}{true, nil}, map[string]interface{}{"x": 1}, strings.Repeat("x", 4097), struct{ X int }{1}} {
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = cypherEquivalenceKeyWithCancellation(value, p) })
		replayPrepassWork(t, func(p *pipelineCancellation) { var out strings.Builder; appendEquivalenceFallback(&out, value, p) })
	}
}

func TestPipelineOutputTypedConversionAndUnicodeGrowth(t *testing.T) {
	count := 513
	for _, value := range []interface{}{
		make([]string, count), make([]int, count), make([]int64, count), make([]float32, count),
		make([]float64, count), make([]bool, count), make([]map[string]interface{}, count), make([]uint16, count),
	} {
		p := &pipelineCancellation{ctx: context.Background()}
		require.Equal(t, toAnySlice(value), toAnySliceWithCancellation(value, p))
		runPrivacyReaderSamples(t, func(p *pipelineCancellation) interface{} { return toAnySliceWithCancellation(value, p) })
	}
	text := strings.Repeat("é😀", 257)
	runes := []rune(text)
	for _, run := range []func(*pipelineCancellation) interface{}{
		func(p *pipelineCancellation) interface{} { return pipelineRunes(text, p) },
		func(p *pipelineCancellation) interface{} { value, _ := pipelineStringFromRunes(runes, p); return value },
		func(p *pipelineCancellation) interface{} {
			value, _ := padRowStringWithCancellation(text, 1025, "é😀", false, p)
			return value
		},
		func(p *pipelineCancellation) interface{} {
			value, _ := padRowStringWithCancellation(text, 1025, "é😀", true, p)
			return value
		},
	} {
		p := &pipelineCancellation{ctx: context.Background()}
		require.Equal(t, run(nil), run(p))
		runPrivacyReaderSamples(t, run)
	}
}

var pipelineOutputBenchmarkSink interface{}

func TestPipelineOutputCollectionBranchesReplay(t *testing.T) {
	for _, run := range []func(*pipelineCancellation){
		func(p *pipelineCancellation) { _ = flattenListWithCancellation(int64(1), true, p) },
		func(p *pipelineCancellation) {
			_ = apocCollSubtractWithCancellation([]interface{}{int64(1), int64(2)}, []interface{}{int64(3)}, p)
		},
		func(p *pipelineCancellation) {
			_ = apocCollSplitWithCancellation([]interface{}{int64(1), int64(2), int64(1), int64(2)}, int64(2), p)
		},
		func(p *pipelineCancellation) {
			_, _ = evaluateListAccessFunctionWithCancellation("reverse", []interface{}{1, 2}, p)
		},
		func(p *pipelineCancellation) {
			_ = (CypherVector{Type: VectorInteger64, Ints: []int64{1, 2}}).coordinatesWithCancellation(p)
		},
		func(p *pipelineCancellation) {
			_ = (CypherVector{Type: VectorInteger64, Ints: []int64{1, 2}}).stringWithCancellation(p)
		},
		func(p *pipelineCancellation) {
			_, _ = fnCollInsert(cypherfn.Context{Cancellation: p, Eval: func(s string) (interface{}, error) {
				switch s {
				case "items":
					return []interface{}{1, 2}, nil
				case "index":
					return int64(1), nil
				default:
					return 3, nil
				}
			}}, []string{"items", "index", "value"})
		},
		func(p *pipelineCancellation) { _, _ = normalizeLiteralTextWithCancellation("RETURN $x", p) },
	} {
		replayPrepassWork(t, run)
	}
}

func BenchmarkPipelineOutputGrowth(b *testing.B) {
	items := make([]string, 4097)
	for index := range items {
		items[index] = "é😀"
	}
	for _, checked := range []bool{false, true} {
		name := "nil"
		if checked {
			name = "checked"
		}
		b.Run(name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				var p *pipelineCancellation
				if checked {
					p = &pipelineCancellation{ctx: ctx}
				}
				pipelineOutputBenchmarkSink = toAnySliceWithCancellation(items, p)
			}
		})
	}
}
