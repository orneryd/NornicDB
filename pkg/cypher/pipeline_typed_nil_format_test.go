package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var pipelineNilFormatCalls atomic.Int64

type pipelineNilStringer struct{}

func (*pipelineNilStringer) String() string {
	pipelineNilFormatCalls.Add(1)
	panic("nil String callback")
}

type pipelineNilGoStringer struct{}

func (*pipelineNilGoStringer) GoString() string {
	pipelineNilFormatCalls.Add(1)
	panic("nil GoString callback")
}

type pipelineNilFormatter struct{}

func (*pipelineNilFormatter) Format(fmt.State, rune) {
	pipelineNilFormatCalls.Add(1)
	panic("nil Format callback")
}

type pipelineNilError struct{}

func (*pipelineNilError) Error() string { pipelineNilFormatCalls.Add(1); panic("nil Error callback") }

func pipelineTypedNilValues() []interface{} {
	return []interface{}{(*pipelineNilStringer)(nil), (*pipelineNilGoStringer)(nil), (*pipelineNilFormatter)(nil), (*pipelineNilError)(nil)}
}

func TestPipelineTypedNilCustomFormatting(t *testing.T) {
	pipelineNilFormatCalls.Store(0)
	for _, value := range pipelineTypedNilValues() {
		for _, checked := range []bool{false, true} {
			ctx, cancel := context.WithCancel(withExpressionFailureSlot(context.Background()))
			var p *pipelineCancellation
			if checked {
				p = &pipelineCancellation{ctx: ctx}
			}
			var out strings.Builder
			appendPipelineDisplayValue(&out, value, p)
			require.Equal(t, "<nil>", out.String(), reflect.TypeOf(value).String())
			out.Reset()
			appendEquivalenceGoValue(&out, reflect.ValueOf(value), p, false)
			require.Equal(t, "("+reflect.TypeOf(value).String()+")(nil)", out.String())
			require.NoError(t, getExpressionFailure(ctx))
			if p != nil {
				require.NoError(t, p.err)
			}
			require.False(t, hasUnboundedPipelineFormatter(value, false))
			require.False(t, hasUnboundedPipelineFormatter(value, true))
			require.Zero(t, pipelineNilFormatCalls.Load())
			cancel()
		}
	}
}

func TestPipelineTypedNilRegisteredExecute(t *testing.T) {
	pipelineNilFormatCalls.Store(0)
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "privacy_typed_nil"))
	values := pipelineTypedNilValues()
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		for index, value := range values {
			if name == fmt.Sprintf("privacy.typednil%d", index) {
				return func() interface{} { return value }, true
			}
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { PluginFunctionLookup = previous })
	for index := range values {
		function := fmt.Sprintf("privacy.typednil%d()", index)
		for _, route := range []string{"join", "distinct", "group"} {
			t.Run(fmt.Sprintf("%d/%s", index, route), func(t *testing.T) {
				query := "WITH " + function + " AS x RETURN apoc.text.join([x], ',') AS joined"
				if route == "distinct" {
					query = "UNWIND [" + function + "," + function + "] AS x WITH DISTINCT x RETURN count(*) AS n"
				}
				if route == "group" {
					query = "UNWIND [" + function + "," + function + "] AS x RETURN x, count(*) AS n"
				}
				ctx, cancel := context.WithCancel(withExpressionFailureSlot(context.Background()))
				defer cancel()
				result, err := executor.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.NoError(t, getExpressionFailure(ctx))
				require.Len(t, result.Rows, 1)
				switch route {
				case "join":
					require.Equal(t, "<nil>", result.Rows[0][0])
				case "distinct":
					require.Equal(t, int64(1), result.Rows[0][0])
				case "group":
					require.Equal(t, int64(2), result.Rows[0][1])
					value := result.Rows[0][0]
					require.True(t, value == nil || reflect.ValueOf(value).IsNil())
				}
				require.Zero(t, pipelineNilFormatCalls.Load())
			})
		}
	}
}
