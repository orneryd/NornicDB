package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type pipelineCallbackString struct{}

func (pipelineCallbackString) String() string { panic("unbudgeted String callback") }

type pipelineCallbackGoString struct{}

func (pipelineCallbackGoString) GoString() string { panic("unbudgeted GoString callback") }

type pipelineCallbackFormat struct{}

func (pipelineCallbackFormat) Format(fmt.State, rune) { panic("unbudgeted Format callback") }

type pipelineCallbackError struct{}

func (pipelineCallbackError) Error() string { panic("unbudgeted Error callback") }

func TestPipelineCustomFormattersDeclineBeforeCalling(t *testing.T) {
	for _, value := range []interface{}{pipelineCallbackString{}, pipelineCallbackGoString{}, pipelineCallbackFormat{}, pipelineCallbackError{}} {
		for _, goSyntax := range []bool{false, true} {
			ctx, cancel := context.WithCancel(withExpressionFailureSlot(context.Background()))
			probe := pipelineCancellation{ctx: ctx}
			var out strings.Builder
			if goSyntax {
				appendEquivalenceFallback(&out, []interface{}{value}, &probe)
			} else {
				appendPipelineDisplayValue(&out, map[string]interface{}{"nested": value}, &probe)
			}
			wantError := hasUnboundedPipelineFormatter(value, goSyntax)
			if wantError {
				require.Error(t, probe.err)
				require.Equal(t, probe.err, getExpressionFailure(ctx))
				require.Equal(t, "Neo.ClientError.Statement.TypeError", probe.err.(interface{ BoltErrorCode() string }).BoltErrorCode())
			} else {
				require.NoError(t, probe.err)
			}
			cancel()
		}
	}
}

func TestPipelineCustomFormattingExecutorFailure(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "privacy_callbacks"))
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		if name == "privacy.customformat" {
			return func() interface{} { return pipelineCallbackFormat{} }, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { PluginFunctionLookup = previous })
	for _, query := range []string{
		"WITH privacy.customformat() AS x RETURN apoc.text.join([x], ',')",
		"UNWIND [privacy.customformat(), privacy.customformat()] AS x WITH DISTINCT x RETURN count(*)",
	} {
		ctx, cancel := context.WithCancel(context.Background())
		_, err := e.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "custom formatting is unsupported")
		cancel()
	}
}

func TestPipelineBuiltinFormattingPreservesValues(t *testing.T) {
	instant := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, value := range []interface{}{
		instant, CypherDate{Time: instant}, CypherDateTime{Time: instant},
		CypherLocalDateTime{Time: instant}, CypherTime{Time: instant}, CypherLocalTime{Time: instant},
		CypherPoint{SRID: 7203, X: 1, Y: 2}, CypherUUID{}, &CypherDuration{Days: 1},
		VectorFloat64, CypherVector{Type: VectorInteger64, Ints: []int64{1, 2}},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		probe := pipelineCancellation{ctx: ctx}
		require.Equal(t, fmt.Sprint(value), pipelineDisplayString(value, &probe))
		require.NoError(t, probe.err)
		cancel()
	}
}
