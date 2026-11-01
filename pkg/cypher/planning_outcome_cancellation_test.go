// Author: #1615 implementation lane (Codex).
// Author of retained-input integration: Codex R17 implementation agent, 30 October 2026.
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Schedule the real context's cancellation inside the actual planning producer.
// This boundary probe cannot pass because of public Execute's finalizer.
type planningOutcomeContext struct {
	context.Context
	once    sync.Once
	action  func()
	reached bool
}

func (ctx *planningOutcomeContext) Err() error {
	if err := ctx.Context.Err(); err != nil {
		return err
	}
	var pcs [80]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	cache, pipeline := false, false
	for {
		frame, more := frames.Next()
		cache = cache || strings.HasSuffix(frame.Function, ".cachedPipelineClauses")
		pipeline = pipeline || strings.HasSuffix(frame.Function, ".(*StorageExecutor).executePipeline")
		if !more {
			break
		}
	}
	if cache && pipeline {
		ctx.once.Do(func() { ctx.reached = true; ctx.action() })
	}
	return ctx.Context.Err()
}

func TestPlanningCancellationDispatchOutcome(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			name := "cold/cancel"
			if warm {
				name = "warm/cancel"
			}
			if deadline {
				name = strings.TrimSuffix(name, "cancel") + "deadline"
			}
			t.Run(name, func(t *testing.T) {
				query := "MATCH (n:`planning-" + name + "`) RETURN count(n) AS c"
				if warm {
					require.True(t, cachedPipelineTextClauses(context.Background(), query, true).ok)
				}
				parent, cancel := context.WithCancel(context.Background())
				action := cancel
				want := context.Canceled
				if deadline {
					cancel()
					parent, cancel = context.WithTimeout(context.Background(), time.Second)
					action = func() { <-parent.Done() }
					want = context.DeadlineExceeded
				}
				defer cancel()
				ctx := &planningOutcomeContext{Context: parent, action: action}
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "planning-outcome"))
				outcome := exec.executePipeline(ctx, query)
				require.True(t, ctx.reached, "producer must observe the original context")
				require.ErrorIs(t, parent.Err(), want)
				require.True(t, outcome.terminal(), "cancellation cannot become not-applicable")
				require.ErrorIs(t, outcome.err, want)
				require.Nil(t, outcome.result)
				// The interrupted attempt cannot poison the reusable source product.
				recovered, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, recovered.Rows)
			})
		}
	}
}

func TestUnsupportedPlanningRetainsFirstReturn(t *testing.T) {
	const query = "USE system RETURN 1 AS retained"
	split := cachedPipelineTextClauses(context.Background(), query, true)
	require.False(t, split.ok)
	require.Equal(t, "RETURN 1 AS retained", split.firstReturn.text)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "planning-outcome"))
	result, err := exec.executeRequiredPipeline(context.Background(), query)
	require.Nil(t, result)
	var semantic *SemanticError
	require.ErrorAs(t, err, &semantic)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semantic.Code)
}
