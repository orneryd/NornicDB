// Author: Codex, NOR-293 revalidation lane, 29 October 2026.
// Test-only overlay for unchanged release cd67390c0; not a product fix.
package cypher

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor293PlanningContext schedules real cancellation at an existing Err call.
// It delegates Deadline, Done, Value and error identity to a standard context.
// No fake error, product hook or replacement planner is installed. The stack
// condition prevents earlier admission readers from consuming the schedule.
type nor293PlanningContext struct {
	context.Context
	action func()
	once   sync.Once
	frames []string
}

func (c *nor293PlanningContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	var pcs [96]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var names []string
	var cache, pipeline, public bool
	for {
		frame, more := frames.Next()
		names = append(names, frame.Function)
		cache = cache || strings.HasSuffix(frame.Function, ".cachedPipelineClauses")
		pipeline = pipeline || strings.HasSuffix(frame.Function, ".(*StorageExecutor).executePipeline")
		public = public || strings.HasSuffix(frame.Function, ".(*StorageExecutor).Execute")
		if !more {
			break
		}
	}
	if cache && pipeline && public {
		c.once.Do(func() {
			c.frames = names
			c.action()
		})
	}
	return c.Context.Err()
}

func TestNOR293PublicPlanningCancellation(t *testing.T) {
	const query = "MATCH p = ANY SHORTEST (a)(()-[:T]->()){1,1000000}(b) RETURN count(p)"
	// Establish that the same public query is valid and empty when not cancelled.
	control := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor293-control"))
	result, err := control.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)

	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor293-"+mode))
			var parent context.Context
			var cancel context.CancelFunc
			var action func()
			var want error
			if mode == "cancel" {
				parent, cancel = context.WithCancel(context.Background())
				action = cancel
				want = context.Canceled
			} else {
				// Wait at the identified planning observation for a real timer
				// deadline; errors before that observation fail the reach check.
				parent, cancel = context.WithTimeout(context.Background(), time.Second)
				action = func() { <-parent.Done() }
				want = context.DeadlineExceeded
			}
			defer cancel()
			ctx := &nor293PlanningContext{Context: parent, action: action}
			got, execErr := e.Execute(ctx, query, nil)
			require.NotEmpty(t, ctx.frames, "public execution did not reach the scheduled planning observation")
			require.ErrorIs(t, parent.Err(), want, "standard parent context must really be cancelled")
			t.Logf("NOR293 mode=%s parent=%v error_type=%T error=%v cancellation=%v result=%v frames=%v", mode, parent.Err(), execErr, execErr, errors.Is(execErr, want), got, ctx.frames)
			require.ErrorIs(t, execErr, want, "public planning must preserve cancellation identity")
		})
	}
}
