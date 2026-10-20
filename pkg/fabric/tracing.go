package fabric

import (
	"context"
	"sync/atomic"
)

// HotPathTrace captures per-query Fabric optimization branch usage.
type HotPathTrace struct {
	ApplyBatchedLookupRows atomic.Bool
}

type hotPathTraceKey struct{}

// WithHotPathTrace attaches a mutable trace object to context.
func WithHotPathTrace(ctx context.Context, trace *HotPathTrace) context.Context {
	if trace == nil {
		return ctx
	}
	return context.WithValue(ctx, hotPathTraceKey{}, trace)
}

func hotPathTraceFromContext(ctx context.Context) *HotPathTrace {
	if ctx == nil {
		return nil
	}
	v := ctx.Value(hotPathTraceKey{})
	if v == nil {
		return nil
	}
	trace, _ := v.(*HotPathTrace)
	return trace
}

func markApplyBatchedLookupRows(ctx context.Context) {
	trace := hotPathTraceFromContext(ctx)
	if trace == nil {
		return
	}
	trace.ApplyBatchedLookupRows.Store(true)
}
