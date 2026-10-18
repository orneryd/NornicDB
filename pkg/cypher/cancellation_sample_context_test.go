package cypher

import (
	"context"
	"sync"
	"time"
)

// queryCacheSampleContext is a context whose Err ends the statement on its
// stop-th call, for tests that end a statement at an exact unit of work.
type queryCacheSampleContext struct {
	context.Context
	mu          sync.Mutex
	cancel      context.CancelFunc
	calls, stop int
	cancelled   time.Time
}

func (ctx *queryCacheSampleContext) Err() error {
	ctx.mu.Lock()
	ctx.calls++
	stop := ctx.calls == ctx.stop
	if stop {
		ctx.cancelled = time.Now()
	}
	ctx.mu.Unlock()
	if stop {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func (ctx *queryCacheSampleContext) receipt() (int, time.Time) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.calls, ctx.cancelled
}
