package testfixture

import (
	"context"
	"sync/atomic"
)

// CheckpointContext cancels its real context on the selected live Err check.
// It is a deterministic work-boundary fixture, not a wall-clock deadline.
// The observer runs once, after cancellation, so tests can snapshot work.
type CheckpointContext struct {
	context.Context
	Cancel    context.CancelFunc
	remaining atomic.Int64
	checked   atomic.Int64
	observe   func()
}

// NewCheckpointContext creates a real cancelable background context. A positive
// limit selects the cancellation check; nonpositive limits never trigger it.
func NewCheckpointContext(limit int, observe func()) *CheckpointContext {
	ctx, cancel := context.WithCancel(context.Background())
	c := &CheckpointContext{Context: ctx, Cancel: cancel, observe: observe}
	c.remaining.Store(int64(limit))
	return c
}

// Err observes the underlying lifetime and advances only while it is live.
func (c *CheckpointContext) Err() error {
	if c.Context.Err() == nil {
		c.checked.Add(1)
		if c.remaining.Add(-1) == 0 {
			c.Cancel()
			if c.observe != nil {
				c.observe()
			}
		}
	}
	return c.Context.Err()
}

// Checks reports the number of live work-boundary observations.
func (c *CheckpointContext) Checks() int { return int(c.checked.Load()) }
