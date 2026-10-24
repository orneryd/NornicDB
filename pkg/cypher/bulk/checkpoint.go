package bulk

import "context"

// Checkpoint is the cancellation owner's one check. A pass over data-sized
// work makes a check per step of work (an element, a byte, a comparison);
// once per Stride checks it observes the statement, and a canceled or expired
// statement's failure is recorded (SetStopRecorder) and the work stops:
// Check unwinds with Stopped to the boundary (RecoverStop), which returns the
// error; Observe returns it, for a pass that returns errors itself. Nothing
// the stopped pass built reaches a caller. A checkpoint without a statement
// (nil, or a nil context) never stops.
type Checkpoint struct {
	ctx context.Context
	// single marks a pass over one row: its producer observes the
	// statement, so the checkpoint never does.
	single bool
	index  int
}

// NewCheckpoint observes ctx from the pass's first step.
func NewCheckpoint(ctx context.Context) Checkpoint { return Checkpoint{ctx: ctx} }

// RowsCheckpoint observes ctx over a pass of rows rows: none for a single row,
// whose producer observes it.
func RowsCheckpoint(ctx context.Context, rows int) Checkpoint {
	return Checkpoint{ctx: ctx, single: rows == 1}
}

// WalkCheckpoint observes ctx across one recursive walk of a value: its first
// observation comes after a stride of values.
func WalkCheckpoint(ctx context.Context) *Checkpoint { return &Checkpoint{ctx: ctx, index: 1} }

// Context is the statement the checkpoint observes (Background without one).
func (c *Checkpoint) Context() context.Context {
	if c == nil || c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// Steps is the checks made so far (a walk's starts at one).
func (c *Checkpoint) Steps() int { return c.index }

// due steps the checkpoint: true at a stride boundary.
func (c *Checkpoint) due() bool {
	index := c.index
	c.index++
	return !c.single && index&(Stride-1) == 0
}

// Check is the owner's check. It inlines to a counter step; the observation
// is out of line (stop).
func (c *Checkpoint) Check() {
	if c != nil {
		probeCheck(c)
		if c.due() {
			c.stop()
		}
	}
}

// stop is Check's observation at a stride boundary.
//
//go:noinline
func (c *Checkpoint) stop() {
	if err := c.failure(); err != nil {
		panic(Stopped{Err: err})
	}
}

// Observe is Check for a pass that returns the statement's error itself.
func (c *Checkpoint) Observe() error {
	if c == nil {
		return nil
	}
	probeCheck(c)
	if !c.due() {
		return nil
	}
	return c.failure()
}

// ObserveSteps is Observe for one step that is n steps of work (a storage
// item read and decoded): the checkpoint observes the statement once per
// Stride steps.
func (c *Checkpoint) ObserveSteps(n int) error {
	if c == nil {
		return nil
	}
	probeCheck(c)
	if !c.advance(n) {
		return nil
	}
	return c.failure()
}

// advance steps the checkpoint n steps: true when they reach or cross a
// stride boundary.
func (c *Checkpoint) advance(n int) bool {
	before := c.index
	c.index += max(n, 1)
	return !c.single && (before&(Stride-1) == 0 || before/Stride != (c.index-1)/Stride)
}

// Err is Observe without recording the failure (SetStopRecorder), for a pass
// that holds the lock the recorder takes: its caller records the error once
// the lock is released.
func (c *Checkpoint) Err() error {
	if c == nil {
		return nil
	}
	probeCheck(c)
	if !c.due() || c.ctx == nil {
		return nil
	}
	return c.ctx.Err()
}

// failure is the statement's error, recorded once it is canceled or expired.
func (c *Checkpoint) failure() error {
	if c.ctx == nil {
		return nil
	}
	err := c.ctx.Err()
	if err != nil && stopRecorder != nil {
		stopRecorder(c.ctx, err)
	}
	return err
}

// stopRecorder records a statement's failure where its outcome reads it
// (package cypher's expression failure), set once at init.
var stopRecorder func(context.Context, error)

// SetStopRecorder sets where a checkpoint records the failure it stops on.
func SetStopRecorder(record func(context.Context, error)) { stopRecorder = record }
