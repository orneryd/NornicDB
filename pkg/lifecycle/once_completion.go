package lifecycle

import (
	"context"
	"fmt"
	"sync"
)

// OnceCompletion publishes one native operation's completion. It caches one
// result; the enclosing lifetime still owns attribution and sibling aggregation.
// The publication fence never spans the operation or a caller's wait.
type OnceCompletion struct {
	init sync.Once
	once sync.Once
	done chan struct{}
	err  error
}

// doneChan exists before the operation starts, so a waiter can join a
// completion that has not been dispatched yet.
func (c *OnceCompletion) doneChan() chan struct{} {
	c.init.Do(func() { c.done = make(chan struct{}) })
	return c.done
}

// Go dispatches the winning operation without waiting and returns the channel
// that closes on its actual completion. Later calls repeat no work.
func (c *OnceCompletion) Go(operation func() error) <-chan struct{} {
	done := c.doneChan()
	c.once.Do(func() { go c.complete(operation) })
	return done
}

// Done closes when the operation has actually completed, including one not yet
// dispatched when Done is called.
func (c *OnceCompletion) Done() <-chan struct{} { return c.doneChan() }

// Err is the operation's result once Done has closed, and nil before.
func (c *OnceCompletion) Err() error {
	select {
	case <-c.doneChan():
		return c.err
	default:
		return nil
	}
}

// Do executes the winning operation in its caller's retained frame and joins
// that same completion for every later caller.
func (c *OnceCompletion) Do(operation func() error) error {
	return c.do(context.Background(), operation, false)
}

// Run joins a void native callback through the same completion owner. The
// callback runs outside the publication fence; its normal outcomes remain with
// its enclosing owner. A previously panicking callback cannot appear successful
// to a later joiner.
func (c *OnceCompletion) Run(operation func()) {
	if err := c.Do(func() error { operation(); return nil }); err != nil {
		panic(err)
	}
}

// DoContext dispatches the winning operation under its completion future. ctx
// bounds only this caller's wait; operation must supply its own live disposal
// context. Later calls observe the same eventual result, without repeating work.
func (c *OnceCompletion) DoContext(ctx context.Context, operation func() error) error {
	requireContext("OnceCompletion.DoContext", ctx)
	return c.do(ctx, operation, true)
}

func (c *OnceCompletion) do(ctx context.Context, operation func() error, dispatch bool) error {
	done := c.doneChan()
	winner := false
	c.once.Do(func() { winner = true })
	if winner {
		if dispatch {
			go c.complete(operation)
		} else {
			c.complete(operation)
		}
	}
	select {
	case <-done:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *OnceCompletion) complete(operation func() error) {
	defer close(c.doneChan())
	defer func() {
		if value := recover(); value != nil {
			c.err = fmt.Errorf("native completion panic: %v", value)
			panic(value) // Preserve the caller/owner's existing panic handling.
		}
	}()
	c.err = operation()
}
