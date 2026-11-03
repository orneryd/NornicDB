# Work admission and shutdown completion

`lifecycle.WorkLifetime` owns admitted work and one shutdown result. Request
contexts control request work; a shutdown caller's context controls only how
long that caller waits. Canceling a waiter does not dispose resources underneath
accepted work.

`BeginStop(dispose)` seals admission, signals the owned work to stop, waits for
its tickets to return, and runs the first disposer exactly once. Use
`WaitStopped(ctx)` for a bounded wait or `JoinStopped()` to wait for actual
retirement without a caller deadline. Both report the same retained outcomes.
`JoinStopped` does not initiate shutdown; it can also join a stop another owner
has already initiated.

An owning component's unconditional `Close` uses the existing completion,
without inventing a cancellation context:

```go
func (c *component) Close() error {
    c.work.BeginStop(c.closeNative)
    return c.work.JoinStopped()
}
```

A supervisor can call `BeginStop` on every sibling before joining any of them.
For a bounded caller, `Shutdown(ctx, dispose)` still starts the same protocol
and waits using the caller's budget. A later caller can join its actual
completion after that budget expires.

`Context()` is the owned **work** context, canceled when stopping begins. Passing
it to `WaitStopped` can end the wait before retirement; use `JoinStopped` when
actual completion is required. An active worker must return its ticket before
joining its own lifetime.

`OnceCompletion` uses the same distinction: `Do` executes the winning operation
in its caller's frame and joins completion; `Go` dispatches it; `DoContext`
dispatches it and bounds only the caller's wait. The operation retains the
context its own owner supplied. Repeated calls do not repeat the operation.
