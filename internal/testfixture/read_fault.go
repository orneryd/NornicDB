package testfixture

import "context"

// ReadWithFailure is the one owner of the injected-read-failure contract shared
// by test doubles: an injected fault returns that exact error and the zero value
// of T without invoking read, without a retry or result transformation. With no fault it returns read's own
// result and error unchanged and hands read the caller's ctx, so underlying
// errors and results keep their identity. For example:
//
//	return testfixture.ReadWithFailure(ctx, e.allNodesErr, e.Engine.AllNodes)
//
// The double keeps its own fields, counters and arguments; this helper decides
// only what a selected fault returns.
func ReadWithFailure[T any](ctx context.Context, fault error, read func(context.Context) (T, error)) (T, error) {
	var zero T
	if fault != nil {
		return zero, fault
	}
	return read(ctx)
}
