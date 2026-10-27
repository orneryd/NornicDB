// Package contextslot owns lazy, type-scoped state shared by nested operations.
package contextslot

import "context"

type key[T any] struct{}

// Get returns the state for T, or nil if that operation has not initialized it.
// For example, Get[StatementState](ctx) observes the enclosing statement's state.
func Get[T any](ctx context.Context) *T {
	value, _ := ctx.Value(key[T]{}).(*T)
	return value
}

// Ensure initializes T once for this context lineage, preserving an existing
// state and the parent's cancellation and values. Each distinct T has its own
// slot. For example, ctx = Ensure[StatementState](ctx) before invoking a subquery.
func Ensure[T any](ctx context.Context) context.Context {
	if Get[T](ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, key[T]{}, new(T))
}
