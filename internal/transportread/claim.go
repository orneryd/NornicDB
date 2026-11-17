// Package transportread marks first-party serialization boundaries. The next
// public Execute may borrow certified immutable cache rows, retained until the
// HTTP response or Bolt result finishes. The claim is consumed before callbacks;
// ordinary and nested Go callers keep independently owned results.
package transportread

import "context"

// claimKey carries a first-party transport's claim for the next statement.
type claimKey struct{}

// Claim marks the next public Execute for serializer delivery. Its caller must
// not modify returned row values.
func Claim(ctx context.Context) context.Context {
	return context.WithValue(ctx, claimKey{}, true)
}

// Take clears the claim before execution so nested Go callbacks cannot inherit
// borrowing. It returns the cleared context and whether a claim was present.
func Take(ctx context.Context) (context.Context, bool) {
	claimed, _ := ctx.Value(claimKey{}).(bool)
	if !claimed {
		return ctx, false
	}
	return context.WithValue(ctx, claimKey{}, false), true
}
