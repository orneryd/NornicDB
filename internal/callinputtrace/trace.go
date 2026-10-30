// Package callinputtrace observes CALL consumer inputs for contract checks.
// It never selects inputs or changes the binding program.
package callinputtrace

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

type observerKey struct{}

// Enabled lets consumers avoid collecting diagnostic names during normal work.
func Enabled(ctx context.Context) bool { return ctx.Value(observerKey{}) != nil }

// WithObserver records the names actually handed to a consumer, alongside the
// canonical branch that consumer is processing. Observers must be safe for
// concurrent calls and must not modify the supplied statement.
func WithObserver(ctx context.Context, observe func(string, resultmetadata.Statement, []string)) context.Context {
	return context.WithValue(ctx, observerKey{}, observe)
}

// Observe reports a consumer's selected names, when a contract observer exists.
func Observe(ctx context.Context, consumer string, branch resultmetadata.Statement, names []string) {
	if observe, ok := ctx.Value(observerKey{}).(func(string, resultmetadata.Statement, []string)); ok {
		observe(consumer, branch, append([]string(nil), names...))
	}
}
