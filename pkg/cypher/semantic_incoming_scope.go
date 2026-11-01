// Author: Claude R17 implementation agent, 30 October 2026.
package cypher

import (
	"context"
	"strings"
)

// semanticIncomingScopeKey carries the variables a statement's validation
// sees as already bound when it starts: a SHOW command's yielded columns for
// the query after its YIELD. The statement is validated as written; no
// binding clause is composed in front of it.
type semanticIncomingScopeKey struct{}

func withSemanticIncomingScope(ctx context.Context, names []string) context.Context {
	return context.WithValue(ctx, semanticIncomingScopeKey{}, append([]string(nil), names...))
}

// semanticIncomingScope is the incoming variables, nil when there are none.
func semanticIncomingScope(ctx context.Context) []string {
	names, _ := ctx.Value(semanticIncomingScopeKey{}).([]string)
	return names
}

// semanticIncomingCacheKey prefixes a validation cache key with the incoming
// variables, which change the verdict for the same text.
func semanticIncomingCacheKey(ctx context.Context, key string) string {
	if semanticGraphScope(ctx) {
		key = "\x00graph-scope:" + key
	}
	names := semanticIncomingScope(ctx)
	if len(names) == 0 {
		return key
	}
	return "incoming:" + strings.Join(names, "\x00") + "\x01" + key
}

// newIncomingSemanticBindingScope is a statement's initial binding scope.
func newIncomingSemanticBindingScope(ctx context.Context) *semanticBindingScope {
	scope := newSemanticBindingScope()
	for _, name := range semanticIncomingScope(ctx) {
		scope.bind(name)
	}
	return scope
}
