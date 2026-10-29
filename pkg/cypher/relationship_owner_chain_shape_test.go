package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A bound COUNT/EXISTS chain body is admitted once per body; each row then
// only traverses from its bound entities.
func TestBoundChainShapeReadOncePerBody(t *testing.T) {
	for body, want := range map[string]boundChainShape{
		"(a)-[:A|B]->(:Q)":                       {pattern: "(a)-[:A|B]->(:Q)", ok: true},
		"MATCH (a)-[:`A|B`]->(q) WHERE q.id > 1": {pattern: "(a)-[:`A|B`]->(q)", where: "q.id > 1", ok: true},
		"(a)-[:A]->(q:Q {id: a.id})":             {},
		"(a)-[:A]->(q), (q)-[:B]->(r)":           {},
		"MATCH (a)-[:A]->(q) RETURN q":           {},
		"MATCH p = (a)-[:A]->(q)":                {},
	} {
		require.Equal(t, want, boundChainShapeFor(t.Context(), body), body)
		require.Zero(t, testing.AllocsPerRun(20, func() { boundChainShapeFor(t.Context(), body) }), body)
	}
}

func TestRelationshipCachedSyntaxUsesContext(t *testing.T) {
	query := "WITH 1 AS `context owner r73` RETURN `context owner r73`"
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	declined, names := canonicalizeQuotedVariables(canceled, query)
	require.Equal(t, query, declined)
	require.Nil(t, names)
	ctx5 := withCypherVersion(t.Context(), "CYPHER 5 "+query)
	ctx25 := withCypherVersion(t.Context(), "CYPHER 25 "+query)
	// ParseSyntax currently shares a grammar across versions. Keep parity with
	// its context-keyed cold read, without inventing a grammar difference.
	for _, ctx := range []context.Context{ctx5, ctx25, ctx5, ctx25} {
		cold, _ := canonicalizeQuotedVariablesWithBindings(ctx, query, nil)
		cached, _ := canonicalizeQuotedVariables(ctx, query)
		require.NotEqual(t, query, cold, "fixture must require a real rewrite")
		require.Equal(t, cold, cached, "canceled parsing poisoned the live quote plan")
	}
	const body = "(a)-[:ContextOwnerR73]->(b)"
	require.False(t, boundChainShapeFor(canceled, body).ok)
	require.True(t, boundChainShapeFor(t.Context(), body).ok, "canceled parsing poisoned the live chain plan")
}
