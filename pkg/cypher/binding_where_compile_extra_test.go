package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCypherWhereStringPredicates_MultiBinding hits the
// compileBindingStringPredicate path via STARTS WITH / ENDS WITH /
// CONTAINS predicates inside a multi-pattern MATCH that compiles
// to the binding-where evaluator.
func TestCypherWhereStringPredicates_MultiBinding(t *testing.T) {
	e := setupTestExecutor(t)
	for _, item := range []struct {
		id    string
		title string
	}{
		{"d-1", "alpha-spec"},
		{"d-2", "beta-spec"},
		{"d-3", "gamma-doc"},
	} {
		createTestNode(t, e, item.id, []string{"Doc"},
			map[string]interface{}{"title": item.title})
	}
	for _, id := range []string{"r-1"} {
		createTestNode(t, e, id, []string{"Reviewer"}, map[string]interface{}{"name": "alice"})
	}

	// STARTS WITH
	res, err := e.Execute(context.Background(),
		"MATCH (d:Doc) MATCH (r:Reviewer) WHERE d.title STARTS WITH 'alpha' RETURN d.title AS t", nil)
	require.NoError(t, err)
	titles := []string{}
	for _, row := range res.Rows {
		titles = append(titles, row[0].(string))
	}
	require.Equal(t, []string{"alpha-spec"}, titles)

	// ENDS WITH
	res, err = e.Execute(context.Background(),
		"MATCH (d:Doc) MATCH (r:Reviewer) WHERE d.title ENDS WITH '-doc' RETURN d.title AS t", nil)
	require.NoError(t, err)
	titles = nil
	for _, row := range res.Rows {
		titles = append(titles, row[0].(string))
	}
	require.Equal(t, []string{"gamma-doc"}, titles)

	// CONTAINS
	res, err = e.Execute(context.Background(),
		"MATCH (d:Doc) MATCH (r:Reviewer) WHERE d.title CONTAINS 'spec' RETURN d.title AS t ORDER BY d.title", nil)
	require.NoError(t, err)
	titles = nil
	for _, row := range res.Rows {
		titles = append(titles, row[0].(string))
	}
	require.Equal(t, []string{"alpha-spec", "beta-spec"}, titles)
}

// TestCypherWhereNullPredicates_MultiBinding hits compileBindingNullPredicate
// via IS NULL and IS NOT NULL across two bound variables. These patterns
// exercise the literal-list-less branches of the predicate compiler.
func TestCypherWhereNullPredicates_MultiBinding(t *testing.T) {
	e := setupTestExecutor(t)
	createTestNode(t, e, "n-with", []string{"Item"}, map[string]interface{}{"sku": "X1"})
	createTestNode(t, e, "n-without", []string{"Item"}, map[string]interface{}{})
	createTestNode(t, e, "p-1", []string{"Person"}, map[string]interface{}{"name": "alice"})

	// IS NOT NULL — only n-with.
	res, err := e.Execute(context.Background(),
		"MATCH (i:Item) MATCH (p:Person) WHERE i.sku IS NOT NULL RETURN i.sku AS s", nil)
	require.NoError(t, err)
	skus := []string{}
	for _, row := range res.Rows {
		skus = append(skus, row[0].(string))
	}
	require.Equal(t, []string{"X1"}, skus)

	// IS NULL — only n-without (its sku is missing → null).
	res, err = e.Execute(context.Background(),
		"MATCH (i:Item) MATCH (p:Person) WHERE i.sku IS NULL RETURN id(i) AS id", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
}

// TestCypherWhereParseBindingLiteralList exercises parseBindingLiteralList
// via WHERE x IN [...] where the list contains strings, ints, mixed types,
// and nested parentheses (which the parser must respect).
func TestCypherWhereParseBindingLiteralList(t *testing.T) {
	e := setupTestExecutor(t)
	createTestNode(t, e, "p-1", []string{"Person"}, map[string]interface{}{"name": "alice", "age": int64(30)})
	createTestNode(t, e, "p-2", []string{"Person"}, map[string]interface{}{"name": "bob", "age": int64(25)})
	createTestNode(t, e, "p-3", []string{"Person"}, map[string]interface{}{"name": "carol", "age": int64(40)})
	createTestNode(t, e, "h-1", []string{"Habitat"}, map[string]interface{}{"loc": "city"})

	// Multi-binding string list: alice and carol match.
	res, err := e.Execute(context.Background(),
		"MATCH (p:Person) MATCH (h:Habitat) WHERE p.name IN ['alice', 'carol'] RETURN p.name AS name ORDER BY p.name", nil)
	require.NoError(t, err)
	got := []string{}
	for _, row := range res.Rows {
		got = append(got, row[0].(string))
	}
	require.Equal(t, []string{"alice", "carol"}, got)

	// Multi-binding integer list with nested parens (calls parseLiteralValue
	// on each item, which the binding compiler delegates to).
	res, err = e.Execute(context.Background(),
		"MATCH (p:Person) MATCH (h:Habitat) WHERE p.age IN [25, 40] RETURN p.name AS name ORDER BY p.name", nil)
	require.NoError(t, err)
	got = nil
	for _, row := range res.Rows {
		got = append(got, row[0].(string))
	}
	require.Equal(t, []string{"bob", "carol"}, got)

	// Empty list — no matches.
	res, err = e.Execute(context.Background(),
		"MATCH (p:Person) MATCH (h:Habitat) WHERE p.name IN [] RETURN p.name AS name", nil)
	require.NoError(t, err)
	require.Empty(t, res.Rows)
}
