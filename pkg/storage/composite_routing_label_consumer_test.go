package storage

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sortedAliases copies and sorts a route result for order-independent comparison.
func sortedAliases(aliases []string) []string {
	out := append([]string(nil), aliases...)
	sort.Strings(out)
	return out
}

// TestQueryAnalyzerRouteQueryLabelConsumerMatrix pins the storage consumer policy
// after the shared label table migration: only a complete nonempty label
// mapping wins, otherwise the existing property lookup followed by the caller
// slice decides.
func TestQueryAnalyzerRouteQueryLabelConsumerMatrix(t *testing.T) {
	all := []string{"dbA", "dbB", "dbC"}
	a := NewQueryAnalyzer()
	a.SetLabelRouting("Person", []string{"dbA", "dbB", "dbB"})
	a.SetLabelRouting("Company", []string{"dbC"})
	a.SetLabelRouting("Empty", []string{})

	assert.Equal(t, all, a.RouteQuery(&QueryInfo{Labels: nil}, all), "no labels must return the caller slice")
	assert.Equal(t, all, a.RouteQuery(&QueryInfo{Labels: []string{}}, all), "empty labels must return the caller slice")
	assert.Equal(t, all, a.RouteQuery(&QueryInfo{Labels: []string{"Unknown"}}, all), "unknown label must return the caller slice")
	assert.Equal(t, all, a.RouteQuery(&QueryInfo{Labels: []string{"Person", "Unknown"}}, all), "unknown after known must abandon the partial union")
	assert.Equal(t, all, a.RouteQuery(&QueryInfo{Labels: []string{"Empty"}}, all), "an all-known empty mapping must not win")

	assert.Equal(t, []string{"dbA", "dbB"}, sortedAliases(a.RouteQuery(&QueryInfo{Labels: []string{"person"}}, all)), "one known label must deduplicate its aliases")
	assert.Equal(t, []string{"dbA", "dbB", "dbC"}, sortedAliases(a.RouteQuery(&QueryInfo{Labels: []string{"PERSON", "company"}}, all)), "multiple known labels must return their union")
}

// TestQueryAnalyzerRouteQueryPropertyFallbackAfterLabelRouting pins the existing
// property fallback and caller-slice default that label routing must not disturb.
func TestQueryAnalyzerRouteQueryPropertyFallbackAfterLabelRouting(t *testing.T) {
	all := []string{"dbA", "dbB", "dbDefault"}
	a := NewQueryAnalyzer()
	a.SetLabelRouting("Person", []string{"dbA"})
	a.SetLabelRouting("Empty", []string{})
	a.SetPropertyRouting("tenant", "a", "dbA")
	a.SetPropertyRouting("tenant", "b", "dbB")
	a.SetPropertyDefault("tenant", "dbDefault")

	// A complete empty label mapping falls through to property routing.
	assert.Equal(t, []string{"dbB"}, a.RouteQuery(&QueryInfo{Labels: []string{"Empty"}, Properties: map[string]interface{}{"tenant": "b"}}, all))
	// An unknown label also falls through to property routing.
	assert.Equal(t, []string{"dbA"}, a.RouteQuery(&QueryInfo{Labels: []string{"Unknown"}, Properties: map[string]interface{}{"tenant": "a"}}, all))
	// An unmatched property value uses the configured property default.
	assert.Equal(t, []string{"dbDefault"}, a.RouteQuery(&QueryInfo{Properties: map[string]interface{}{"tenant": "zzz"}}, all))
	// A complete nonempty label mapping still wins over property routing.
	assert.Equal(t, []string{"dbA"}, a.RouteQuery(&QueryInfo{Labels: []string{"person"}, Properties: map[string]interface{}{"tenant": "b"}}, all))
	// No labels and no properties returns the caller slice unchanged.
	assert.Equal(t, all, a.RouteQuery(&QueryInfo{}, all))
	require.Nil(t, a.RouteQuery(&QueryInfo{}, nil), "a nil caller slice stays nil")
}

// TestCompositeEngineLabelRoutingConsumerContract covers the public engine setters,
// the constructor defaults, and the execution-view clone that now shares the same
// label table owner.
func TestCompositeEngineLabelRoutingConsumerContract(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)
	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1, "db2": engine2},
		map[string]string{"db1": "db1", "db2": "db2"},
		map[string]string{"db1": "read_write", "db2": "read"},
	)

	// The constructor auto-routes a writable alias label to its own constituent.
	assert.Equal(t, []string{"db1"}, sortedAliases(composite.labelRouting.Select([]string{"DB1"}).Aliases))
	assert.Equal(t, []string{"db1"}, sortedAliases(composite.labelRouting.Select([]string{"db1"}).Aliases))
	// A read-only alias is not auto-routed.
	assert.Equal(t, 1, len(composite.labelRouting))

	aliases := []string{"db2"}
	composite.SetLabelRouting("Person", aliases)
	// The public setter normalizes once and assigns the caller slice.
	assert.Equal(t, []string{"db2"}, sortedAliases(composite.labelRouting.Select([]string{"person"}).Aliases))
	aliases[0] = "db9"
	assert.Equal(t, []string{"db9"}, sortedAliases(composite.labelRouting.Select([]string{"PERSON"}).Aliases), "engine assignment must not copy the caller slice")
	aliases[0] = "db2"

	// An empty selector result keeps the shared no-label distinction.
	assert.Equal(t, 2, len(composite.labelRouting))

	// The statement view clones label keys but keeps the configured slices shared.
	view := composite.WithConstituentAccess(nil)
	assert.Equal(t, []string{"db2"}, sortedAliases(view.labelRouting.Select([]string{"person"}).Aliases))
	view.SetLabelRouting("Order", []string{"db1"})
	assert.Equal(t, 2, len(composite.labelRouting), "the view must not write new label keys into the source table")
	assert.Equal(t, 3, len(view.labelRouting))
	aliases[0] = "db7"
	assert.Equal(t, []string{"db7"}, sortedAliases(view.labelRouting.Select([]string{"PERSON"}).Aliases), "the view must keep the per-label slice identity")
}
