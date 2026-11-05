package multidb

import (
	"sort"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sortedLabels copies and sorts a label slice for order-independent comparison.
func sortedLabels(aliases []string) []string {
	out := append([]string(nil), aliases...)
	sort.Strings(out)
	return out
}

// TestLabelRoutingRouteQueryConsumerMatrix pins every consumer policy of
// multidb.LabelRouting.RouteQuery after the shared label table migration: no
// labels and any unknown label are a nil full scan, an all-known mapping is
// returned even when it is empty, and no property fallback is introduced.
func TestLabelRoutingRouteQueryConsumerMatrix(t *testing.T) {
	routing := NewLabelRouting()
	routing.SetLabelRouting("Person", []string{"db2", "db1", "db1"})
	routing.SetLabelRouting("Order", []string{"db3"})
	routing.SetLabelRouting("Empty", []string{})

	require.Nil(t, routing.RouteQuery(&QueryInfo{}), "no labels must stay a nil full scan")
	require.Nil(t, routing.RouteQuery(&QueryInfo{Labels: []string{}}), "empty labels must stay a nil full scan")
	require.Nil(t, routing.RouteQuery(&QueryInfo{Labels: []string{"Unknown"}}), "unknown label must stay a nil full scan")
	require.Nil(t, routing.RouteQuery(&QueryInfo{Labels: []string{"Person", "Unknown"}}), "unknown after known must stay a nil full scan")
	require.Nil(t, routing.RouteQuery(&QueryInfo{Labels: []string{"Unknown", "Person"}}), "unknown before known must stay a nil full scan")

	assert.Equal(t, []string{"db1", "db2"}, sortedLabels(routing.RouteQuery(&QueryInfo{Labels: []string{"person"}})), "one known label must deduplicate its aliases")
	assert.Equal(t, []string{"db1", "db2", "db3"}, sortedLabels(routing.RouteQuery(&QueryInfo{Labels: []string{"PERSON", "order"}})), "multiple known labels must return their union")

	empty := routing.RouteQuery(&QueryInfo{Labels: []string{"Empty"}})
	require.NotNil(t, empty, "an all-known empty mapping must stay a non-nil empty result")
	assert.Empty(t, empty)

	noPropertyFallback := routing.RouteQuery(&QueryInfo{Labels: []string{"Unknown"}, Properties: map[string]interface{}{"database_id": "db9"}})
	require.Nil(t, noPropertyFallback, "label routing must not consult properties")
}

// TestLabelRoutingRouteWriteConsumerPolicy pins write selection: with no labels
// the route is ambiguous, otherwise the first label and its first configured
// alias win and a configured empty slice selects nothing.
func TestLabelRoutingRouteWriteConsumerPolicy(t *testing.T) {
	routing := NewLabelRouting()
	routing.SetLabelRouting("Person", []string{"db2", "db1"})
	routing.SetLabelRouting("Empty", []string{})

	assert.Empty(t, routing.RouteWrite("create", nil, nil))
	assert.Empty(t, routing.RouteWrite("create", []string{}, nil))
	assert.Equal(t, "db2", routing.RouteWrite("create", []string{"PERSON"}, nil), "first configured alias must win")
	assert.Equal(t, "db2", routing.RouteWrite("create", []string{"person", "Empty"}, nil), "the first label must decide")
	assert.Empty(t, routing.RouteWrite("create", []string{"Empty"}, nil), "a configured empty slice must route nowhere")
	assert.Empty(t, routing.RouteWrite("create", []string{"Unknown"}, nil))
	assert.Empty(t, routing.RouteWrite("create", []string{"Unknown"}, map[string]interface{}{"database_id": "db1"}), "write label routing must not consult properties")
}

// TestLabelRoutingUnicodeAndSliceAliasBacking keeps the existing name-folding
// result and records that the setter stores the caller slice as configured.
func TestLabelRoutingUnicodeAndSliceAliasBacking(t *testing.T) {
	routing := NewLabelRouting()
	aliases := []string{"db1"}
	routing.SetLabelRouting("M\\u00e9tier", aliases)
	assert.Equal(t, []string{"db1"}, routing.RouteQuery(&QueryInfo{Labels: []string{"M\\u00e9TIER"}}))
	aliases[0] = "db9"
	assert.Equal(t, "db9", routing.RouteWrite("create", []string{"m\\u00e9tier"}, nil), "the configured slice must stay assigned, not copied")
}

// TestLabelRoutingPublicAccessIsRaceFree exercises the existing public setters and
// readers concurrently so the owner lock boundary is proved, not assumed.
func TestLabelRoutingPublicAccessIsRaceFree(t *testing.T) {
	routing := NewLabelRouting()
	const workers = 8
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			alias := []string{"db1"}
			if worker%2 == 0 {
				alias = []string{"db2"}
			}
			routing.SetLabelRouting("Person", alias)
			_ = routing.RouteQuery(&QueryInfo{Labels: []string{"person"}})
			_ = routing.RouteWrite("create", []string{"Person"}, nil)
		}(worker)
	}
	group.Wait()
	routed := routing.RouteQuery(&QueryInfo{Labels: []string{"person"}})
	require.Len(t, routed, 1, "the last completed write must decide the configured aliases")
	assert.Contains(t, []string{"db1", "db2"}, routed[0])
}

// TestLabelRoutingNormalizationMatchesSharedOwner proves the consumer key is the
// existing name-folding owner and not a private copy of that policy.
func TestLabelRoutingNormalizationMatchesSharedOwner(t *testing.T) {
	routing := NewLabelRouting()
	routing.SetLabelRouting("Person", []string{"db1"})
	for _, label := range []string{"Person", "PERSON", "person", "pErSoN"} {
		require.Equal(t, cyphertext.NameLower(label), cyphertext.NameLower("Person"))
		assert.Equal(t, []string{"db1"}, routing.RouteQuery(&QueryInfo{Labels: []string{label}}))
	}
}
