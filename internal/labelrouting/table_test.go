package labelrouting

import (
	"github.com/stretchr/testify/require"
	"maps"
	"testing"
)

func TestLabelSelectionContracts(t *testing.T) {
	table := New()
	table.Set("Person", []string{"db1", "db2"})
	table.Set("Order", []string{"db2", "db3"})
	table.Set("Empty", nil)
	table.Set("EmptyAlias", []string{""})
	for _, test := range []struct {
		name     string
		labels   []string
		kind     SelectionKind
		aliases  []string
		complete bool
	}{
		{"no labels", nil, SelectionNoLabels, nil, false},
		{"empty input", []string{}, SelectionNoLabels, nil, false},
		{"unknown", []string{"Unknown"}, SelectionUnknownLabel, nil, false},
		{"partial mapping", []string{"Person", "Unknown"}, SelectionUnknownLabel, nil, false},
		{"known empty", []string{"Empty"}, SelectionEmptyMapping, []string{}, true},
		{"single label", []string{"pErSoN"}, SelectionMapped, []string{"db1", "db2"}, true},
		{"union", []string{"Person", "Order", "Person"}, SelectionMapped, []string{"db1", "db2", "db3"}, true},
		{"literal empty alias", []string{"EmptyAlias"}, SelectionMapped, []string{""}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := table.Select(test.labels)
			require.Equal(t, test.kind, got.Kind)
			require.Equal(t, test.complete, got.Kind.Complete())
			if test.aliases == nil {
				require.Nil(t, got.Aliases)
			} else {
				require.NotNil(t, got.Aliases)
				require.ElementsMatch(t, test.aliases, got.Aliases)
			}
		})
	}
}

func TestLabelAssignmentAndCloneOwnership(t *testing.T) {
	aliases := []string{"db1", "db2"}
	table := New()
	table.Set("Équipe", aliases)
	got, exists := table.Lookup("éQUIPE")
	require.True(t, exists)
	require.Equal(t, aliases, got)
	require.Equal(t, &aliases[0], &got[0])
	cloned := maps.Clone(table)
	cloned.Set("Order", []string{"db3"})
	_, exists = table.Lookup("Order")
	require.False(t, exists)
	aliases[0] = "changed"
	original, exists := table.Lookup("équipe")
	require.True(t, exists)
	copyAliases, exists := cloned.Lookup("équipe")
	require.True(t, exists)
	require.Equal(t, "changed", original[0])
	require.Equal(t, "changed", copyAliases[0])
	table.Set(`unit\u0041`, []string{"literal"})
	got, exists = table.Lookup(`unit\u0041`)
	require.True(t, exists)
	require.Equal(t, []string{"literal"}, got)
	_, exists = table.Lookup("unitA")
	require.False(t, exists, "runtime names must not undergo source escape decoding")
}
