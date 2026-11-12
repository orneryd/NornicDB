package cypher

// The identity routes — an id()/elementId() equality, its IN list and its OR —
// resolve their candidates to node IDs and then admit them with the pattern's
// labels and its inline properties. These are the deterministic same-list and
// different-list controls of that admission on both parsers: the canonical
// MATCH node test compares a list property by value, so the pattern's own list
// admits the node an identity route listed and any other list (the same values
// reordered, or another node's) does not.
//
// TestElementIdentitySeekAdmitsListPropertyByValue
// (id_seek_property_admission_test.go) is the public fail-first probe of the
// equality route, with its independent identity-only and property-only
// controls. The controls here cover the IN list and the compound OR as well;
// the compound OR's rows prove that each listed node is admitted by its own
// list, so a union that lists two nodes answers with the one the pattern's
// property admits.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIdentityRoutesAdmitListPropertyByValue(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "identity_route_admission"), QueryCachePolicy{MaxEntries: 0})
		elementIDs := make([]string, 0, 2)
		for _, properties := range []string{"{name:'one', tags:['a','b']}", "{name:'two', tags:['c','d']}"} {
			created, err := exec.Execute(t.Context(), "CREATE (n:IdentityRoute "+properties+") RETURN elementId(n) AS id", nil)
			require.NoError(t, err, properties)
			require.Len(t, created.Rows, 1, properties)
			id, ok := created.Rows[0][0].(string)
			require.True(t, ok, "elementId(n) is the node's element ID: %s", properties)
			elementIDs = append(elementIDs, id)
		}
		one, two := elementIDs[0], elementIDs[1]

		for _, control := range []struct {
			name   string
			where  string
			params map[string]interface{}
			tags   []interface{}
			want   [][]interface{}
		}{
			{
				name:  "literal identity, the pattern's list",
				where: "elementId(n) = '" + one + "'",
				tags:  []interface{}{"a", "b"},
				want:  [][]interface{}{{"one"}},
			},
			{
				name:  "literal identity, the same values in another order",
				where: "elementId(n) = '" + one + "'",
				tags:  []interface{}{"b", "a"},
			},
			{
				name:   "parameter identity, the pattern's list",
				where:  "elementId(n) = $id",
				params: map[string]interface{}{"id": one},
				tags:   []interface{}{"a", "b"},
				want:   [][]interface{}{{"one"}},
			},
			{
				name:   "parameter identity, the same values in another order",
				where:  "elementId(n) = $id",
				params: map[string]interface{}{"id": one},
				tags:   []interface{}{"b", "a"},
			},
			{
				name:   "identity IN list, the pattern's list",
				where:  "elementId(n) IN [$id]",
				params: map[string]interface{}{"id": one},
				tags:   []interface{}{"a", "b"},
				want:   [][]interface{}{{"one"}},
			},
			{
				name:   "identity IN list, the same values in another order",
				where:  "elementId(n) IN [$id]",
				params: map[string]interface{}{"id": one},
				tags:   []interface{}{"b", "a"},
			},
			{
				name:   "identity compound OR, the pattern's list",
				where:  "elementId(n) = $id OR elementId(n) = $other",
				params: map[string]interface{}{"id": one, "other": two},
				tags:   []interface{}{"a", "b"},
				want:   [][]interface{}{{"one"}},
			},
			{
				name:   "identity compound OR, the same values in another order",
				where:  "elementId(n) = $id OR elementId(n) = $other",
				params: map[string]interface{}{"id": one, "other": two},
				tags:   []interface{}{"b", "a"},
			},
			{
				name:   "identity compound OR, the other listed node's own list",
				where:  "elementId(n) = $id OR elementId(n) = $other",
				params: map[string]interface{}{"id": one, "other": two},
				tags:   []interface{}{"c", "d"},
				want:   [][]interface{}{{"two"}},
			},
		} {
			params := map[string]interface{}{"tags": control.tags}
			for name, value := range control.params {
				params[name] = value
			}
			query := "MATCH (n:IdentityRoute {tags:$tags}) WHERE " + control.where + " RETURN n.name"
			result, err := exec.Execute(t.Context(), query, params)
			require.NoError(t, err, control.name)
			if len(control.want) == 0 {
				require.Empty(t, result.Rows, control.name)
				continue
			}
			require.Equal(t, control.want, result.Rows, control.name)
		}
	})
}
