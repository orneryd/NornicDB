package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestElementIdentitySeekAdmitsListPropertyByValue(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "identity_property_probe"), QueryCachePolicy{MaxEntries: 0})
		created, err := exec.Execute(t.Context(), "CREATE (n:IdentityCandidate {name:'one', tags:['a','b']}) RETURN elementId(n) AS id", nil)
		require.NoError(t, err)
		require.Len(t, created.Rows, 1)
		id := created.Rows[0][0]
		for _, control := range []string{
			"MATCH (n:IdentityCandidate) WHERE elementId(n)=$id RETURN n.name",
			"MATCH (n:IdentityCandidate {tags:$tags}) RETURN n.name",
		} {
			result, err := exec.Execute(t.Context(), control, map[string]interface{}{"tags": []interface{}{"a", "b"}, "id": id})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"one"}}, result.Rows, control)
		}
		for _, tags := range []struct {
			values []interface{}
			want   [][]interface{}
		}{
			{[]interface{}{"a", "b"}, [][]interface{}{{"one"}}},
			{[]interface{}{"b", "a"}, [][]interface{}{}},
		} {
			result, err := exec.Execute(t.Context(), "MATCH (n:IdentityCandidate {tags:$tags}) WHERE elementId(n)=$id RETURN n.name", map[string]interface{}{"tags": tags.values, "id": id})
			require.NoError(t, err)
			if len(tags.want) == 0 {
				require.Empty(t, result.Rows, "a reordered list is not the stored list")
				continue
			}
			require.Equal(t, tags.want, result.Rows)
		}
	})
}
