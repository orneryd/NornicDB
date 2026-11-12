package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPreparedNodeIDListingSurvivesValueCopies(t *testing.T) {
	source := []storage.NodeID{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"}
	for _, length := range []int{0, 1, 8, 9, 17} {
		var built nodeIDResults
		for _, id := range source[:length] {
			built.add(id)
		}
		prepared := built
		// Reusing the builder must not replace a copied plan's inline IDs.
		built = nodeIDResults{}
		built.add("replacement")
		require.Equal(t, length, prepared.len())
		require.Equal(t, source[:length], prepared.ids())
		require.Equal(t, []storage.NodeID{"replacement"}, built.ids())
	}
	original := singleIDResult("original")
	copied := original
	copied.add("next")
	require.Equal(t, []storage.NodeID{"original"}, original.ids())
	require.Equal(t, []storage.NodeID{"original", "next"}, copied.ids())
}
