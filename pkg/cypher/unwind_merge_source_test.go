package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// Author: #1663 lane. An equal accepted cache key is a Source recipe, not the
// identity of its first statement. Each hit retains the current original map.
func TestUnwindMergeCacheRebindsOriginalMapRecord(t *testing.T) {
	executor := &StorageExecutor{}
	const mutation = `MERGE (n:Item {id: row.id})`
	var first *cyphertext.Source
	for _, prefix := range []string{`UNWIND [] AS row `, `WITH '\u03bb' AS unused UNWIND [] AS row `} {
		query := prefix + mutation
		root := originalBraceView(query)
		view := root.window(len(root.text())-len(mutation), len(root.text()))
		plan := executor.cachedUnwindMergeChainPlan(t.Context(), view)
		require.True(t, plan.supported)
		propertyMap := plan.steps[0].node.matchAssignments[0].mapInput
		require.Same(t, root.source.OriginalView(), propertyMap.source.OriginalView())
		entries, valid := propertyMap.keyedEntries()
		require.True(t, valid)
		require.Len(t, entries, 1)
		origin := propertyMap.source.Origin(cyphertext.Span{End: len(propertyMap.text())})
		require.False(t, origin.Generated)
		start := strings.Index(query, `{id: row.id}`)
		require.Equal(t, cyphertext.Span{Start: start, End: start + len(`{id: row.id}`)}, origin.Original)
		if first == nil {
			first = root.source.OriginalView()
		} else {
			require.NotSame(t, first, propertyMap.source.OriginalView())
		}
	}
	prototype := executor.unwindMergeChainPlanCache.plans[mutation]
	require.Same(t, first, prototype.steps[0].node.matchAssignments[0].mapInput.source.OriginalView())
}
