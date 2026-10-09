package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// SET counts a null for a key the entity lacks when the key name is known in
// the database, in a map and in a run of more than one assignment; a lone
// assignment never counts it. A rolled-back write makes its names known. The
// counts are Neo4j 5.26.30's (#907).
func TestSetCountsNullsForKnownKeyNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "set_key_names"))
	ctx := context.Background()
	propertiesSet := func(query string) int {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Stats.PropertiesSet
	}
	_, err := exec.Execute(ctx, "CREATE (:T {h: 1})-[:R]->(:T)", nil)
	require.NoError(t, err)
	match := "MATCH (n:T {h: 1}) "

	// Never used: no null counts.
	require.Equal(t, 0, propertiesSet(match+"SET n += {k: null}"))
	require.Equal(t, 0, propertiesSet(match+"SET n.k = null, n.k2 = null"))
	// A null write makes no name known.
	require.Equal(t, 0, propertiesSet(match+"SET n += {k: null}"))

	// A rolled-back write makes k and k2 known.
	require.Equal(t, 2, propertiesSet(match+"SET n.k = 1, n.k2 = 1"))
	for query, count := range map[string]int{
		"SET n.k = null":                       0,
		"SET n += {k: null}":                   1,
		"SET n.k = null, n.y = 1":              2,
		"SET n.y = 1, n.k = null":              2,
		"SET n.k = null, n.k2 = null":          2,
		"SET n.k = null, n.never = null":       1,
		"SET n.never = null, n.never2 = null":  0,
		"SET n.k = null SET n.y = 1":           1,
		"SET n.k = null, n:L":                  0,
		"SET n:L, n.k = null":                  0,
		"SET n.k = null, n.h = 2":              2,
		"SET n.k = null, n.k = null":           1,
		"SET n.k = null, n += {}":              0,
		"SET n = {h: 1, k: null}":              2,
		"SET n += {k: null, never: null}":      1,
		"SET n.k = null, n.k2 = 5":             2,
	} {
		require.Equal(t, count, propertiesSet(match+query), query)
	}
	require.Equal(t, 1, propertiesSet("MATCH ()-[r:R]->() SET r += {k: null}"))
	require.Equal(t, 2, propertiesSet("MATCH ()-[r:R]->() SET r.k = null, r.k2 = null"))
	require.Equal(t, 2, propertiesSet("MERGE (m:T {h: 1}) ON MATCH SET m.x = m.h + 1, m.k = m.missing"))
}
