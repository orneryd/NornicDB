package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Union by rank attaches the lower-ranked root under the higher one from
// either side. The hub is created last: in creation order the first leaf
// becomes the root, and each later leaf joins as the lower-ranked side.
// Repeating covers any order storage lists the nodes in.
func TestComputeWCCUnionByRank(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (a:W {id: 1}), (b:W {id: 2}), (c:W {id: 3}), (d:W {id: 4}), (e:W {id: 5})
		CREATE (h:W {id: 0}), (h)-[:R]->(a), (h)-[:R]->(b), (h)-[:R]->(c), (h)-[:R]->(d), (h)-[:R]->(e), (:W {id: 9})`, nil)
	require.NoError(t, err)
	for i := 0; i < 32; i++ {
		components := exec.computeWCC("W")
		require.Len(t, components, 7)
		ids := map[int]int{}
		for _, id := range components {
			ids[id]++
		}
		require.Len(t, ids, 2)
	}
}
