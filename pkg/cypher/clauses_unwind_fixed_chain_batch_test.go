package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteUnwindFixedChainLinkBatch_SuccessAndIdempotence(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "unwind_fixed_chain_batch_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, key := range []string{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID("orig-" + key), Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"textKey": key}})
		require.NoError(t, err)
		for depth := 1; depth <= 2; depth++ {
			_, err = store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("hop-%s-%d", key, depth)), Labels: []string{"Hop"}, Properties: map[string]interface{}{"hopId": fmt.Sprintf("%s:%d", key, depth)}})
			require.NoError(t, err)
		}
	}

	rest := `
MATCH (o:OriginalText {textKey: row.textKey})
MATCH (h1:Hop {hopId: row.textKey + ':1'})
MATCH (h2:Hop {hopId: row.textKey + ':2'})
MERGE (o)-[:NEXT]->(h1)
MERGE (h1)-[:NEXT]->(h2)
RETURN count(o) AS linked
`
	items := []interface{}{
		map[string]interface{}{"textKey": "a"},
		map[string]interface{}{"textKey": "b"},
	}

	res, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, rest)
	require.NoError(t, err)
	require.True(t, supported)
	require.Equal(t, []string{"linked"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 2, res.Rows[0][0])
	require.EqualValues(t, 4, res.Stats.RelationshipsCreated)

	verify, err := exec.Execute(ctx, "MATCH ()-[r:NEXT]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 4, verify.Rows[0][0])

	// Re-running the same batch should not create duplicates.
	res2, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, rest)
	require.NoError(t, err)
	require.True(t, supported)
	require.EqualValues(t, 2, res2.Rows[0][0])
	require.EqualValues(t, 0, res2.Stats.RelationshipsCreated)
}

func TestExecuteUnwindFixedChainLinkBatch_ElementIDRootPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "unwind_fixed_chain_batch_id_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	rootID, err := store.CreateNode(&storage.Node{ID: storage.NodeID("orig-id"), Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"textKey": "k"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: storage.NodeID("hop-id-1"), Labels: []string{"Hop"}, Properties: map[string]interface{}{"hopId": "k:1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: storage.NodeID("hop-id-2"), Labels: []string{"Hop"}, Properties: map[string]interface{}{"hopId": "k:2"}})
	require.NoError(t, err)

	rest := `
MATCH (o:OriginalText) WHERE elementId(o) = row.root_id
MATCH (h1:Hop {hopId: row.prefix + ':1'})
MATCH (h2:Hop {hopId: row.prefix + ':2'})
MERGE (o)-[:NEXT]->(h1)
MERGE (h1)-[:NEXT]->(h2)
RETURN count(o) AS linked
`
	items := []interface{}{
		map[string]interface{}{"root_id": string(rootID), "prefix": "k"},
	}

	res, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, rest)
	require.NoError(t, err)
	require.True(t, supported)
	require.EqualValues(t, 1, res.Rows[0][0])
	require.EqualValues(t, 2, res.Stats.RelationshipsCreated)
}

func TestExecuteUnwindFixedChainLinkBatch_RejectsUnsupportedShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unwind_fixed_chain_batch_rejects"))
	ctx := context.Background()
	items := []interface{}{map[string]interface{}{"textKey": "a"}}

	_, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, "MATCH (o:OriginalText {textKey: row.textKey})")
	require.NoError(t, err)
	require.False(t, supported)

	_, supported, err = exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, "MATCH (o:OriginalText {textKey: row.textKey}) RETURN count(x) AS c")
	require.NoError(t, err)
	require.False(t, supported)

	// Non-contiguous hop depths (1 and 3) should be rejected.
	restNonContig := `
MATCH (o:OriginalText {textKey: row.textKey})
MATCH (h1:Hop {hopId: row.textKey + ':1'})
MATCH (h3:Hop {hopId: row.textKey + ':3'})
MERGE (o)-[:NEXT]->(h1)
MERGE (h1)-[:NEXT]->(h3)
RETURN count(o) AS linked
`
	_, supported, err = exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, restNonContig)
	require.NoError(t, err)
	require.False(t, supported)

	// Root match must reference the active UNWIND variable.
	restWrongVar := `
MATCH (o:OriginalText {textKey: other.textKey})
MATCH (h1:Hop {hopId: row.textKey + ':1'})
MERGE (o)-[:NEXT]->(h1)
RETURN count(o) AS linked
`
	_, supported, err = exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, restWrongVar)
	require.NoError(t, err)
	require.False(t, supported)
}
