package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// More stale markers than one Badger batch can delete sort before a live
// node. Discovery must commit its cleanup batch by batch and reach the live
// node in one call; aborting the whole cleanup would rebuild the same stale
// prefix on every call and strand the live node behind it.
func TestPendingCandidatesStaleMarkerCleanupSpansBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("writes more pending markers than one Badger batch holds")
	}
	// The low-memory profile holds about 13,000 entries per batch.
	const stale = 30000
	staleID := func(i int) NodeID { return NodeID(fmt.Sprintf("test:a-%06d", i)) }
	for _, kind := range []string{"missing", "excluded"} {
		t.Run(kind, func(t *testing.T) {
			e := openLargeCommitTestEngine(t, t.TempDir())
			e.SetEmbeddingsEnabled(true)
			switch kind {
			case "missing":
				// Durable markers whose node bodies are gone.
				for start := 0; start < stale; start += 5000 {
					require.NoError(t, e.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
						for i := start; i < start+5000; i++ {
							if err := e.setPendingEmbeddingInTxn(txn, staleID(i)); err != nil {
								return err
							}
						}
						return nil
					}))
				}
			case "excluded":
				// A pending backlog whose label is excluded afterwards.
				nodes := make([]*Node, stale)
				for i := range nodes {
					nodes[i] = &Node{ID: staleID(i), Labels: []string{"Old"}, Properties: map[string]any{"content": "old"}}
				}
				require.NoError(t, e.BulkCreateNodes(nodes))
			}
			live := &Node{ID: "test:z-live", Labels: []string{"Doc"}, Properties: map[string]any{"content": "live"}}
			_, err := e.CreateNode(live)
			require.NoError(t, err)
			require.Equal(t, stale+1, e.PendingEmbeddingsCount())
			if kind == "excluded" {
				e.SetEmbeddingLabelPolicy("test", nil, []string{"Old"})
			}

			// The fixture must exceed one batch: deleting every stale marker in
			// one transaction does not fit (and is rolled back).
			err = e.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
				for i := 0; i < stale; i++ {
					if err := txn.Delete(pendingEmbedKey(staleID(i))); err != nil {
						return err
					}
				}
				return nil
			})
			require.True(t, IsTransactionTooBig(err), "fixture must need more than one cleanup batch, got %v", err)
			require.Equal(t, stale+1, e.PendingEmbeddingsCount())

			node := e.FindNodeNeedingEmbedding()
			require.NotNil(t, node, "cleanup larger than one batch must still reach the live node")
			require.Equal(t, live.ID, node.ID)
			require.Equal(t, 1, e.PendingEmbeddingsCount(), "every stale marker is durably removed")

			e.MarkNodeEmbedded(node.ID)
			require.Nil(t, e.FindNodeNeedingEmbedding())
			require.Zero(t, e.PendingEmbeddingsCount())
		})
	}
}
