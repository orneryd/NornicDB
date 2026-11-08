package replication

import (
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPreparedTransactionsWithSharedLogicalIDReplicateBothDatabases(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	leader := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: time.Second})
	for _, namespace := range []string{"alpha", "beta"} {
		tx, err := base.BeginTransaction(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace(namespace))
		_, err = tx.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(namespace + ":n"), Properties: map[string]any{"database": namespace}})
		require.NoError(t, err)
		prepared, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "shared"})
		require.NoError(t, err)
		t.Cleanup(prepared.Abort)
		receipt, err := leader.engine.SubmitEntityCommit(t.Context(), prepared, nil)
		require.NoError(t, err)
		require.NoError(t, receipt.Wait(t.Context()))
	}
	require.NoError(t, leader.adapter.FlushWAL())
	entries, err := leader.adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	peerBase := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	peer := setupTestAdapter(t, adapterFixtureOptions{backing: peerBase})
	last, err := NewWALApplier(peer.adapter).ApplyBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, entries[1].Position, last)
	for _, namespace := range []string{"alpha", "beta"} {
		node, err := storage.GetStoredNode(t.Context(), peer.adapter.engine, storage.NodeID(namespace+":n"))
		require.NoError(t, err)
		require.Equal(t, namespace, node.Properties["database"])
	}
}
