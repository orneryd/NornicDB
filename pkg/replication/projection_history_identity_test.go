package replication

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProjectionReopenRejectsUnreadableTransactionIdentity(t *testing.T) {
	base, fixture := newDatabaseScopedFixture(t)
	require.NoError(t, fixture.adapter.wal.Append(storage.OperationType("replication_command"), replicationWALRecord{Command: &Command{Type: CmdCommitTransaction, RequestID: "unreadable", Data: []byte("not a transaction"), Timestamp: time.Now()}}))
	require.NoError(t, fixture.adapter.Close())
	reopened, err := NewStorageAdapterWithWAL(base, fixture.walDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Error(t, reopened.projectionLoadErr)
	_, err = reopened.ApplyCommand(t.Context(), databaseScopedCommand(t, "alpha", "new", "must-not-apply"), WALPublished)
	require.Error(t, err)
	_, err = base.GetStoredNode(t.Context(), "alpha:must-not-apply")
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestProjectionDirectRequestAndTransactionIdentityRemainDistinct(t *testing.T) {
	_, fixture := newDatabaseScopedFixture(t)
	payload, err := storage.EncodeCommandPayload(struct{ Prefix string }{Prefix: "unused:"})
	require.NoError(t, err)
	direct := &Command{Type: CmdDeleteByPrefix, RequestID: "shared", Data: payload, Timestamp: time.Now()}
	transaction := databaseScopedCommand(t, "alpha", "shared", "n")
	for range 2 {
		for _, cmd := range []*Command{direct, transaction} {
			_, err := fixture.adapter.ApplyCommand(t.Context(), cmd, WALPublished)
			require.NoError(t, err)
		}
	}
	assertNOR291ProjectionCount(t, fixture.adapter, 2)
}
