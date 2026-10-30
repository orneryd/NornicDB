package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedBufferedCommitCapturesStoredImagesOnce(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
	_, err := b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	var reads atomic.Int64
	var observed []StoredEntityChange
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(event StoredEntityPublication) (func(), error) {
			observed = event.Changes
			return func() {}, nil
		}, nil
	}, storedReadTestBoundary{read: func(kind StoredEntityReadKind, id string, read func() error) error {
		if kind == StoredEntityNodePriorRead {
			reads.Add(1)
		}
		return read()
	}}))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:a", Properties: map[string]any{"v": int64(2)}}, Policy: ClientNodeEdits}))
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "capture-once"})
	require.NoError(t, err)
	defer p.Abort()
	require.Equal(t, int64(1), reads.Load())
	public := p.Changes()
	require.Len(t, public, 1)
	public[0].AfterNode.Properties["v"] = int64(99)
	require.NoError(t, p.Commit(nil))
	require.Nil(t, tx.preparedEntityCommit, "closed transactions must release prepared image retention")
	require.Equal(t, int64(1), reads.Load(), "native continuation must reuse its immutable prepared image pairs")
	require.Len(t, observed, 1)
	require.Equal(t, int64(1), observed[0].BeforeNode.Properties["v"])
	require.Equal(t, int64(2), observed[0].AfterNode.Properties["v"])
	actual, err := b.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(2), actual.Properties["v"])
}
