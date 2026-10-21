package storage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPendingCandidatesBufferedFlush(t *testing.T) {
	e := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: t.TempDir(), wal: true})
	wal := e.bufferedCommitWAL
	t.Cleanup(func() { _ = e.Close(); _ = wal.Close() })
	e.SetEmbeddingsEnabled(true)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Nil(t, e.FindNodeNeedingEmbedding(), "unflushed marker is not durable work yet")
	require.NoError(t, e.FlushWriteBehind())
	require.Equal(t, 1, e.PendingEmbeddingsCount())
	node := e.FindNodeNeedingEmbedding()
	require.NotNil(t, node, "flush must publish into the loaded candidate cache")
	require.Equal(t, pendingCandidateNode(0).ID, node.ID)
}

func TestPendingCandidatesBufferedWALReplay(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	e.MarkNodeEmbedded(pendingCandidateNode(0).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	payload, err := json.Marshal(WALBufferedCommitData{Namespace: "test", Writes: map[string][]byte{string(pendingEmbedKey(pendingCandidateNode(0).ID)): {}}})
	require.NoError(t, err)
	require.NoError(t, ReplayWALEntry(e, WALEntry{Operation: OpBufferedCommit, Database: "test", Data: payload}))
	require.Equal(t, 1, e.PendingEmbeddingsCount())
	node := e.FindNodeNeedingEmbedding()
	require.NotNil(t, node, "buffered replay must publish into the loaded candidate cache")
	require.Equal(t, pendingCandidateNode(0).ID, node.ID)
}
