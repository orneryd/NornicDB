package storage

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMVCCNodeVersionKeyOrdersByCommitVersion(t *testing.T) {
	t.Parallel()

	// Post-refactor version keys use 8-byte numeric IDs; the sort check
	// only depends on the version suffix, so any fixed numID works.
	const numID uint64 = 42
	base := time.Unix(1700000000, 0).UTC()

	earlier := MVCCVersion{CommitTimestamp: base, CommitSequence: 1}
	sameTimeLaterSeq := MVCCVersion{CommitTimestamp: base, CommitSequence: 2}
	later := MVCCVersion{CommitTimestamp: base.Add(time.Second), CommitSequence: 1}

	require.Less(t, bytes.Compare(mvccNodeVersionKey(numID, earlier), mvccNodeVersionKey(numID, sameTimeLaterSeq)), 0)
	require.Less(t, bytes.Compare(mvccNodeVersionKey(numID, sameTimeLaterSeq), mvccNodeVersionKey(numID, later)), 0)

	decoded, err := decodeMVCCSortVersion(encodeMVCCSortVersion(later))
	require.NoError(t, err)
	require.Equal(t, 0, later.Compare(decoded))
}

func TestBadgerTransaction_AssignsMonotonicMVCCCommitVersions(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	tx1, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx1.CreateNode(&Node{ID: NodeID("nornic:node-1"), Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, tx1.Commit())
	require.False(t, tx1.CommitVersion.IsZero())

	persistedSeq, err := engine.loadPersistedMVCCSequence()
	require.NoError(t, err)
	require.Equal(t, tx1.CommitVersion.CommitSequence, persistedSeq)

	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx2.CreateNode(&Node{ID: NodeID("nornic:node-2"), Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, tx2.Commit())
	require.Equal(t, uint64(1), tx2.CommitVersion.CommitSequence-tx1.CommitVersion.CommitSequence)
	require.Equal(t, 1, tx2.CommitVersion.Compare(tx1.CommitVersion))

	tx3, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx3.Commit())
	require.True(t, tx3.CommitVersion.IsZero())

	persistedAfterEmpty, err := engine.loadPersistedMVCCSequence()
	require.NoError(t, err)
	require.Equal(t, tx2.CommitVersion.CommitSequence, persistedAfterEmpty)

	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	defer reopened.Close()

	reopenedSeq, err := reopened.loadPersistedMVCCSequence()
	require.NoError(t, err)
	require.Equal(t, tx2.CommitVersion.CommitSequence, reopenedSeq)

	tx4, err := reopened.BeginTransaction()
	require.NoError(t, err)
	_, err = tx4.CreateNode(&Node{ID: NodeID("nornic:node-3"), Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, tx4.Commit())
	require.Equal(t, tx2.CommitVersion.CommitSequence+1, tx4.CommitVersion.CommitSequence)
}
