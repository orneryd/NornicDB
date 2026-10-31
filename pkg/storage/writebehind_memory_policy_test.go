package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestWriteBehindMemoryAdmissionRejectsBeforeWAL(t *testing.T) {
	policy := memory.NewPolicy(256<<10, 0, time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: t.TempDir(), wal: true})
	wal := engine.bufferedCommitWAL
	engine.memoryPolicy = policy
	t.Cleanup(func() { _ = engine.Close(); _ = wal.Close() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:large", Labels: []string{"L"}, Properties: map[string]any{"payload": strings.Repeat("x", 1<<20)}})
	require.NoError(t, err)
	require.ErrorIs(t, tx.Commit(), memory.ErrLimitExceeded)
	require.Zero(t, policy.Reserved())
	_, err = engine.GetNode("test:large")
	require.ErrorIs(t, err, ErrNotFound)
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotEqual(t, OpBufferedCommit, entry.Operation, "rejected write must not reach replayable WAL")
	}
}

func TestRejectedBufferedCommitClearsDetachedDictionaryEntries(t *testing.T) {
	policy := memory.NewPolicy(32<<10, 0, time.Hour, func() (memory.Sample, error) {
		return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: t.TempDir(), wal: true})
	wal := engine.bufferedCommitWAL
	engine.memoryPolicy = policy
	t.Cleanup(func() { _ = engine.Close(); _ = wal.Close() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	for i := 0; i < 256; i++ {
		_, err = tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:rejected-%03d", i)), Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.ErrorIs(t, tx.Commit(), memory.ErrLimitExceeded)
	for i := 0; i < 256; i++ {
		id := NodeID(fmt.Sprintf("test:rejected-%03d", i))
		_, pending := engine.idDict.nodePendingWriter(id)
		require.False(t, pending, "%s retained a detached dictionary state", id)
	}
	require.Zero(t, policy.Reserved())
}

func TestBufferedMemoryErrorRetainsLocalizationAndCause(t *testing.T) {
	for _, tc := range []struct {
		cause error
		id    localization.MessageID
	}{
		{memory.ErrLimitExceeded, localization.ApplicationMemoryPressure().ID},
		{memory.ErrStaleSample, localization.ApplicationMemoryUnavailable().ID},
	} {
		err := bufferedMemoryError(tc.cause)
		require.ErrorIs(t, err, tc.cause)
		var localized *localization.LocalizedError
		require.ErrorAs(t, err, &localized)
		require.Equal(t, tc.id, localized.Message.ID)
		var coded interface{ BoltErrorCode() string }
		require.ErrorAs(t, err, &coded)
		require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", coded.BoltErrorCode())
	}
}

func TestWriteBehindMemoryReservationEndsAfterFlush(t *testing.T) {
	policy := memory.NewPolicy(8<<20, 0, time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: t.TempDir(), wal: true})
	wal := engine.bufferedCommitWAL
	engine.memoryPolicy = policy
	t.Cleanup(func() { _ = engine.Close(); _ = wal.Close() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:small", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.Positive(t, policy.Reserved(), "acknowledged generation still owns its forecast")
	require.NoError(t, engine.FlushWriteBehind(nil))
	require.Zero(t, policy.Reserved(), "retired generation must release its forecast")
	got, err := engine.GetNode("test:small")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Properties["v"])
}

func TestWriteBehindMemoryReservationSurvivesReplayFailure(t *testing.T) {
	policy := memory.NewPolicy(1<<20, 0, time.Hour, func() (memory.Sample, error) {
		return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	lease, err := policy.Reserve(1024)
	require.NoError(t, err)
	applyErr := errors.New("replay unavailable")
	buffer := NewWriteBehindBuffer(time.Hour, 0, func(*CommitBuffer) error { return applyErr })
	defer buffer.Close()
	buffer.AppendCommit(bufferedCommit{memoryLease: lease, ops: []Operation{{}}})
	buffer.rotate()
	buffer.drainOne(true, nil)
	require.ErrorIs(t, buffer.LastErr(), applyErr)
	require.EqualValues(t, 1024, policy.Reserved(), "failed replay retains its payload")
	applyErr = nil
	buffer.drainOne(true, nil)
	require.NoError(t, buffer.LastErr())
	require.Zero(t, policy.Reserved(), "successful retirement releases the payload")
}

func TestBufferedCommitForecastIncludesRetainedDictionaryState(t *testing.T) {
	base := &bufferedCommit{namespace: "test", txID: "tx"}
	withDictionary := &bufferedCommit{
		namespace: "test", txID: "tx",
		idEntries: &txnCounterState{nodes: []NodeID{"test:node"}, edges: []EdgeID{"test:edge"}},
		propKeyDrain: propKeyTxnDrain{
			counters: map[string]uint64{"test": 2},
			pending:  []propKeyPersistEntry{{namespace: "test", name: "payload", id: 2}},
		},
	}
	require.Greater(t, estimateBufferedCommitBytes(withDictionary), estimateBufferedCommitBytes(base))
}
