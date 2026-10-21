package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

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
	require.NoError(t, engine.FlushWriteBehind())
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
	buffer.drainOne()
	require.ErrorIs(t, buffer.LastErr(), applyErr)
	require.EqualValues(t, 1024, policy.Reserved(), "failed replay retains its payload")
	applyErr = nil
	buffer.drainOne()
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
