package replication

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicationRecordMeasurementMatchesJournalEncoding(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, {0}, {1, 2}, {1, 2, 3}, bytes.Repeat([]byte{255}, 1024)} {
		cmd := &Command{Type: CmdCommitTransaction, Data: payload, Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 17, time.UTC), RequestID: "quoted\" <& \u2603"}
		record := replicationWALRecord{Timestamp: cmd.Timestamp.UnixNano(), Command: cmd}
		size, err := record.encodedSize()
		require.NoError(t, err)
		var buffer bytes.Buffer
		require.NoError(t, valueconvert.PropertyJSON.NewEncoder(&buffer).Encode(record))
		require.Equal(t, buffer.Len()-1, size)
		require.Equal(t, payload, cmd.Data, "measurement never mutates accepted bytes")
	}
}

func TestAcceptedCommandCapacityRejectsBeforeQueueOwnership(t *testing.T) {
	engine, adapter := wireFixture(t, false)
	cmd := &Command{Type: CmdCommitTransaction, Data: make([]byte, 12*1024*1024), Timestamp: time.Now()}
	require.ErrorContains(t, adapter.checkCommandCapacity(t.Context(), cmd), "maximum WAL frame size")
	before := adapter.wal.Sequence()
	var ran atomic.Bool
	var lifetime lifecycle.WorkLifetime
	_, err := submitAcceptedByApply(context.Background(), adapter, cmd, nil, func() error { return nil }, &lifetime, func(context.Context, *Command, time.Duration) error { ran.Store(true); return nil }, 0)
	require.ErrorContains(t, err, "maximum WAL frame size")
	require.False(t, ran.Load())
	require.Equal(t, before, adapter.wal.Sequence())
	require.Empty(t, adapter.walQueue)
	_ = engine
}

func TestRaftCapacityRefusalNeverEntersApplyQueue(t *testing.T) {
	engine, adapter := wireFixture(t, false)
	r, _ := newCoverageRaftReplicator(t)
	r.storage = adapter
	r.started.Store(true)
	r.state = StateLeader
	r.currentTerm = 1
	t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
	// The accepted handle only supplies admission identity here; rejection must
	// happen before work enrollment or an application can inspect it.
	cmd := &Command{Type: CmdCommitTransaction, Data: make([]byte, 12*1024*1024), Timestamp: time.Now(), accepted: &storage.AcceptedEntityCommitHandle{}}
	receipt, err := r.SubmitAcceptedCommand(t.Context(), cmd, nil)
	require.Nil(t, receipt)
	require.ErrorContains(t, err, "maximum WAL frame size")
	require.Empty(t, r.applyCh)
	_ = engine
}

func TestCommandCapacityAdmissionDoesNotCopyFittingPayload(t *testing.T) {
	_, adapter := wireFixture(t, false)
	cmd := &Command{Type: CmdCommitTransaction, Data: make([]byte, 8*1024*1024), Timestamp: time.Now(), RequestID: "accepted-id"}
	ctx := t.Context()
	allocations := testing.AllocsPerRun(100, func() {
		if err := adapter.checkCommandCapacity(ctx, cmd); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocations, "ordinary admission must only measure retained bytes, not encode them again")
}

func TestCommandCapacityRejectsInvalidTimestampBeforeAcceptance(t *testing.T) {
	_, adapter := wireFixture(t, false)
	cmd := &Command{Type: CmdCommitTransaction, Data: []byte("small"), Timestamp: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}
	require.Error(t, adapter.checkCommandCapacity(t.Context(), cmd))
}
