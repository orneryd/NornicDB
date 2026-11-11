package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// NOR-412: appendDecidingLargeRecord with two records: one whose payload is
// 4,096 bytes short of a frame, and one whose payload is a frame long, so that
// the record does not fit one frame. A journal that writes large records
// appends the first without asking its caller. For the second it asks: when the
// caller refuses, the caller's error is returned inside the journal's record
// rejection (the type the frame refusal has) and the journal holds no further
// record; when the caller agrees, the record is written and reads back as one
// record. A journal that writes standard records appends the first, refuses the
// second with the frame error, and asks about neither.
func TestJournalAsksBeforeARecordOverOneFrame(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	open := func(policy WALRecordPolicy) *WAL {
		cfg := DefaultWALConfig()
		cfg.Dir = t.TempDir()
		cfg.SyncMode = "immediate"
		cfg.BatchSyncInterval = time.Hour
		cfg.RecordPolicy = policy
		w, err := NewWAL(cfg.Dir, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	records := func(w *WAL) (commits, largest int) {
		entries, err := ReadWALEntriesFromDir(context.Background(), w.config.Dir)
		require.NoError(t, err)
		for _, entry := range entries {
			if entry.Operation == OpEntityCommit {
				commits++
				largest = max(largest, len(entry.Data))
			}
		}
		return commits, largest
	}
	fits := walEntityCommitData{Payload: make([]byte, int(walMaxEntrySize)-4096)}
	over := walEntityCommitData{Payload: make([]byte, int(walMaxEntrySize))}
	refusal := errors.New("the caller refuses")

	large := open(WALRecordsLarge)
	asked := 0
	decide := func(answer error) func() error {
		return func() error { asked++; return answer }
	}
	_, err := large.appendDecidingLargeRecord(OpEntityCommit, fits, "test", decide(refusal))
	require.NoError(t, err, "a record that fits one frame is appended")
	require.Zero(t, asked, "without the question")
	commits, _ := records(large)
	require.Equal(t, 1, commits)

	_, err = large.appendDecidingLargeRecord(OpEntityCommit, over, "test", decide(refusal))
	require.ErrorIs(t, err, refusal, "a record over one frame is refused with the caller's answer")
	var rejection *walRecordRejection
	require.ErrorAs(t, err, &rejection, "inside the journal's record rejection")
	require.Equal(t, 1, asked)
	commits, _ = records(large)
	require.Equal(t, 1, commits, "the journal holds no record of the refused append")

	_, err = large.appendDecidingLargeRecord(OpEntityCommit, over, "test", decide(nil))
	require.NoError(t, err, "and written when the caller agrees")
	require.Equal(t, 2, asked)
	commits, largest := records(large)
	require.Equal(t, 2, commits)
	require.Greater(t, largest, int(walMaxEntrySize)-8, "it reads back as one record of at least a frame less 8 bytes")

	// A journal that writes standard records appends the first and refuses the
	// second with the frame error; the caller is asked about neither.
	standard := open(WALRecordsStandard)
	asked = 0
	_, err = standard.appendDecidingLargeRecord(OpEntityCommit, fits, "test", decide(nil))
	require.NoError(t, err)
	_, err = standard.appendDecidingLargeRecord(OpEntityCommit, over, "test", decide(nil))
	require.ErrorContains(t, err, "entry exceeds maximum WAL frame size")
	require.ErrorAs(t, err, &rejection)
	require.Zero(t, asked)
}
