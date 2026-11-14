package storage

import (
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestRejectedRecordDoesNotHideWriteAfterSnapshotRestart covers the ways the
// journal can refuse a record and the way a snapshot cut can be taken past a
// number that was published but not written:
//
//   - refused_record: the ordinary append of a record that does not fit one
//     frame under the standard policy is refused with the journal's record
//     rejection.
//   - historical_sealed_gap: an older writer sealed a segment through a
//     number it allocated and never wrote.
//   - declined_large_record: a journal that writes large records offers a
//     record that needs more than one frame to its caller, and the caller
//     declines it.
//   - batch_single_oversize: a batch whose only record does not fit one
//     frame fails and writes nothing.
//   - batch_accepted_prefix_then_oversize: a batch writes its first record,
//     refuses its second, reports the refusal, and leaves the first record
//     published, so the snapshot cut covers it.
//
// Every variant acknowledges one or more nodes, refuses the record, takes a
// snapshot at the sequence the journal has published, truncates the journal
// under retention, restarts it, acknowledges one more node, and recovers. The
// refused record may not publish a sequence that the snapshot cut then reuses
// for the node acknowledged after the restart, and every acknowledged node
// must come back from the snapshot and the remaining journal with its value.
func TestRejectedRecordDoesNotHideWriteAfterSnapshotRestart(t *testing.T) {
	// One payload for every variant: 16 MiB of bytes no longer fits one frame
	// once the record's entry header and payload encoding are added.
	oversize := walEntityCommitData{Payload: make([]byte, int(walMaxEntrySize))}

	entriesAt := func(t *testing.T, w *WAL) []WALEntry {
		t.Helper()
		require.NoError(t, w.Sync())
		entries, err := ReadWALEntriesFromDir(t.Context(), w.config.Dir)
		require.NoError(t, err)
		return entries
	}
	sequencesAt := func(t *testing.T, w *WAL) []uint64 {
		t.Helper()
		var out []uint64
		for _, entry := range entriesAt(t, w) {
			out = append(out, entry.Sequence)
		}
		return out
	}

	variants := []struct {
		name   string
		policy WALRecordPolicy
		// refuse drives the refusal and checks what it wrote and published.
		refuse func(t *testing.T, w *WAL, oversize walEntityCommitData)
	}{
		{
			name:   "refused_record",
			policy: WALRecordsStandard,
			refuse: func(t *testing.T, w *WAL, oversize walEntityCommitData) {
				require.Error(t, w.AppendWithDatabase(OpEntityCommit, oversize, "test"))
			},
		},
		{
			name:   "historical_sealed_gap",
			policy: WALRecordsStandard,
			refuse: func(t *testing.T, w *WAL, oversize walEntityCommitData) {
				require.Error(t, w.AppendWithDatabase(OpEntityCommit, oversize, "test"))
				// Simulate the prior writer sealing an allocated but unwritten sequence.
				w.sequence.Add(1)
			},
		},
		{
			name:   "declined_large_record",
			policy: WALRecordsLarge,
			refuse: func(t *testing.T, w *WAL, oversize walEntityCommitData) {
				declined := errors.New("the caller declines the large record")
				_, err := w.appendDecidingLargeRecord(OpEntityCommit, oversize, "test", func() error { return declined })
				require.ErrorIs(t, err, declined, "the caller's decline is the refusal's cause")
				var rejection *walRecordRejection
				require.ErrorAs(t, err, &rejection, "the decline is the journal's record rejection")
				require.EqualValues(t, 1, w.Sequence(), "the declined record publishes no sequence")
				require.Equal(t, []uint64{1}, sequencesAt(t, w), "the declined record is not written")
			},
		},
		{
			name:   "buffered_missing_dictionary",
			policy: WALRecordsStandard,
			refuse: func(t *testing.T, w *WAL, _ walEntityCommitData) {
				_, err := w.appendWithDatabaseForcedFlush(OpBufferedCommit, &walBufferedCommitPayload{Namespace: "test"}, "test", nil)
				require.ErrorContains(t, err, "property dictionary is required")
				require.EqualValues(t, 1, w.Sequence())
				require.Equal(t, []uint64{1}, sequencesAt(t, w))
			},
		},
		{
			name:   "buffered_incompressible_frame",
			policy: WALRecordsStandard,
			refuse: func(t *testing.T, w *WAL, _ walEntityCommitData) {
				value := make([]byte, int(walMaxEntrySize)+4096)
				_, err := rand.New(rand.NewSource(42)).Read(value)
				require.NoError(t, err)
				_, err = w.appendWithDatabaseForcedFlush(OpBufferedCommit, &walBufferedCommitPayload{Namespace: "test", Writes: map[string][]byte{"incompressible": value}}, "test", nil)
				require.ErrorContains(t, err, "exceeds maximum WAL frame size")
				require.EqualValues(t, 1, w.Sequence())
				require.Equal(t, []uint64{1}, sequencesAt(t, w))
			},
		},
		{
			name:   "batch_single_oversize",
			policy: WALRecordsStandard,
			refuse: func(t *testing.T, w *WAL, oversize walEntityCommitData) {
				batch := w.NewBatch()
				require.NoError(t, batch.append(OpEntityCommit, oversize))
				_, _, err := batch.CommitWithSeq()
				var rejection *walRecordRejection
				require.ErrorAs(t, err, &rejection, "the batch reports the journal's record refusal")
				require.EqualValues(t, 1, w.Sequence(), "the refused batch publishes no sequence")
				require.Equal(t, []uint64{1}, sequencesAt(t, w), "the refused batch writes nothing")
			},
		},
		{
			name:   "batch_accepted_prefix_then_oversize",
			policy: WALRecordsStandard,
			refuse: func(t *testing.T, w *WAL, oversize walEntityCommitData) {
				batch := w.NewBatch()
				require.NoError(t, batch.append(OpCheckpoint, map[string]interface{}{"reason": "accepted-prefix"}))
				require.NoError(t, batch.append(OpEntityCommit, oversize))
				_, _, err := batch.CommitWithSeq()
				var rejection *walRecordRejection
				require.ErrorAs(t, err, &rejection, "the batch reports the journal's record refusal")
				require.EqualValues(t, 2, w.Sequence(), "the accepted prefix record publishes its number and the refused record publishes none")
				require.Equal(t, []uint64{1, 2}, sequencesAt(t, w), "the accepted prefix record is written and the refused record is not")
			},
		},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			root := t.TempDir()
			cfg := DefaultWALConfig()
			cfg.Dir = filepath.Join(root, "wal")
			cfg.SyncMode = "immediate"
			cfg.RecordPolicy = variant.policy
			cfg.RetentionMaxSegments = 100
			cfg.RetentionMaxAge = time.Hour
			open := func() *WAL {
				w, err := NewWAL(cfg.Dir, cfg)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, w.Close()) })
				return w
			}
			w := open()
			source := newTestMemoryEngine(t)
			writer := fixtureowner.OwnChecked(t, NewWALEngine(source, w))
			_, err := writer.CreateNode(t.Context(), &Node{ID: "test:before", Labels: []string{"Doc"}, Properties: map[string]interface{}{"value": "before"}})
			require.NoError(t, err)
			variant.refuse(t, w, oversize)
			snapshot := filepath.Join(root, "snapshot.nds")
			require.NoError(t, SaveStreamingSnapshot(t.Context(), source, snapshot, SnapshotOptions{Sequence: w.Sequence()}))
			require.NoError(t, w.ManageSnapshot(t.Context(), w.Sequence(), WALSnapshotTruncate))
			require.NoError(t, w.Close())

			cfg.MaxEntries = 2 // Exercise the first rollover after restarting a sealed range.
			w = open()
			writer = fixtureowner.OwnChecked(t, NewWALEngine(newTestMemoryEngine(t), w))
			_, err = writer.CreateNode(t.Context(), &Node{ID: "test:after", Labels: []string{"Doc"}, Properties: map[string]interface{}{"value": "after"}})
			require.NoError(t, err)
			previous := w.Sequence()
			for i := 0; i < 5; i++ {
				next, err := w.AppendReturningSeq(OpCheckpoint, map[string]any{"rollover": i})
				require.NoError(t, err, "rollover must not overlap the retained historical range")
				require.Greater(t, next, previous)
				previous = next
			}
			require.NoError(t, w.Close())

			recovered := newTestMemoryEngine(t)
			_, _, err = RecoverIntoEngine(t.Context(), recovered, cfg.Dir, snapshot)
			require.NoError(t, err)
			for _, id := range []string{"before", "after"} {
				node, err := recovered.GetNode(t.Context(), NodeID("test:"+id))
				require.NoError(t, err, "acknowledged node must survive snapshot plus subsequent journal recovery")
				require.Equal(t, id, node.Properties["value"])
			}

		})
	}
}
