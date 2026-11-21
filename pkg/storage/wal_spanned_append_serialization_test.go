package storage

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// spanningCountingRecord is a record payload whose serialization is observable:
// it is not one of the journal's typed recovery payloads, so
// marshalWALRecoveryPayload encodes it through the property JSON encoder, which
// calls MarshalJSON once for each logical serialization. Its data is a JSON
// object holding body, so a body one frame long makes it a spanned record.
type spanningCountingRecord struct {
	body     string
	marshals atomic.Int64
}

func (record *spanningCountingRecord) MarshalJSON() ([]byte, error) {
	record.marshals.Add(1)
	return fmt.Appendf(nil, `{"body":%q}`, record.body), nil
}

// An accepted append of a record that needs more than one frame serializes that
// record once. The first attempt has to learn only whether the record spans, so
// it may not serialize the record a second time on its own behalf.
func TestSpannedAppendSerializesAnAcceptedRecordOnce(t *testing.T) {
	w := newSpannedTestWAL(t, t.TempDir(), nil)
	defer func() { require.NoError(t, w.Close()) }()

	record := &spanningCountingRecord{body: strings.Repeat("a", int(walMaxEntrySize))}
	asked := 0
	seq, err := w.appendDecidingLargeRecord(OpEntityCommit, record, "test", func() error {
		asked++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, asked, "a record that needs more than one frame is offered to its caller")
	require.EqualValues(t, 1, record.marshals.Load(), "the admitted record is serialized once, not once for each append attempt")

	entries, err := ReadWALEntriesFromDir(context.Background(), w.config.Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, seq, entries[0].Sequence)
	require.Equal(t, walFormatVersionSpanned, entries[0].framingVersion, "the record is written in parts")
	require.Equal(t, fmt.Sprintf(`{"body":%q}`, record.body), string(entries[0].Data), "the parts read back as the record that was serialized")
}

// BenchmarkSpannedAppend is the bounded large-record workload of the allocation
// test: one admitted entity commit through the journal's large-record question.
// SetBytes reports the serialized record, so B/op and MB/s are per record byte.
func BenchmarkSpannedAppend(b *testing.B) {
	b.Cleanup(config.WithWALEnabled())
	cfg := DefaultWALConfig()
	cfg.Dir = b.TempDir()
	cfg.SyncMode = "batch"
	cfg.BatchSyncInterval = time.Hour
	cfg.RecordPolicy = WALRecordsLarge
	w, err := NewWAL(cfg.Dir, cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := w.Close(); err != nil {
			b.Error(err)
		}
	})
	record := walEntityCommitData{Payload: make([]byte, int(walMaxEntrySize))}
	serialized, err := marshalWALRecoveryPayload(nil, record)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(serialized)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := w.appendDecidingLargeRecord(OpEntityCommit, record, "test", func() error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSpannedAppendStopsIfJournalClosesDuringAdmission(t *testing.T) {
	w := newSpannedTestWAL(t, t.TempDir(), nil)
	defer func() { require.NoError(t, w.Close()) }()
	record := walEntityCommitData{Payload: make([]byte, int(walMaxEntrySize))}
	seq, err := w.appendDecidingLargeRecord(OpEntityCommit, record, "test", w.Close)
	require.ErrorIs(t, err, ErrWALClosed)
	require.Zero(t, seq)
	require.Zero(t, w.Sequence())
	entries, err := ReadWALEntriesFromDir(t.Context(), w.Config().Dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// One commit of the admission fixture whose record spans frames: the commit is
// admitted, the journal holds one record of it written in parts, the store holds
// its node, and the recovery at open reads the node back from that record.
func TestAdmittedSpannedCommitIsOneRecordThatReplays(t *testing.T) {
	dir := t.TempDir()
	b := admissionEngine(t, dir, false, false)
	text := int(walMaxEntrySize) + 1<<20
	stage := admissionNodes(1, admissionText(text))
	const logicalID = "spanned-record"
	require.NoError(t, admissionCommit(t, b, CommitModeExplicit, logicalID, stage), "the commit is admitted and applies")
	require.Equal(t, 1, admissionRecords(t, dir, logicalID), "the journal holds one record of the commit")
	require.Equal(t, 1, admissionStored(t, b), "the store holds the commit's node")

	entries, err := ReadWALEntriesFromDir(context.Background(), b.bufferedCommitWAL.config.Dir)
	require.NoError(t, err)
	spanned := 0
	for _, entry := range entries {
		if entry.Operation != OpEntityCommit {
			continue
		}
		var data walEntityCommitData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
		envelope, _, err := DecodeEntityTransaction(data.Payload)
		require.NoError(t, err)
		if envelope.LogicalTxID != logicalID {
			continue
		}
		spanned++
		require.Equal(t, walFormatVersionSpanned, entry.framingVersion, "the commit's record is written in parts")
	}
	require.Equal(t, 1, spanned, "the commit's record is one record, not one per part")

	b = admissionReopen(t, b, dir, false, false)
	node, err := b.GetNode(context.Background(), "test:admission-0")
	require.NoError(t, err, "the recovery at open replays the record's node")
	require.Equal(t, strings.Repeat("a", text), node.Properties["text"])
}
