package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-412 round 4: the replication journal keeps every record in one frame
// (WALConfig.SingleFrameRecords), and CheckOneFrameRecord lets it decide
// before anything is written with the encoders an append uses.

type policyTestRecord struct {
	Text string `json:"t"`
}

func newPolicyTestWAL(t *testing.T, single bool) *WAL {
	t.Helper()
	cfg := DefaultWALConfig()
	cfg.SingleFrameRecords = single
	w := newSpannedTestWAL(t, t.TempDir(), cfg)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// A single-frame WAL refuses a record larger than one frame with the error
// every WAL returned before records could span frames, writes nothing for it,
// and keeps appending; a WAL whose records may span frames accepts it.
func TestSingleFrameWALRefusesRecordsLargerThanOneFrame(t *testing.T) {
	large := policyTestRecord{Text: strings.Repeat("z", int(walMaxEntrySize)+1024)}
	single := newPolicyTestWAL(t, true)
	_, err := single.AppendWithDatabaseReturningSeq(OpEntityCommit, large, "test")
	require.ErrorContains(t, err, "wal: entry exceeds maximum WAL frame size of 16777216 bytes")
	var rejection *walRecordRejection
	require.ErrorAs(t, err, &rejection)
	require.Error(t, single.CheckOneFrameRecord(OpEntityCommit, large, "test"))
	_, err = single.AppendWithDatabaseReturningSeq(OpEntityCommit, policyTestRecord{Text: "small"}, "test")
	require.NoError(t, err)
	require.NoError(t, single.Sync())
	entries, err := ReadWALEntriesFromDir(t.Context(), single.config.Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "only the small record was written")

	spanning := newPolicyTestWAL(t, false)
	_, err = spanning.AppendWithDatabaseReturningSeq(OpEntityCommit, large, "test")
	require.NoError(t, err)
	require.NoError(t, spanning.CheckOneFrameRecord(OpEntityCommit, large, "test"), "a spanning WAL accepts any record")
	require.NoError(t, spanning.Sync())
	entries, err = ReadWALEntriesFromDir(t.Context(), spanning.config.Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Greater(t, len(entries[0].Data), int(walMaxEntrySize))
}

// Across the frame boundary, CheckOneFrameRecord never admits a record the
// append would refuse, and never lets through one it refuses: it differs from
// an actual append only where the append's own sequence number and timestamp
// are shorter than the widest the header can carry.
func TestCheckOneFrameRecordAgreesWithTheAppend(t *testing.T) {
	w := newPolicyTestWAL(t, true)
	base := int(walMaxEntrySize) - 256
	window := 0
	sawAccept, sawRefuse := false, false
	for size := base; size <= base+320; size += 16 {
		record := policyTestRecord{Text: strings.Repeat("y", size)}
		checkErr := w.CheckOneFrameRecord(OpEntityCommit, record, "test")
		_, appendErr := w.AppendWithDatabaseReturningSeq(OpEntityCommit, record, "test")
		t.Logf("POLICY size=%d check=%v append=%v", size, checkErr == nil, appendErr == nil)
		if checkErr == nil {
			require.NoError(t, appendErr, "a record the check admits always appends")
			sawAccept = true
		}
		if appendErr != nil {
			require.Error(t, checkErr, "a record the append refuses is always refused")
			require.ErrorContains(t, checkErr, "wal: entry exceeds maximum WAL frame size of 16777216 bytes")
			sawRefuse = true
		}
		if checkErr != nil && appendErr == nil {
			window += 16
		}
	}
	require.True(t, sawAccept)
	require.True(t, sawRefuse)
	require.LessOrEqual(t, window, 64, "the check is stricter only by the header's variable width")
}
