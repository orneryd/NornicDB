package storage

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWALCapacityAdmissionPreservesEncodingLimits(t *testing.T) {
	for _, policy := range []WALRecordPolicy{WALRecordsStandard, WALRecordsLarge} {
		w := &WAL{recordPolicy: policy}
		require.NoError(t, w.CheckRecordCapacity(OpEntityCommit, 100, "quoted\"<&\u2603"))
		if policy == WALRecordsStandard {
			require.ErrorContains(t, w.CheckRecordCapacity(OpEntityCommit, int(walMaxEntrySize), "test"), "maximum WAL frame size")
		} else {
			require.NoError(t, w.CheckRecordCapacity(OpEntityCommit, int(walMaxEntrySize), "test"))
			require.ErrorContains(t, w.CheckRecordCapacity(OpEntityCommit, int(uint64(math.MaxUint32)+1), "test"), "record format's limit")
		}
		require.ErrorIs(t, w.CheckRecordCapacity(OpEntityCommit, -1, "test"), ErrInvalidData)
	}
}

func TestWALCapacityAdmissionReservesGrowingAppendMetadata(t *testing.T) {
	w := &WAL{}
	// Find the exact admission boundary without allocating the payload.
	low, high := 0, int(walMaxEntrySize)
	for low < high {
		middle := low + (high-low+1)/2
		if w.CheckRecordCapacity(OpEntityCommit, middle, "test") == nil {
			low = middle
		} else {
			high = middle - 1
		}
	}
	require.Error(t, w.CheckRecordCapacity(OpEntityCommit, low+1, "test"))
	payload := make([]byte, low)
	for _, sequence := range []uint64{1, 9, 10, math.MaxUint64} {
		for _, at := range []time.Time{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("", -7*60*60))} {
			entry := WALEntry{Sequence: sequence, Timestamp: at, Operation: OpEntityCommit, Checksum: math.MaxUint32, Database: "test", Data: payload}
			_, large, err := encodeWALEntryPayload(&bytes.Buffer{}, &entry, false)
			require.NoError(t, err)
			require.False(t, large)
		}
	}
}

// A fitting standard replication frame contains a base64 semantic payload,
// hence at most 12 MiB of payload bytes. The native frame stores those bytes as
// MessagePack binary. Even the largest namespace admitted by native physical-key
// validation, escaped sixfold in the JSON header, cannot consume the remaining
// four MiB. This pins the cross-journal capacity argument without assuming a
// realistic/small database name.
func TestStandardReplicationPayloadCeilingFitsNativeJournal(t *testing.T) {
	body, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: make([]byte, 12*1024*1024), Capture: 255})
	require.NoError(t, err)
	namespace := strings.Repeat("\x01", storeKeyLimit)
	w := &WAL{recordPolicy: WALRecordsStandard}
	require.NoError(t, w.CheckRecordCapacity(OpEntityCommit, len(body), namespace))
	entry := WALEntry{Sequence: math.MaxUint64, Timestamp: time.Now(), Operation: OpEntityCommit, Database: namespace, Checksum: math.MaxUint32, Data: body}
	_, large, err := encodeWALEntryPayload(&bytes.Buffer{}, &entry, false)
	require.NoError(t, err)
	require.False(t, large)
}
