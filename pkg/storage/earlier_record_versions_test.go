package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// NOR-412: a transaction record whose version is 4, 5 or 6. This change writes
// version 3. DecodeEntityTransaction returns ErrInvalidData for such a record;
// when the journal holds one, the recovery at open
// (RecoverBufferedCommitsAtOpen) returns ErrInvalidData and the record's update
// is not applied.
//
// Each record here is a version-3 record with only its version rewritten; the
// test does not use records that an earlier head of this pull request wrote.
func TestEarlierRecordVersionsAreRefusedAtOpen(t *testing.T) {
	for _, version := range []uint16{4, 5, 6} {
		t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) {
			dir := t.TempDir()
			b, w := largeRecordEngine(t, dir, WALRecordsLarge)
			largeRecordSeed(t, b, "test:earlier", "alpha", 4, 1)
			payload := largeRecordPrepare(t, b, "earlier", map[NodeID]func(*Node){"test:earlier": func(n *Node) { n.Properties["flag"] = true }})
			var wire map[string]msgpack.RawMessage
			require.NoError(t, msgpack.Unmarshal(payload, &wire))
			encoded, err := msgpack.Marshal(version)
			require.NoError(t, err)
			wire["Version"] = encoded
			earlier, err := msgpack.Marshal(wire)
			require.NoError(t, err)
			_, _, err = DecodeEntityTransaction(earlier)
			require.ErrorIs(t, err, ErrInvalidData)
			largeRecordJournal(t, w, earlier)
			crashClose(t, b, w)

			b, w = largeRecordEngine(t, dir, WALRecordsLarge)
			_, err = RecoverBufferedCommitsAtOpen(b, w)
			t.Logf("recovery at open with a version-%d record: %v", version, err)
			require.ErrorIs(t, err, ErrInvalidData, "the recovery at open returns ErrInvalidData")
			got := requireLargeRecordVectors(t, b, "test:earlier", 4, 1)
			require.Equal(t, false, got.Properties["flag"], "the record is not applied")
		})
	}
}
