package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBufferedWALRejectsIncompleteNumericBindingsBeforeApplyingData(t *testing.T) {
	f := newForeignNumericReplayFixture(t)
	const priorID = EdgeID("test:binding-sentinel")
	const newID = EdgeID("test:binding-replayed")
	require.NoError(t, f.dest.CreateEdge(&Edge{
		ID: priorID, StartNode: f.holderNode, EndNode: f.holderEnd, Type: "SENTINEL",
	}))
	entry := f.bufferedEdgeCommit(t, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(&Edge{ID: newID, StartNode: foreignStartNode, EndNode: foreignEndNode, Type: "LINK"})
	})
	data, err := decodeBufferedWALRecord(entry)
	require.NoError(t, err)
	require.Greater(t, len(data.NumericBindings), 1)
	for _, invalid := range [][]byte{nil, {1}, data.NumericBindings[:len(data.NumericBindings)-1]} {
		data.NumericBindings = invalid
		var buffer bytes.Buffer
		payload, err := marshalWALRecoveryPayload(&buffer, &data)
		require.NoError(t, err)
		broken := entry
		broken.Data = compressWALBufferedCommitPayload(payload)
		broken.Checksum = crc32Checksum(broken.Data)
		require.Error(t, ReplayWALEntry(f.dest, broken))
		f.dest.invalidateCachesAfterRestore()
		_, err = f.dest.GetEdge(newID)
		require.ErrorIs(t, err, ErrNotFound)
		prior, err := f.dest.GetEdge(priorID)
		require.NoError(t, err)
		require.Equal(t, f.holderNode, prior.StartNode)
		require.Equal(t, f.holderEnd, prior.EndNode)
		out, err := f.dest.GetOutgoingEdges(f.holderNode)
		require.NoError(t, err)
		require.Equal(t, []EdgeID{priorID}, foreignEdgeIDs(out))
	}
}

// Recovery must reject unknown physical key families before publishing any
// part of a buffered record, even when its numeric identity table is valid.
func TestBufferedWALRejectsUnknownPhysicalKeyBeforeApplyingData(t *testing.T) {
	f := newForeignNumericReplayFixture(t)
	const newID = EdgeID("test:unknown-family-replayed")
	entry := f.bufferedEdgeCommit(t, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(&Edge{ID: newID, StartNode: foreignStartNode, EndNode: foreignEndNode, Type: "LINK"})
	})
	data, err := decodeBufferedWALRecord(entry)
	require.NoError(t, err)
	unknown := []byte{0xee, 0x00, 0xff}
	data.Writes[string(unknown)] = []byte("unexpected")
	var buffer bytes.Buffer
	payload, err := marshalWALRecoveryPayload(&buffer, &data)
	require.NoError(t, err)
	entry.Data = compressWALBufferedCommitPayload(payload)
	entry.Checksum = crc32Checksum(entry.Data)
	require.Error(t, ReplayWALEntry(f.dest, entry))
	f.dest.invalidateCachesAfterRestore()
	_, err = f.dest.GetEdge(newID)
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, f.dest.db.View(func(tx *badger.Txn) error {
		_, err := tx.Get(unknown)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestBufferedWALRejectsUnknownKeyWithoutNumericFields(t *testing.T) {
	f := newForeignNumericReplayFixture(t)
	entry := f.bufferedEdgeCommit(t, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(&Edge{ID: "test:forged", StartNode: foreignStartNode, EndNode: foreignEndNode, Type: "LINK"})
	})
	data, err := decodeBufferedWALRecord(entry)
	require.NoError(t, err)
	unknown := []byte{0xee, 0x00, 0xff}
	data.Ops = nil
	data.Writes = map[string][]byte{string(unknown): []byte("unexpected")}
	data.Deletes = nil
	data.NumericBindings = []byte{1}
	var buffer bytes.Buffer
	payload, err := marshalWALRecoveryPayload(&buffer, &data)
	require.NoError(t, err)
	entry.Data = compressWALBufferedCommitPayload(payload)
	entry.Checksum = crc32Checksum(entry.Data)
	require.Error(t, ReplayWALEntry(f.dest, entry))
	require.NoError(t, f.dest.db.View(func(tx *badger.Txn) error {
		_, err := tx.Get(unknown)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestBufferedWALRejectsUnknownKeyBeforeAppend(t *testing.T) {
	engine, wal, commit := bindingCostFixture(t, 1)
	commit.writes[string([]byte{0xee, 0x00})] = []byte("unexpected")
	sequence := wal.Sequence()
	require.Error(t, wal.AppendBufferedCommit(commit, engine.propKeyDict))
	require.Equal(t, sequence, wal.Sequence())
}

func TestValidateReboundWALWrites(t *testing.T) {
	cases := []struct {
		name    string
		writes  map[string][]byte
		deletes map[string]bool
		wantErr bool
	}{
		{name: "known families", writes: map[string][]byte{string([]byte{prefixNode, 'n'}): []byte("value")}, deletes: map[string]bool{string([]byte{prefixNode, 'd'}): true}},
		{name: "unknown write", writes: map[string][]byte{string([]byte{0xee, 'x'}): []byte("value")}, wantErr: true},
		{name: "unknown delete", deletes: map[string]bool{string([]byte{0xee, 'x'}): true}, wantErr: true},
		{name: "empty write key", writes: map[string][]byte{"": nil}, wantErr: true},
		{name: "malformed numeric delete", deletes: map[string]bool{string([]byte{prefixMVCCNode, 1}): true}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateReboundWALWrites(tc.writes, tc.deletes)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHistoricalWALNumericAliasesRequireCompletePairs(t *testing.T) {
	var nodeKey [9]byte
	nodeKey[0] = prefixIDDictNodeReverse
	binary.BigEndian.PutUint64(nodeKey[1:], 7)
	var seven [8]byte
	binary.BigEndian.PutUint64(seven[:], 7)
	forward := string(nodeIDForwardKey("test:node"))
	reverse := string(nodeKey[:])
	for _, tc := range []struct {
		name   string
		writes map[string][]byte
		valid  bool
	}{
		{"paired", map[string][]byte{forward: seven[:], reverse: []byte("test:node")}, true},
		{"missing reverse", map[string][]byte{forward: seven[:]}, false},
		{"missing forward", map[string][]byte{reverse: []byte("test:node")}, false},
		{"wrong reverse", map[string][]byte{forward: seven[:], reverse: []byte("test:other")}, false},
		{"short number", map[string][]byte{forward: {7}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sentinel := walNumericToken{kind: walNumericEdge, id: 99}
			aliases := map[walNumericToken]string{sentinel: "test:sentinel"}
			err := mergeWALNumericSourceEntries(tc.writes, aliases)
			if !tc.valid {
				require.Error(t, err)
				require.Equal(t, map[walNumericToken]string{sentinel: "test:sentinel"}, aliases)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "test:node", aliases[walNumericToken{kind: walNumericNode, id: 7}])
		})
	}
}
