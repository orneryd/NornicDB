package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestBufferedWALPreservesPhysicalKeysAndTypedOperations(t *testing.T) {
	b, wal, commit := bindingCostFixture(t, 1)
	first := string([]byte{prefixNode, 0, 0xff, 0x80, 1})
	second := string([]byte{prefixNode, 0, 0xfe, 0x80, 1})
	deleted := string([]byte{prefixNode, 0xfe, 0, 0xff})
	commit.writes[first] = []byte{1, 2}
	commit.writes[second] = []byte{3, 4}
	commit.deletes = map[string]bool{deleted: true}
	zone, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)
	when := time.Date(2026, 2, 3, 4, 5, 6, 7, zone)
	properties := map[string]any{"when": when, "exact": int64(9007199254740993), "text": "lone\xed\xa0\x80surrogate"}
	commit.ops = []Operation{{Type: OpCreateNode, Node: &Node{ID: "perf:typed", Properties: properties}}}

	require.NoError(t, wal.AppendBufferedCommit(commit, b.propKeyDict))
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	payload, err := decompressWALBufferedCommitPayload(entries[0].Data)
	require.NoError(t, err)
	serializer, _, headered, err := splitSerializationHeader(payload)
	require.NoError(t, err)
	require.True(t, headered)
	require.Equal(t, serializerIDMsgpack, serializer)
	decoded, err := decodeBufferedWALRecord(entries[0])
	require.NoError(t, err)
	require.Equal(t, commit.writes, decoded.Writes)
	require.Equal(t, []string{deleted}, decoded.Deletes)
	require.Len(t, decoded.Ops, 1)
	got := decoded.Ops[0].Node.Properties
	require.Equal(t, int64(9007199254740993), got["exact"])
	require.Equal(t, properties["text"], got["text"])
	gotTime, ok := got["when"].(time.Time)
	require.True(t, ok)
	require.True(t, when.Equal(gotTime))
	require.Equal(t, when.Location().String(), gotTime.Location().String())
	_, ok = properties["when"].(time.Time)
	require.True(t, ok, "encoding must not replace the caller's temporal value")
}

func TestBufferedWALBinaryIndexPublicReplay(t *testing.T) {
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
	dest := newTestEngine(t)
	for i := 0; i < 140; i++ {
		id := NodeID(fmt.Sprintf("test:n%d", i))
		_, err := source.CreateNode(&Node{ID: id})
		require.NoError(t, err)
		_, err = dest.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	tx, err := source.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:n128", EndNode: "test:n139", Type: "LINK", Properties: map[string]any{"p": int64(1)}}))
	var binaryKeys []string
	for key := range tx.pendingWrites {
		if !utf8.ValidString(key) {
			binaryKeys = append(binaryKeys, key)
		}
	}
	require.NotEmpty(t, binaryKeys)
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	entries, err := ReadWALEntriesFromDir(source.bufferedCommitWAL.Config().Dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	entry := entries[len(entries)-1]
	require.Equal(t, OpBufferedCommit, entry.Operation)
	decoded, err := decodeBufferedWALRecord(entry)
	require.NoError(t, err)
	for _, key := range binaryKeys {
		require.Contains(t, decoded.Writes, key)
	}
	require.NoError(t, ReplayWALEntry(dest, entry))
	edge, err := dest.GetEdge("test:r")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:r"), edge.ID)
	outgoing, err := dest.GetOutgoingEdges("test:n128")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	incoming, err := dest.GetIncomingEdges("test:n139")
	require.NoError(t, err)
	require.Len(t, incoming, 1)
}

// A target can assign different numeric IDs to the same logical nodes. Replay
// must preserve traversal and not install source-local IDs into that target.
func TestBufferedWALForeignNumericIDsPublicReplay(t *testing.T) {
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
	dest := newTestEngine(t)
	for i := 0; i < 140; i++ {
		_, err := source.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i))})
		require.NoError(t, err)
	}
	for i := 139; i >= 0; i-- {
		_, err := dest.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i))})
		require.NoError(t, err)
	}
	// Give the target's first relationship a different logical identity so
	// reusing the source's relationship number would corrupt an existing
	// dictionary entry as well as misroute the imported relationship.
	require.NoError(t, dest.CreateEdge(&Edge{ID: "test:existing", StartNode: "test:n10", EndNode: "test:n11", Type: "KEEP"}))
	sourceID, sourceFound := source.idDict.lookupNodeNumID("test:n128")
	destID, destFound := dest.idDict.lookupNodeNumID("test:n128")
	require.True(t, sourceFound && destFound)
	require.NotEqual(t, sourceID, destID)

	tx, err := source.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:n128", EndNode: "test:n139", Type: "LINK", Properties: map[string]any{"p": int64(1)}}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	entries, err := ReadWALEntriesFromDir(source.bufferedCommitWAL.Config().Dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	entry := entries[len(entries)-1]
	require.Equal(t, OpBufferedCommit, entry.Operation)
	require.NoError(t, ReplayWALEntry(dest, entry))
	got, err := dest.GetEdge("test:r")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n128"), got.StartNode)
	require.Equal(t, NodeID("test:n139"), got.EndNode)
	outgoing, err := dest.GetOutgoingEdges("test:n128")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.Equal(t, EdgeID("test:r"), outgoing[0].ID)
	incoming, err := dest.GetIncomingEdges("test:n139")
	require.NoError(t, err)
	require.Len(t, incoming, 1)
	require.Equal(t, EdgeID("test:r"), incoming[0].ID)
	old, err := dest.GetEdge("test:existing")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n10"), old.StartNode)
	require.Equal(t, NodeID("test:n11"), old.EndNode)
	keep, err := dest.GetOutgoingEdges("test:n10")
	require.NoError(t, err)
	require.Len(t, keep, 1)
	require.Equal(t, EdgeID("test:existing"), keep[0].ID)
}

func TestBufferedWALRejectsOversizedTypedPayloadBeforeAppend(t *testing.T) {
	b, wal, commit := bindingCostFixture(t, 1)
	commit.writes[string([]byte{prefixNode})+"oversized"] = bytes.Repeat([]byte{1}, maxBufferedWALPayloadBytes)
	err := wal.AppendBufferedCommit(commit, b.propKeyDict)
	require.ErrorContains(t, err, "exceeds decoded payload limit")
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// A record that fits the decoded-payload limit can still exceed the outer
// frame limit. It must be rejected before an ACK, not written unreadably.
func TestBufferedWALRejectsUnreadableOversizedFrameBeforeAppend(t *testing.T) {
	b, wal, commit := bindingCostFixture(t, 1)
	value := make([]byte, int(walMaxEntrySize)+4096)
	_, err := rand.New(rand.NewSource(42)).Read(value)
	require.NoError(t, err)
	commit.writes[string([]byte{prefixNode})+"incompressible"] = value
	err = wal.AppendBufferedCommit(commit, b.propKeyDict)
	require.ErrorContains(t, err, "exceeds maximum WAL frame size")
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}
