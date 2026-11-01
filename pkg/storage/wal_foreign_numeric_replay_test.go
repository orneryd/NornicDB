package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A replayed foreign buffered write-behind commit carries the *source* store's
// numeric node allocation. The destination may have handed those same numbers
// to different logical nodes, so replay must re-resolve every relationship
// endpoint by logical identity instead of installing the source-local numbers.
//
// Each test below drives create, update or delete through the real public
// write-behind/WAL path on the source and replays the resulting
// OpBufferedCommit entry into a destination whose allocation order differs. It
// then asserts the destination's edge endpoints, directional/between/type
// indexes, preservation of unrelated destination data, and the persisted
// result after a close and reopen.
const (
	foreignStartNode = NodeID("test:n128")
	foreignEndNode   = NodeID("test:n139")
	foreignNodeCount = 140
)

// foreignNumericReplayFixture holds a source and a file-backed destination
// holding the same logical nodes in a deliberately different numeric
// allocation order: the source allocates test:n0..test:n139 ascending, the
// destination descending. Every logical node therefore keeps an identity in
// both stores while the mutated endpoints get different numbers.
//
// The holder nodes are looked up, never assumed: they are the destination
// nodes that own the numbers the source gave the mutated endpoints, and they
// are exactly the nodes a source-number-based replay would corrupt.
type foreignNumericReplayFixture struct {
	source     *BadgerEngine
	dest       *BadgerEngine
	destDir    string
	srcStart   uint64 // source number of foreignStartNode
	srcEnd     uint64 // source number of foreignEndNode
	holderNode NodeID // destination node that owns srcStart
	holderEnd  NodeID // destination node that owns srcEnd
}

func newForeignNumericReplayFixture(t *testing.T) *foreignNumericReplayFixture {
	t.Helper()

	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
	destDir := t.TempDir()
	dest := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: destDir})
	t.Cleanup(func() {
		if source.bufferedCommitWAL != nil {
			_ = source.bufferedCommitWAL.Close()
		}
	})

	for i := 0; i < foreignNodeCount; i++ {
		id := NodeID(fmt.Sprintf("test:n%d", i))
		_, err := source.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	for i := foreignNodeCount - 1; i >= 0; i-- {
		id := NodeID(fmt.Sprintf("test:n%d", i))
		_, err := dest.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}

	f := &foreignNumericReplayFixture{source: source, dest: dest, destDir: destDir}

	var ok bool
	f.srcStart, ok = source.idDict.lookupNodeNumID(foreignStartNode)
	require.True(t, ok, "source must allocate a number for %s", foreignStartNode)
	f.srcEnd, ok = source.idDict.lookupNodeNumID(foreignEndNode)
	require.True(t, ok)
	destStartNum, ok := dest.idDict.lookupNodeNumID(foreignStartNode)
	require.True(t, ok)
	destEndNum, ok := dest.idDict.lookupNodeNumID(foreignEndNode)
	require.True(t, ok)

	// Both stores hold the same logical graph; only the numbers differ.
	require.NotEqual(t, f.srcStart, destStartNum,
		"the divergent allocation order must give %s a different destination number", foreignStartNode)
	require.NotEqual(t, f.srcEnd, destEndNum,
		"the divergent allocation order must give %s a different destination number", foreignEndNode)
	require.NotEqual(t, f.srcStart, f.srcEnd)
	require.NotEqual(t, destStartNum, destEndNum)

	// The destination nodes that currently own the source's numbers for the
	// mutated endpoints.
	f.holderNode, ok = dest.idDict.lookupNodeIDByNum(f.srcStart)
	require.True(t, ok)
	require.NotEqual(t, foreignStartNode, f.holderNode)
	f.holderEnd, ok = dest.idDict.lookupNodeIDByNum(f.srcEnd)
	require.True(t, ok)
	require.NotEqual(t, foreignEndNode, f.holderEnd)
	require.NotEqual(t, f.holderNode, f.holderEnd)

	return f
}

// bufferedEdgeCommit runs one relationship mutation as a real write-behind
// transaction on the source and returns the resulting OpBufferedCommit entry.
func (f *foreignNumericReplayFixture) bufferedEdgeCommit(t *testing.T, mutate func(tx *BadgerTransaction) error) WALEntry {
	t.Helper()
	tx, err := f.source.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, mutate(tx))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the source commit must be a buffered write-behind commit")

	entries, err := ReadWALEntriesFromDir(f.source.bufferedCommitWAL.Config().Dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	entry := entries[len(entries)-1]
	require.Equal(t, OpBufferedCommit, entry.Operation)
	return entry
}

func (f *foreignNumericReplayFixture) replay(t *testing.T, entry WALEntry) {
	t.Helper()
	require.NoError(t, ReplayWALEntry(f.dest, entry))
	// Assertions read the destination's stored bodies and indexes, not a
	// replayed in-memory publication cache.
	f.dest.invalidateCachesAfterRestore()
}

// closeSource releases the source so the replayed entry is the only authority
// under test from here on.
func (f *foreignNumericReplayFixture) closeSource(t *testing.T) {
	t.Helper()
	if f.source.bufferedCommitWAL != nil {
		require.NoError(t, f.source.bufferedCommitWAL.Close())
		f.source.bufferedCommitWAL = nil
	}
	require.NoError(t, f.source.Close())
}

// reopenDestination closes the destination and opens a new engine on the same
// data directory so later assertions read persisted state.
func (f *foreignNumericReplayFixture) reopenDestination(t *testing.T) {
	t.Helper()
	require.NoError(t, f.dest.Close())
	f.dest = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: f.destDir})
	f.dest.invalidateCachesAfterRestore()
}

// requireIntendedEndpoints is the primary regression assertion: the replayed
// relationship must keep the logical endpoints it had in the source, whatever
// numbers the destination assigned them.
func (f *foreignNumericReplayFixture) requireIntendedEndpoints(t *testing.T, id EdgeID) *Edge {
	t.Helper()
	edge, err := f.dest.GetEdge(id)
	require.NoError(t, err)
	require.Equal(t, foreignStartNode, edge.StartNode,
		"replay must resolve the start endpoint by identity, not by the source's number %d", f.srcStart)
	require.Equal(t, foreignEndNode, edge.EndNode,
		"replay must resolve the end endpoint by identity, not by the source's number %d", f.srcEnd)
	return edge
}

// requireEdgeIndexed asserts that id is reachable through every index the
// destination maintains for it: outgoing and incoming adjacency, the between
// index in its stored direction, and the type index.
func (f *foreignNumericReplayFixture) requireEdgeIndexed(t *testing.T, id EdgeID) {
	t.Helper()
	edge := f.requireIntendedEndpoints(t, id)
	outgoing, err := f.dest.GetOutgoingEdges(context.Background(), edge.StartNode)
	require.NoError(t, err)
	require.Contains(t, testEdgeIDs(outgoing), id, "outgoing index of %s", edge.StartNode)
	incoming, err := f.dest.GetIncomingEdges(context.Background(), edge.EndNode)
	require.NoError(t, err)
	require.Contains(t, testEdgeIDs(incoming), id, "incoming index of %s", edge.EndNode)
	between, err := f.dest.GetEdgesBetween(context.Background(), edge.StartNode, edge.EndNode)
	require.NoError(t, err)
	require.Contains(t, testEdgeIDs(between), id, "between index of %s->%s", edge.StartNode, edge.EndNode)
	typed, err := f.dest.GetEdgesByType(context.Background(), edge.Type)
	require.NoError(t, err)
	require.Contains(t, testEdgeIDs(typed), id, "type index of %s", edge.Type)
}

func TestForeignBufferedWALNumericIDsCreateKeepsDestinationEndpoints(t *testing.T) {
	const (
		priorID   = EdgeID("test:prior-create")
		linkID    = EdgeID("test:r")
		priorType = "PRIORTYPE"
		linkType  = "LINK"
	)

	f := newForeignNumericReplayFixture(t)

	// The destination already owns an edge between the logical nodes whose
	// numbers equal the source numbers of the mutated endpoints.
	require.NoError(t, f.dest.CreateEdge(&Edge{
		ID: priorID, StartNode: f.holderNode, EndNode: f.holderEnd,
		Type: priorType, Properties: map[string]any{"keep": "prior-create"},
	}))

	entry := f.bufferedEdgeCommit(t, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(&Edge{
			ID: linkID, StartNode: foreignStartNode, EndNode: foreignEndNode,
			Type: linkType, Properties: map[string]any{"p": int64(1)},
		})
	})
	f.closeSource(t)
	f.replay(t, entry)

	got := f.requireIntendedEndpoints(t, linkID)
	require.Equal(t, linkType, got.Type)
	require.Equal(t, int64(1), got.Properties["p"])
	f.requireEdgeIndexed(t, linkID)

	// Direction is not symmetric: the replayed edge must not surface in the
	// reversed traversal.
	reverseOut, err := f.dest.GetOutgoingEdges(context.Background(), foreignEndNode)
	require.NoError(t, err)
	require.NotContains(t, testEdgeIDs(reverseOut), linkID)
	reverseIn, err := f.dest.GetIncomingEdges(context.Background(), foreignStartNode)
	require.NoError(t, err)
	require.NotContains(t, testEdgeIDs(reverseIn), linkID)
	reverseBetween, err := f.dest.GetEdgesBetween(context.Background(), foreignEndNode, foreignStartNode)
	require.NoError(t, err)
	require.NotContains(t, testEdgeIDs(reverseBetween), linkID)

	// The destination-owned edge whose endpoints hold the source numbers is
	// untouched in body and in every index.
	prior, err := f.dest.GetEdge(priorID)
	require.NoError(t, err, "the destination edge at the source numbers must survive the replayed create")
	require.Equal(t, f.holderNode, prior.StartNode)
	require.Equal(t, f.holderEnd, prior.EndNode)
	require.Equal(t, priorType, prior.Type)
	require.Equal(t, "prior-create", prior.Properties["keep"])
	priorOut, err := f.dest.GetOutgoingEdges(context.Background(), f.holderNode)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorOut), "the holder keeps only its own outgoing edge")
	priorIn, err := f.dest.GetIncomingEdges(context.Background(), f.holderEnd)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorIn), "the holder keeps only its own incoming edge")
	priorBetween, err := f.dest.GetEdgesBetween(context.Background(), f.holderNode, f.holderEnd)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorBetween))
	priorTyped, err := f.dest.GetEdgesByType(context.Background(), priorType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorTyped))

	// The mapping and every traversal survive a close and reopen.
	f.reopenDestination(t)
	persisted := f.requireIntendedEndpoints(t, linkID)
	require.Equal(t, int64(1), persisted.Properties["p"])
	f.requireEdgeIndexed(t, linkID)
	_, err = f.dest.GetEdge(priorID)
	require.NoError(t, err)
}

func TestForeignBufferedWALNumericIDsUpdateKeepsDestinationIndexes(t *testing.T) {
	const (
		priorID   = EdgeID("test:prior-update")
		movingID  = EdgeID("test:moving")
		priorType = "PRIORTYPE"
		oldType   = "OLDTYPE"
		newType   = "NEWTYPE"
	)

	f := newForeignNumericReplayFixture(t)

	require.NoError(t, f.dest.CreateEdge(&Edge{
		ID: priorID, StartNode: f.holderNode, EndNode: f.holderEnd,
		Type: priorType, Properties: map[string]any{"keep": "prior-update"},
	}))
	// The mutated edge exists in both stores with identical logical
	// endpoints; only its endpoints' numeric identities differ.
	require.NoError(t, f.source.CreateEdge(&Edge{
		ID: movingID, StartNode: foreignStartNode, EndNode: foreignEndNode,
		Type: oldType, Properties: map[string]any{"v": "old"},
	}))
	require.NoError(t, f.dest.CreateEdge(&Edge{
		ID: movingID, StartNode: foreignStartNode, EndNode: foreignEndNode,
		Type: oldType, Properties: map[string]any{"v": "old"},
	}))

	entry := f.bufferedEdgeCommit(t, func(tx *BadgerTransaction) error {
		return tx.UpdateEdge(EdgeUpdateRequest{After: &Edge{
			ID: movingID, StartNode: foreignStartNode, EndNode: foreignEndNode,
			Type: newType, Properties: map[string]any{"v": "new"},
		}})
	})
	f.closeSource(t)
	f.replay(t, entry)

	got := f.requireIntendedEndpoints(t, movingID)
	require.Equal(t, newType, got.Type)
	require.Equal(t, "new", got.Properties["v"], "the replayed update must win the changed property")

	outgoing, err := f.dest.GetOutgoingEdges(context.Background(), foreignStartNode)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{movingID}, testEdgeIDs(outgoing))
	incoming, err := f.dest.GetIncomingEdges(context.Background(), foreignEndNode)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{movingID}, testEdgeIDs(incoming))

	// The previous type index entry is retired; the new one is exact.
	stale, err := f.dest.GetEdgesByType(context.Background(), oldType)
	require.NoError(t, err)
	require.Empty(t, testEdgeIDs(stale), "the previous type index entry must be retired")
	typed, err := f.dest.GetEdgesByType(context.Background(), newType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{movingID}, testEdgeIDs(typed))

	between, err := f.dest.GetEdgesBetween(context.Background(), foreignStartNode, foreignEndNode)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{movingID}, testEdgeIDs(between))

	// Unrelated destination data keeps its body and its indexes.
	prior, err := f.dest.GetEdge(priorID)
	require.NoError(t, err, "unrelated destination data must survive the replayed update")
	require.Equal(t, f.holderNode, prior.StartNode)
	require.Equal(t, f.holderEnd, prior.EndNode)
	require.Equal(t, priorType, prior.Type)
	require.Equal(t, "prior-update", prior.Properties["keep"])
	priorOut, err := f.dest.GetOutgoingEdges(context.Background(), f.holderNode)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorOut))
	priorIn, err := f.dest.GetIncomingEdges(context.Background(), f.holderEnd)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorIn))
	priorTyped, err := f.dest.GetEdgesByType(context.Background(), priorType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorTyped))

	f.reopenDestination(t)
	persisted := f.requireIntendedEndpoints(t, movingID)
	require.Equal(t, newType, persisted.Type)
	require.Equal(t, "new", persisted.Properties["v"])
	stale, err = f.dest.GetEdgesByType(context.Background(), oldType)
	require.NoError(t, err)
	require.Empty(t, testEdgeIDs(stale))
	_, err = f.dest.GetEdge(priorID)
	require.NoError(t, err)
}

func TestForeignBufferedWALNumericIDsDeleteKeepsDestinationEdges(t *testing.T) {
	const (
		priorID   = EdgeID("test:prior-delete")
		goneID    = EdgeID("test:gone")
		priorType = "PRIORTYPE"
		goneType  = "GONE"
	)

	f := newForeignNumericReplayFixture(t)

	require.NoError(t, f.dest.CreateEdge(&Edge{
		ID: priorID, StartNode: f.holderNode, EndNode: f.holderEnd,
		Type: priorType, Properties: map[string]any{"keep": "prior-delete"},
	}))
	// The relationship being deleted exists in both stores between the same
	// logical endpoints. Its destination numbers are the source numbers of the
	// holder nodes, so a replay that installs source numbers, or that tombstones
	// by index position, removes the unrelated destination edge instead.
	require.NoError(t, f.source.CreateEdge(&Edge{
		ID: goneID, StartNode: foreignStartNode, EndNode: foreignEndNode,
		Type: goneType, Properties: map[string]any{"v": "gone"},
	}))
	require.NoError(t, f.dest.CreateEdge(&Edge{
		ID: goneID, StartNode: foreignStartNode, EndNode: foreignEndNode,
		Type: goneType, Properties: map[string]any{"v": "gone"},
	}))

	entry := f.bufferedEdgeCommit(t, func(tx *BadgerTransaction) error {
		return tx.DeleteEdge(goneID)
	})
	f.closeSource(t)
	f.replay(t, entry)

	check := func(t *testing.T) {
		t.Helper()

		_, err := f.dest.GetEdge(goneID)
		require.ErrorIs(t, err, ErrNotFound, "the replayed delete must remove its own relationship")
		goneOut, err := f.dest.GetOutgoingEdges(context.Background(), foreignStartNode)
		require.NoError(t, err)
		require.NotContains(t, testEdgeIDs(goneOut), goneID)
		goneIn, err := f.dest.GetIncomingEdges(context.Background(), foreignEndNode)
		require.NoError(t, err)
		require.NotContains(t, testEdgeIDs(goneIn), goneID)
		goneTyped, err := f.dest.GetEdgesByType(context.Background(), goneType)
		require.NoError(t, err)
		require.Empty(t, testEdgeIDs(goneTyped), "no index may retain the deleted relationship")

		// The destination edge whose numbers equal the source endpoints
		// survives in its body and in every index.
		prior, err := f.dest.GetEdge(priorID)
		require.NoError(t, err, "deleting another relationship must not remove the destination edge at the source numbers")
		require.Equal(t, f.holderNode, prior.StartNode)
		require.Equal(t, f.holderEnd, prior.EndNode)
		require.Equal(t, priorType, prior.Type)
		require.Equal(t, "prior-delete", prior.Properties["keep"])
		priorOut, err := f.dest.GetOutgoingEdges(context.Background(), f.holderNode)
		require.NoError(t, err)
		require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorOut))
		priorIn, err := f.dest.GetIncomingEdges(context.Background(), f.holderEnd)
		require.NoError(t, err)
		require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorIn))
		priorBetween, err := f.dest.GetEdgesBetween(context.Background(), f.holderNode, f.holderEnd)
		require.NoError(t, err)
		require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorBetween))
		priorTyped, err := f.dest.GetEdgesByType(context.Background(), priorType)
		require.NoError(t, err)
		require.Equal(t, []EdgeID{priorID}, testEdgeIDs(priorTyped))
	}
	check(t)

	f.reopenDestination(t)
	check(t)
}
