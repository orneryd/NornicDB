package storage

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests are the component proof of the empty-adjacency decision: a
// relationship mutation that decided a range was empty records that decision
// from its current-policy read, the guard rechecks it after body-lock waits and
// again at the commit owner before durable acceptance, and a peer relationship
// that makes the decision false is reported as a genuine optimistic conflict
// rather than a fabricated deadlock. Ordinary snapshot reads keep their
// existing contract and record no decision.

const (
	adjacencyDecisionAnchor    = NodeID("test:anchor")
	adjacencyDecisionOther     = NodeID("test:other")
	adjacencyDecisionPeerNode  = NodeID("test:peer")
	adjacencyDecisionStaged    = NodeID("test:staged")
	adjacencyDecisionLink      = "LINK"
	adjacencyDecisionUnrelated = "OTHER"
)

// adjacencyDecisionMode names one committed write mode. buffered selects the
// write-behind engine whose implicit commits are acknowledged into the buffer
// before they land in Badger; the direct engine commits through Badger.
type adjacencyDecisionMode struct {
	name     string
	buffered bool
}

func adjacencyDecisionModes(t *testing.T, run func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine)) {
	t.Helper()
	for _, mode := range []adjacencyDecisionMode{{name: "direct"}, {name: "write-behind", buffered: true}} {
		t.Run(mode.name, func(t *testing.T) {
			run(t, mode, newAdjacencyDecisionEngine(t, mode.buffered))
		})
	}
}

// newAdjacencyDecisionEngine seeds the committed nodes a decision is made
// about. No relationship is seeded: every decision starts from an empty range.
func newAdjacencyDecisionEngine(t *testing.T, buffered bool) *BadgerEngine {
	t.Helper()
	var engine *BadgerEngine
	if buffered {
		engine = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	} else {
		engine = newTestMemoryEngine(t).BadgerEngine
	}
	for _, id := range []NodeID{adjacencyDecisionAnchor, adjacencyDecisionOther, adjacencyDecisionPeerNode} {
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:         id,
			Labels:     []string{"Decision"},
			Properties: map[string]interface{}{"identity": string(id)},
		})
		require.NoError(t, err)
	}
	return engine
}

// beginAdjacencyDecisionTransaction opens the mutation transaction both modes
// share: one namespace, implicit commits so the write-behind engine takes its
// buffered route.
func beginAdjacencyDecisionTransaction(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	return tx
}

// adjacencyDecisionRequest is the read a relationship mutation makes when it
// decides the range is empty: the current committed view, never the plain
// snapshot.
func adjacencyDecisionRequest(node NodeID, edgeType string) AdjacentEdgeRead {
	return AdjacentEdgeRead{Node: node, Types: []string{edgeType}, ReadPolicy: CurrentEdgeMatch}
}

// adjacencyDecisionSnapshotRequest is the ordinary read an unrelated statement
// makes: the transaction snapshot, no decision.
func adjacencyDecisionSnapshotRequest(node NodeID, edgeType string) AdjacentEdgeRead {
	return AdjacentEdgeRead{Node: node, Types: []string{edgeType}}
}

func adjacencyDecisionAbsences(t *testing.T, tx *BadgerTransaction) []AdjacentEdgeRead {
	t.Helper()
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return slices.Clone(tx.entityLocks.absent)
}

// adjacencyDecisionAnchorLock reports both halves of "write-owned": the entity
// write lock the mutation took, and whether a node image was staged. An empty
// SET-like lock has the first without the second.
func adjacencyDecisionAnchorLock(t *testing.T, tx *BadgerTransaction, anchor NodeID) (locked, hasNodeImage bool) {
	t.Helper()
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.entityLocks.find(entityWriteLockKey(bufferedNodeEntity, string(anchor))) != nil,
		tx.ownsEntityLocked(bufferedNodeEntity, string(anchor))
}

func adjacencyDecisionGuard(t *testing.T, tx *BadgerTransaction, ctx context.Context) error {
	t.Helper()
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.validateEmptyAdjacencyLocked(ctx)
}

func adjacencyDecisionAuthorityKeys(t *testing.T, tx *BadgerTransaction, ctx context.Context) ([]NodeID, []EdgeID, []NodeID, error) {
	t.Helper()
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.pendingEntityAuthorityKeysLocked(ctx)
}

func requireAdjacencyDecisionEdges(t *testing.T, engine *BadgerEngine, request AdjacentEdgeRead, want ...EdgeID) {
	t.Helper()
	edges, err := engine.ReadAdjacentEdges(t.Context(), request)
	require.NoError(t, err)
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	require.ElementsMatch(t, want, ids)
}

// createAdjacencyDecisionPeerEdge commits the peer's relationship. In the
// write-behind mode the peer is an implicit transaction whose acknowledged
// commit is still only in the buffer, and the guard must still see it; a
// relationship create takes no end-node body lock either way, so the peer
// commits under the decision's anchor lock.
func createAdjacencyDecisionPeerEdge(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine, id EdgeID, edgeType string, start, end NodeID) *Edge {
	t.Helper()
	edge := &Edge{
		ID:         id,
		StartNode:  start,
		EndNode:    end,
		Type:       edgeType,
		Properties: map[string]interface{}{"writer": "peer"},
	}
	if !mode.buffered {
		require.NoError(t, engine.CreateEdge(t.Context(), edge))
		return edge
	}
	peer := beginAdjacencyDecisionTransaction(t, engine)
	require.NoError(t, peer.CreateEdge(t.Context(), edge))
	require.NoError(t, peer.Commit())
	require.Positive(t, engine.writeBehind.PendingOps(), "the peer's acknowledged commit is still only buffered")
	return edge
}

// A peer relationship in the recorded range, committed after the decision's
// guard and anchor lock but before the commit owner accepts anything, refuses
// the transaction before acceptance and leaves the peer's graph intact.
func TestAdjacencyDecisionMatchingPeerRelationshipRefusesBeforeAcceptance(t *testing.T) {
	adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
		ctx := t.Context()
		tx := beginAdjacencyDecisionTransaction(t, engine)

		edges, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.NoError(t, err)
		require.Empty(t, edges, "the decision reads the committed range")
		require.Len(t, adjacencyDecisionAbsences(t, tx), 1, "the current-policy read records the empty range")

		// The mutation's SET finds no property to write: the anchor lock is
		// taken without a pending node image, so only the absence decision and
		// the lock make the anchor write-owned.
		_, err = tx.LockNodeForWrite(ctx, adjacencyDecisionAnchor)
		require.NoError(t, err)
		locked, hasNodeImage := adjacencyDecisionAnchorLock(t, tx, adjacencyDecisionAnchor)
		require.True(t, locked)
		require.False(t, hasNodeImage, "an empty SET takes the anchor lock without staging a node image")
		require.NoError(t, adjacencyDecisionGuard(t, tx, ctx), "the guard agrees while the range is still empty")

		// The transaction's unrelated write is what carries it to the commit
		// owner; it must not be accepted once the decision is false.
		_, err = tx.CreateNode(ctx, &Node{
			ID:         adjacencyDecisionStaged,
			Labels:     []string{"Staged"},
			Properties: map[string]interface{}{"writer": "transaction"},
		})
		require.NoError(t, err)

		// Between the decision and the final commit, a peer fills the range.
		peerEdge := createAdjacencyDecisionPeerEdge(t, mode, engine, "test:peer-link", adjacencyDecisionLink, adjacencyDecisionAnchor, adjacencyDecisionOther)
		requireAdjacencyDecisionEdges(t, engine, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink), peerEdge.ID)

		err = tx.Commit()
		require.ErrorIs(t, err, ErrConflict, "the final commit owner rechecks the write-owned decision")
		require.NotErrorIs(t, err, ErrDeadlock, "a changed decision is a genuine optimistic conflict, not a fabricated deadlock")
		require.Contains(t, err.Error(), string(adjacencyDecisionAnchor), "the refusal names the anchor")
		require.Contains(t, err.Error(), string(peerEdge.ID), "the refusal names the relationship that changed the decision")

		// Refused before acceptance: the transaction's own write is absent.
		_, err = engine.GetNode(ctx, adjacencyDecisionStaged)
		require.ErrorIs(t, err, ErrNotFound, "a refused commit accepts nothing of the transaction")

		// The peer's graph is preserved exactly.
		requireAdjacencyDecisionEdges(t, engine, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink), peerEdge.ID)
		committed, err := engine.GetEdge(ctx, peerEdge.ID)
		require.NoError(t, err)
		require.Equal(t, adjacencyDecisionLink, committed.Type)
		require.Equal(t, adjacencyDecisionAnchor, committed.StartNode)
		require.Equal(t, adjacencyDecisionOther, committed.EndNode)
		require.Equal(t, "peer", committed.Properties["writer"])
		anchor, err := engine.GetNode(ctx, adjacencyDecisionAnchor)
		require.NoError(t, err)
		require.Equal(t, string(adjacencyDecisionAnchor), anchor.Properties["identity"], "the locked anchor is unchanged")
	})
}

// The recorded range keeps its own Types: a caller that reuses its request
// cannot move or widen the decision, so the peer's LINK relationship still
// refuses the commit.
func TestAdjacencyDecisionRecordedRangeSurvivesCallerSliceReuse(t *testing.T) {
	adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
		ctx := t.Context()
		tx := beginAdjacencyDecisionTransaction(t, engine)

		types := []string{adjacencyDecisionLink}
		_, err := tx.ReadEdges(ctx, AdjacentEdgeRead{Node: adjacencyDecisionAnchor, Types: types, ReadPolicy: CurrentEdgeMatch})
		require.NoError(t, err)
		// The caller reuses its own slice for its next request.
		types[0] = adjacencyDecisionUnrelated
		types = append(types, adjacencyDecisionLink)

		absences := adjacencyDecisionAbsences(t, tx)
		require.Len(t, absences, 1)
		require.Equal(t, []string{adjacencyDecisionLink}, absences[0].Types, "the decision owns a copy of the caller's types")

		_, err = tx.LockNodeForWrite(ctx, adjacencyDecisionAnchor)
		require.NoError(t, err)
		_, err = tx.CreateNode(ctx, &Node{ID: adjacencyDecisionStaged})
		require.NoError(t, err)
		peerEdge := createAdjacencyDecisionPeerEdge(t, mode, engine, "test:peer-link", adjacencyDecisionLink, adjacencyDecisionAnchor, adjacencyDecisionOther)

		err = tx.Commit()
		require.ErrorIs(t, err, ErrConflict, "the guard still decides against the recorded LINK range")
		require.Contains(t, err.Error(), string(peerEdge.ID))
		_, err = engine.GetNode(ctx, adjacencyDecisionStaged)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

// Repeated reads of one empty range are one decision, a different range is a
// different decision, and an ordinary snapshot read is no decision at all.
func TestAdjacencyDecisionRangeOwnershipAndDeduplication(t *testing.T) {
	adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
		ctx := t.Context()
		tx := beginAdjacencyDecisionTransaction(t, engine)

		_, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.NoError(t, err)
		absences := adjacencyDecisionAbsences(t, tx)
		require.Len(t, absences, 1)
		require.Equal(t, []string{adjacencyDecisionLink}, absences[0].Types)
		require.True(t, absences[0].Headers, "the guard rechecks headers only")
		require.Equal(t, adjacencyDecisionAnchor, absences[0].Node)
		require.Equal(t, AdjacentOutgoing, absences[0].Direction)

		// The same range read again, as records and as headers, is the same
		// decision even though each request carries its own Types slice.
		_, err = tx.ReadEdges(ctx, AdjacentEdgeRead{Node: adjacencyDecisionAnchor, Types: []string{adjacencyDecisionLink}, ReadPolicy: CurrentEdgeMatch})
		require.NoError(t, err)
		_, err = tx.ReadEdges(ctx, AdjacentEdgeRead{Node: adjacencyDecisionAnchor, Types: []string{adjacencyDecisionLink}, Headers: true, ReadPolicy: CurrentEdgeMatch})
		require.NoError(t, err)
		require.Len(t, adjacencyDecisionAbsences(t, tx), 1, "repeated reads of one range are one observation")

		// A different type set is a different decision.
		_, err = tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionUnrelated))
		require.NoError(t, err)
		require.Len(t, adjacencyDecisionAbsences(t, tx), 2)

		// An ordinary snapshot read of an empty range decides nothing, in either
		// committed write mode.
		_, err = tx.ReadEdges(ctx, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.NoError(t, err)
		require.Len(t, adjacencyDecisionAbsences(t, tx), 2, "ordinary reads retain no decision state")
	})
}

// The transaction's own staged relationship is the range, not an absence, and
// a decision recorded before that relationship still accepts it: the guard
// rechecks only the committed range.
func TestAdjacencyDecisionOwnStagedRelationshipIsNotAnAbsence(t *testing.T) {
	t.Run("staged before the decision read", func(t *testing.T) {
		adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
			ctx := t.Context()
			tx := beginAdjacencyDecisionTransaction(t, engine)

			ownEdge := &Edge{ID: "test:own-link", StartNode: adjacencyDecisionAnchor, EndNode: adjacencyDecisionOther, Type: adjacencyDecisionLink, Properties: map[string]interface{}{"writer": "transaction"}}
			require.NoError(t, tx.CreateEdge(ctx, ownEdge))

			edges, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
			require.NoError(t, err)
			require.Len(t, edges, 1, "the read answers the transaction's own relationship")
			require.Equal(t, ownEdge.ID, edges[0].ID)
			require.Empty(t, adjacencyDecisionAbsences(t, tx), "a range the transaction itself fills is not an empty decision")

			require.NoError(t, tx.Commit())
			requireAdjacencyDecisionEdges(t, engine, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink), ownEdge.ID)
		})
	})
	t.Run("staged after the decision", func(t *testing.T) {
		adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
			ctx := t.Context()
			tx := beginAdjacencyDecisionTransaction(t, engine)

			_, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
			require.NoError(t, err)
			_, err = tx.LockNodeForWrite(ctx, adjacencyDecisionAnchor)
			require.NoError(t, err)
			require.Len(t, adjacencyDecisionAbsences(t, tx), 1)

			// The mutation now creates the relationship it decided was missing.
			ownEdge := &Edge{ID: "test:own-link", StartNode: adjacencyDecisionAnchor, EndNode: adjacencyDecisionOther, Type: adjacencyDecisionLink, Properties: map[string]interface{}{"writer": "transaction"}}
			require.NoError(t, tx.CreateEdge(ctx, ownEdge))
			edges, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
			require.NoError(t, err)
			require.Len(t, edges, 1, "the decision's own relationship answers its read")
			require.Len(t, adjacencyDecisionAbsences(t, tx), 1, "the transaction's own create adds no second decision")

			require.NoError(t, tx.Commit(), "the guard rechecks the committed range, which the transaction's own create does not fill")
			requireAdjacencyDecisionEdges(t, engine, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink), ownEdge.ID)
		})
	})
}

// Rolling back the transaction clears its recorded decisions and its anchor
// lock with the rest of its entity state.
func TestAdjacencyDecisionRollbackClearsRecordedDecisions(t *testing.T) {
	adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
		ctx := t.Context()
		tx := beginAdjacencyDecisionTransaction(t, engine)

		_, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.NoError(t, err)
		_, err = tx.LockNodeForWrite(ctx, adjacencyDecisionAnchor)
		require.NoError(t, err)
		require.Len(t, adjacencyDecisionAbsences(t, tx), 1)
		locked, _ := adjacencyDecisionAnchorLock(t, tx, adjacencyDecisionAnchor)
		require.True(t, locked)

		require.NoError(t, tx.Rollback())
		require.Empty(t, adjacencyDecisionAbsences(t, tx), "a rolled back transaction retains no decision")
		locked, hasNodeImage := adjacencyDecisionAnchorLock(t, tx, adjacencyDecisionAnchor)
		require.False(t, locked, "the transaction's entity write locks are released")
		require.False(t, hasNodeImage)
		_, err = tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.ErrorIs(t, err, ErrTransactionClosed)

		// The rolled back decision cannot refuse anyone: a peer fills the range.
		peerEdge := createAdjacencyDecisionPeerEdge(t, mode, engine, "test:peer-link", adjacencyDecisionLink, adjacencyDecisionAnchor, adjacencyDecisionOther)
		requireAdjacencyDecisionEdges(t, engine, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink), peerEdge.ID)
	})
}

// Cancellation ends a decision read without recording one, and the guard and
// the authority projection report the caller's own error unchanged.
func TestAdjacencyDecisionCancellationKeepsCallerError(t *testing.T) {
	adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
		ctx := t.Context()
		tx := beginAdjacencyDecisionTransaction(t, engine)

		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := tx.ReadEdges(canceled, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, adjacencyDecisionAbsences(t, tx), "a canceled read stages no decision")

		_, err = tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.NoError(t, err)
		_, err = tx.LockNodeForWrite(ctx, adjacencyDecisionAnchor)
		require.NoError(t, err)
		require.Len(t, adjacencyDecisionAbsences(t, tx), 1)

		require.NoError(t, adjacencyDecisionGuard(t, tx, ctx), "the live guard agrees while the range is empty")
		require.ErrorIs(t, adjacencyDecisionGuard(t, tx, canceled), context.Canceled, "a canceled guard reports the caller's error, not a conflict")
		_, _, _, err = adjacencyDecisionAuthorityKeys(t, tx, canceled)
		require.ErrorIs(t, err, context.Canceled, "the authority projection observes the caller's context")

		require.NoError(t, tx.Rollback())
	})
}

// A peer relationship of another type is outside the recorded range: the
// transaction still commits and both populations survive.
func TestAdjacencyDecisionUnrelatedTypeDoesNotConflict(t *testing.T) {
	adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
		ctx := t.Context()
		tx := beginAdjacencyDecisionTransaction(t, engine)

		_, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.NoError(t, err)
		_, err = tx.LockNodeForWrite(ctx, adjacencyDecisionAnchor)
		require.NoError(t, err)
		_, err = tx.CreateNode(ctx, &Node{ID: adjacencyDecisionStaged, Properties: map[string]interface{}{"writer": "transaction"}})
		require.NoError(t, err)

		peerEdge := createAdjacencyDecisionPeerEdge(t, mode, engine, "test:peer-other", adjacencyDecisionUnrelated, adjacencyDecisionAnchor, adjacencyDecisionOther)

		require.NoError(t, tx.Commit(), "the peer's relationship is outside the recorded type filter")

		staged, err := engine.GetNode(ctx, adjacencyDecisionStaged)
		require.NoError(t, err)
		require.Equal(t, "transaction", staged.Properties["writer"])
		if mode.buffered {
			require.Positive(t, engine.writeBehind.PendingOps(), "the transaction's own commit was acknowledged through the buffer")
		}
		requireAdjacencyDecisionEdges(t, engine, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		requireAdjacencyDecisionEdges(t, engine, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionUnrelated), peerEdge.ID)
	})
}

// A peer relationship in the opposite direction is outside the recorded range:
// the transaction still commits and both directions keep their own population.
func TestAdjacencyDecisionOppositeDirectionDoesNotConflict(t *testing.T) {
	adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
		ctx := t.Context()
		tx := beginAdjacencyDecisionTransaction(t, engine)

		_, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.NoError(t, err)
		require.Equal(t, AdjacentOutgoing, adjacencyDecisionAbsences(t, tx)[0].Direction)
		_, err = tx.LockNodeForWrite(ctx, adjacencyDecisionAnchor)
		require.NoError(t, err)
		_, err = tx.CreateNode(ctx, &Node{ID: adjacencyDecisionStaged})
		require.NoError(t, err)

		peerEdge := createAdjacencyDecisionPeerEdge(t, mode, engine, "test:peer-incoming", adjacencyDecisionLink, adjacencyDecisionPeerNode, adjacencyDecisionAnchor)

		require.NoError(t, tx.Commit(), "the recorded direction is the anchor's outgoing relationships only")

		_, err = engine.GetNode(ctx, adjacencyDecisionStaged)
		require.NoError(t, err)
		requireAdjacencyDecisionEdges(t, engine, adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		requireAdjacencyDecisionEdges(t, engine, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: adjacencyDecisionAnchor, Types: []string{adjacencyDecisionLink}}, peerEdge.ID)
	})
}

// A plain read records no decision and keeps the ordinary contract: an explicit
// direct transaction keeps its pinned snapshot, a write-behind transaction
// keeps its read-committed view, and neither refuses the commit.
func TestAdjacencyDecisionPlainSnapshotReadKeepsItsContract(t *testing.T) {
	adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
		ctx := t.Context()
		tx := beginAdjacencyDecisionTransaction(t, engine)
		pinned := adjacencyDecisionSnapshotRequest(adjacencyDecisionAnchor, adjacencyDecisionLink)

		edges, err := tx.ReadEdges(ctx, pinned)
		require.NoError(t, err)
		require.Empty(t, edges)
		require.Empty(t, adjacencyDecisionAbsences(t, tx), "an ordinary read records no decision")

		peerEdge := createAdjacencyDecisionPeerEdge(t, mode, engine, "test:peer-link", adjacencyDecisionLink, adjacencyDecisionAnchor, adjacencyDecisionOther)

		again, err := tx.ReadEdges(ctx, pinned)
		require.NoError(t, err)
		if mode.buffered {
			require.Len(t, again, 1, "a write-behind transaction reads the last committed adjacency")
			require.Equal(t, peerEdge.ID, again[0].ID)
		} else {
			require.Empty(t, again, "an ordinary explicit transaction keeps its pinned snapshot")
		}

		current, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
		require.NoError(t, err)
		require.Len(t, current, 1, "the current policy resolves the peer's relationship")
		require.Equal(t, peerEdge.ID, current[0].ID)
		require.Empty(t, adjacencyDecisionAbsences(t, tx), "a non-empty current read is no absence decision")

		_, err = tx.CreateNode(ctx, &Node{ID: adjacencyDecisionStaged, Properties: map[string]interface{}{"writer": "transaction"}})
		require.NoError(t, err)
		require.NoError(t, tx.Commit(), "no decision was made, so no decision can be refused")
		staged, err := engine.GetNode(ctx, adjacencyDecisionStaged)
		require.NoError(t, err)
		require.Equal(t, "transaction", staged.Properties["writer"])
	})
}

// The commit's final identity authority covers exactly the write-owned absence
// anchors: a lock-only anchor (no node image) is an entity identity, an anchor
// nobody write-owned contributes nothing, and an absence never becomes a shared
// endpoint identity with a blank ID.
func TestAdjacencyDecisionAuthorityKeysCoverOnlyWriteOwnedAnchors(t *testing.T) {
	t.Run("lock-only anchor is authority, observed non-owned anchor is not", func(t *testing.T) {
		adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
			ctx := t.Context()
			tx := beginAdjacencyDecisionTransaction(t, engine)

			_, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
			require.NoError(t, err)
			_, err = tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionOther, adjacencyDecisionLink))
			require.NoError(t, err)
			require.Len(t, adjacencyDecisionAbsences(t, tx), 2)

			_, err = tx.LockNodeForWrite(ctx, adjacencyDecisionAnchor)
			require.NoError(t, err)
			locked, hasNodeImage := adjacencyDecisionAnchorLock(t, tx, adjacencyDecisionAnchor)
			require.True(t, locked)
			require.False(t, hasNodeImage, "the authority comes from the empty SET-like lock, not a node image")

			nodes, edges, endpoints, err := adjacencyDecisionAuthorityKeys(t, tx, ctx)
			require.NoError(t, err)
			require.Equal(t, []NodeID{adjacencyDecisionAnchor}, nodes,
				"only the write-owned anchor is authority; the observed but unowned anchor is not")
			require.Empty(t, edges)
			require.Empty(t, endpoints, "an absence anchor never takes a shared endpoint identity")

			for _, id := range nodes {
				require.NotEmpty(t, id, "no blank entity identity")
			}
			for _, id := range edges {
				require.NotEmpty(t, id, "no blank entity identity")
			}
			for _, id := range endpoints {
				require.NotEmpty(t, id, "no blank endpoint identity")
			}

			// The commit builds these keys from the same tuple: the anchor's
			// node identity and the body key of the write it owns.
			keys, err := entityCommitKeys(ctx, entityAuthorityFootprint{nodes: nodes, edges: edges, endpoints: endpoints, nodeBodies: nodes, edgeBodies: edges})
			require.NoError(t, err)
			require.Equal(t, []uniqueConstraintLockKey{
				{entityKind: 1, entityID: string(adjacencyDecisionAnchor)},
				{entityKind: nodeWriteLockKind, entityID: string(adjacencyDecisionAnchor)},
			}, keys)

			require.NoError(t, tx.Rollback())
		})
	})
	t.Run("observed anchor without a write contributes no authority", func(t *testing.T) {
		adjacencyDecisionModes(t, func(t *testing.T, mode adjacencyDecisionMode, engine *BadgerEngine) {
			ctx := t.Context()
			tx := beginAdjacencyDecisionTransaction(t, engine)

			_, err := tx.ReadEdges(ctx, adjacencyDecisionRequest(adjacencyDecisionAnchor, adjacencyDecisionLink))
			require.NoError(t, err)
			require.Len(t, adjacencyDecisionAbsences(t, tx), 1)

			nodes, edges, endpoints, err := adjacencyDecisionAuthorityKeys(t, tx, ctx)
			require.NoError(t, err)
			require.Empty(t, nodes, "a read that acquired no write ownership holds no authority")
			require.Empty(t, edges)
			require.Empty(t, endpoints)

			require.NoError(t, tx.Rollback())
		})
	})
}

func TestAdjacencyDecisionIndexedRangesRetainExactCollisions(t *testing.T) {
	ctx := context.Background()
	tx := &BadgerTransaction{}
	requests := make([]AdjacentEdgeRead, 32)
	for i := range requests {
		requests[i] = AdjacentEdgeRead{Node: "anchor", Other: NodeID(fmt.Sprintf("other-%d", i)), Direction: AdjacentOutgoing, Types: []string{"LINK"}}
		require.NoError(t, tx.observeEmptyAdjacencyLocked(ctx, requests[i]))
	}
	for _, request := range requests {
		require.NoError(t, tx.observeEmptyAdjacencyLocked(ctx, request))
	}
	require.Len(t, tx.entityLocks.absent, len(requests))
	// Force a fingerprint collision without changing the production hash: the
	// primary bucket names a different exact range. Both observations must survive.
	collision := AdjacentEdgeRead{Node: "anchor", Other: "collision", Direction: AdjacentOutgoing, Types: []string{"LINK"}}
	key, err := adjacencyDecisionFingerprint(ctx, collision)
	require.NoError(t, err)
	tx.entityLocks.absenceIndex[key] = 0
	require.NoError(t, tx.observeEmptyAdjacencyLocked(ctx, collision))
	require.NoError(t, tx.observeEmptyAdjacencyLocked(ctx, collision))
	require.Len(t, tx.entityLocks.absent, 33)
	require.Equal(t, collision.Other, tx.entityLocks.absent[32].Other)
	require.Equal(t, []int{32}, tx.entityLocks.absenceCollisions[key])
	requests[0].Types[0] = "changed"
	require.Equal(t, "LINK", tx.entityLocks.absent[0].Types[0])
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, tx.observeEmptyAdjacencyLocked(canceled, collision), context.Canceled)
	require.Len(t, tx.entityLocks.absent, 33)
}
