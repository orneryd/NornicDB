package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// A commit that wrote nothing has an empty cache scope, not an unknown one.
// These tests keep the two halves of that decision honest with real cache
// warmth: a read-only explicit commit must leave the caches installed by
// earlier reads alone, and a write the engine cannot classify must still
// revoke every representation.

type warmCacheState struct {
	nodeGen, edgeGen, edgeTypeGen, adjGen, labelFirstGen uint64
	nodes, edges, edgeTypes, outgoing, incoming          int
	labelFirst                                           int
	nodeImage                                            *Node
}

func snapshotWarmCaches(engine *BadgerEngine) warmCacheState {
	state := warmCacheState{
		nodeGen:     engine.nodeCacheGen.current(),
		edgeGen:     engine.edgeCacheGen.current(),
		edgeTypeGen: engine.edgeTypeCacheGen.current(),
		adjGen:      engine.adjCacheGen.current(),
	}
	engine.nodeCacheMu.RLock()
	state.nodes = len(engine.nodeCache)
	state.nodeImage = engine.nodeCache["caches:a"]
	engine.nodeCacheMu.RUnlock()
	engine.edgeCacheMu.RLock()
	state.edges = len(engine.edgeCache)
	engine.edgeCacheMu.RUnlock()
	engine.edgeTypeCacheMu.RLock()
	state.edgeTypes = len(engine.edgeTypeCache)
	engine.edgeTypeCacheMu.RUnlock()
	engine.adjCacheMu.RLock()
	state.outgoing = len(engine.outgoingAdjCache)
	state.incoming = len(engine.incomingAdjCache)
	engine.adjCacheMu.RUnlock()
	engine.labelFirstNodeCacheMu.RLock()
	state.labelFirstGen = engine.labelFirstCacheGen.current()
	state.labelFirst = len(engine.labelFirstNodeCache)
	engine.labelFirstNodeCacheMu.RUnlock()
	return state
}

// warmCacheFixture creates one labelled node, one unlabelled node and one edge,
// then reads through every cache the commit scope governs so each holds a real
// body installed by a read.
func warmCacheFixture(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		runtimeFixtureOwner.Own(t, engine)
	}
	require.NoError(t, err)
	require.True(t, waitForLabelIndexReady(t, engine, 5*time.Second), "label index backfill did not finish")
	_, err = engine.CreateNode(t.Context(), &Node{ID: "caches:a", Labels: []string{"Keep"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	require.True(t, waitForLabelIndexReady(t, engine, 5*time.Second), "label-index backfill must finish before cache generation assertions")
	_, err = engine.CreateNode(t.Context(), &Node{ID: "caches:b"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "caches:r", StartNode: "caches:a", EndNode: "caches:b", Type: "KEEP"}))
	node, err := engine.GetNode(t.Context(), "caches:a")
	require.NoError(t, err)
	require.NotNil(t, node)
	first, err := engine.GetFirstNodeByLabel(t.Context(), "Keep")
	require.NoError(t, err)
	require.NotNil(t, first)
	edges, err := engine.GetEdgesByType(context.Background(), "KEEP")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	var streamedFirst NodeID
	require.NoError(t, engine.ForEachNodeIDByLabel(context.Background(), "Keep", func(id NodeID) bool {
		streamedFirst = id
		return false
	}))
	require.Equal(t, NodeID("caches:a"), streamedFirst)
	_, err = engine.GetEdge(t.Context(), "caches:r")
	require.NoError(t, err)
	_, err = engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: "caches:a"})
	require.NoError(t, err)
	_, err = engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "caches:b"})
	require.NoError(t, err)
	state := snapshotWarmCaches(engine)
	require.NotZero(t, state.edges)
	require.NotZero(t, state.outgoing)
	require.NotZero(t, state.incoming)
	require.NotEmpty(t, state.nodeImage, "fixture read did not warm the node cache")
	require.NotZero(t, state.edgeTypes, "fixture read did not warm the edge type cache")
	require.NotZero(t, state.labelFirst, "fixture read did not warm the label first-node cache")
	return engine
}

// readOnlyTransaction reads one stored node and commits without writing.
func readOnlyTransaction(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, getErr := tx.GetNode(t.Context(), id)
	require.NoError(t, getErr)
	require.NoError(t, tx.Commit())
}

// requireWarmCachesUnchanged pins the shared cache contract for metadata-only
// and empty publications, including identity of the resident node image.
func requireWarmCachesUnchanged(t *testing.T, before, after warmCacheState) {
	t.Helper()
	require.Equal(t, before, after, "publication changed an unaffected cache")
	require.Same(t, before.nodeImage, after.nodeImage, "publication rebuilt an unaffected node cache image")
}

// TestReadOnlyTransactionCommitKeepsWarmCaches is the read-in-transaction
// regression: BEGIN, a read, COMMIT. The commit moved no entity and no raw key,
// so it must not recreate the capacity-sized caches nor advance their
// generations.
func TestReadOnlyTransactionCommitKeepsWarmCaches(t *testing.T) {
	engine := warmCacheFixture(t)
	before := snapshotWarmCaches(engine)

	readOnlyTransaction(t, engine, "caches:a")

	requireWarmCachesUnchanged(t, before, snapshotWarmCaches(engine))
}

// TestReadOnlyTransactionCommitKeepsLaterInvalidation proves the empty commit
// did not poison the durable cache floor: a later write still revokes the body
// that the empty commit left warm, so reads cannot return a superseded image.
func TestReadOnlyTransactionCommitKeepsLaterInvalidation(t *testing.T) {
	engine := warmCacheFixture(t)

	readOnlyTransaction(t, engine, "caches:a")

	require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{
		After:  &Node{ID: "caches:a", Labels: []string{"Keep"}, Properties: map[string]any{"v": int64(2)}},
		Policy: ClientNodeEdits,
	}))
	node, err := engine.GetNode(t.Context(), "caches:a")
	require.NoError(t, err)
	require.Equal(t, int64(2), node.Properties["v"], "read after write returned a superseded image")

	require.NoError(t, engine.DeleteNode(t.Context(), "caches:a"))
	_, err = engine.GetNode(t.Context(), "caches:a")
	require.ErrorIs(t, err, ErrNotFound, "read after delete returned a superseded image")
}

// TestUnknownRawTransactionWriteInvalidatesCaches keeps the conservative side
// of the decision: a raw key the projection cannot classify still revokes every
// representation, which is what an unknown writer requires.
func TestUnknownRawTransactionWriteInvalidatesCaches(t *testing.T) {
	engine := warmCacheFixture(t)
	before := snapshotWarmCaches(engine)

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("caches"))
	tx.bufferSet([]byte{0xFE, 'r', 'a', 'w'}, []byte("value"))
	require.NoError(t, tx.Commit())

	after := snapshotWarmCaches(engine)
	require.Greater(t, after.nodeGen, before.nodeGen, "unclassified raw write left the node cache warm")
	require.Greater(t, after.edgeGen, before.edgeGen, "unclassified raw write left the edge cache warm")
	require.Greater(t, after.edgeTypeGen, before.edgeTypeGen, "unclassified raw write left the edge type cache warm")
	require.Greater(t, after.labelFirstGen, before.labelFirstGen, "unclassified raw write left the label first-node cache warm")
	require.Empty(t, after.nodeImage, "unclassified raw write left a node body cached")
}

func TestReadOnlyCommitPreservesPublicationDecision(t *testing.T) {
	for _, veto := range []bool{false, true} {
		t.Run(map[bool]string{false: "accept", true: "reject"}[veto], func(t *testing.T) {
			engine := warmCacheFixture(t)
			before := snapshotWarmCaches(engine)
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			_, err = tx.GetNode(t.Context(), "caches:a")
			require.NoError(t, err)
			rejected := errors.New("read-only publication rejected")
			calls := 0
			var ctx context.Context
			ctx = WithPublicationDecision(context.Background(), func(actual context.Context, id PublicationIdentity, action func() error) error {
				calls++
				require.Same(t, ctx, actual)
				require.NotEqual(t, PublicationIdentity{}, id)
				if veto {
					return rejected
				}
				return action()
			})
			err = tx.CommitContext(ctx)
			if veto {
				require.ErrorIs(t, err, rejected)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, calls)
			require.False(t, tx.IsActive())
			after := snapshotWarmCaches(engine)
			require.Equal(t, before.nodeGen, after.nodeGen)
			require.Same(t, before.nodeImage, after.nodeImage)
		})
	}
}
