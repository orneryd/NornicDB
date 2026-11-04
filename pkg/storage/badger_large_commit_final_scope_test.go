package storage

// PR #1682: cache revocation belongs to the final batch of a large commit.
//
// A preparation batch of a large commit is durable but unpublished: the
// commit's intent timestamp stays unfinished, so the published timestamp is
// below every preparation batch and no reader can see what they wrote. A
// preparation batch therefore supersedes no cached body a reader still holds,
// and its cache scope names no facts. The batch that also deletes the intent
// (largeCommit.finalTxn) revokes the commit's complete, retained fact set,
// once, at the commit's latest data timestamp, after the whole commit is
// durable and before the intent timestamp finishes.
//
// These tests observe public read outcomes, not only counters: what a reader
// gets back during preparation, what a delayed preimage fill may install after
// the revocation, and what the store returns once the commit publishes.

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const finalScopeNodes = 4000

const (
	finalScopeLabel      = "FinalScope"
	finalScopeEdgeType   = "FSNEXT"
	finalScopeKeepLabel  = "FinalScopeKeep"
	finalScopeKeepType   = "FSKEEP"
	finalScopeMovedLabel = "FinalScopeMoved"
	finalScopeBulkLabel  = "FinalScopeBulk"
)

func finalScopeNodeID(i int) NodeID { return NodeID(fmt.Sprintf("test:fs-%d", i)) }
func finalScopeEdgeID(i int) EdgeID { return EdgeID(fmt.Sprintf("test:fse-%d", i)) }

func finalScopeKeepA() NodeID { return "test:fs-keep-a" }
func finalScopeKeepB() NodeID { return "test:fs-keep-b" }
func finalScopeKeepE() EdgeID { return "test:fs-keep-e" }

// openFinalScopeEngine keeps the per-entity durable floors individually
// observable: these fixtures' populations stay below the retention limit
// instead of coarsening into the whole-store floor.
func openFinalScopeEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	b := openLargeCommitTestEngine(t, t.TempDir())
	b.nodeCacheMaxEntries = 1 << 16
	b.edgeCacheMaxItems = 1 << 16
	b.adjCacheMaxNodes = 1 << 16
	return b
}

// finalScopeSampleIndexes are the entities every pause re-reads. They are
// spread across the population so that at least one is already physically
// written by the first preparation batch of the update generation.
func finalScopeSampleIndexes(nodes int) []int {
	step := nodes / 8
	if step < 2 {
		step = 2
	}
	indexes := make([]int, 0, 9)
	for i := 1; i < nodes; i += step {
		indexes = append(indexes, i)
	}
	return append(indexes, nodes-1)
}

// finalScopeMissIndex is never read before the commit, so reading it during a
// preparation batch is a cache miss that must still produce the preimage. It
// sits between two samples, so it is part of the updated population without
// being warmed.
func finalScopeMissIndex(nodes int) int {
	step := nodes / 8
	if step < 2 {
		step = 2
	}
	return 1 + step/2
}

// seedFinalScopeGeneration commits the preimage generation: one labelled
// relationship chain long enough that updating all of it is larger than one
// Badger batch.
func seedFinalScopeGeneration(t *testing.T, b *BadgerEngine, nodes int) {
	t.Helper()
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	for i := 0; i < nodes; i++ {
		_, err := tx.CreateNode(t.Context(), &Node{
			ID:         finalScopeNodeID(i),
			Labels:     []string{finalScopeLabel},
			Properties: map[string]any{"v": int64(1), "pad": "0123456789abcdef"},
		})
		require.NoError(t, err)
		if i > 0 {
			require.NoError(t, tx.CreateEdge(t.Context(), &Edge{
				ID:         finalScopeEdgeID(i),
				StartNode:  finalScopeNodeID(i - 1),
				EndNode:    finalScopeNodeID(i),
				Type:       finalScopeEdgeType,
				Properties: map[string]any{"p": int64(1), "pad": "0123456789abcdef"},
			}))
		}
	}
	require.NoError(t, tx.Commit())
}

// seedFinalScopeUnrelated commits entities the update generation never touches.
func seedFinalScopeUnrelated(t *testing.T, b *BadgerEngine) {
	t.Helper()
	for _, id := range []NodeID{finalScopeKeepA(), finalScopeKeepB()} {
		_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{finalScopeKeepLabel}, Properties: map[string]any{"v": int64(7)}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(t.Context(), &Edge{ID: finalScopeKeepE(), StartNode: finalScopeKeepA(), EndNode: finalScopeKeepB(), Type: finalScopeKeepType, Properties: map[string]any{"p": int64(7)}}))
}

// finalScopeUpdateCommit is the generation under test: it updates every node
// and every relationship created by seedFinalScopeGeneration, so every
// preparation batch carries affected facts.
func finalScopeUpdateCommit(t *testing.T, b *BadgerEngine, nodes int, prefix string) error {
	t.Helper()
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	for i := 0; i < nodes; i++ {
		properties := map[string]any{"v": int64(2), "pad": "0123456789abcdef"}
		if prefix != "" {
			properties[prefix+strconv.Itoa(i)] = int64(2)
		}
		require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{
			After:  &Node{ID: finalScopeNodeID(i), Labels: []string{finalScopeLabel}, Properties: properties},
			Policy: ClientNodeEdits,
		}))
		if i > 0 {
			require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{
				ID:         finalScopeEdgeID(i),
				StartNode:  finalScopeNodeID(i - 1),
				EndNode:    finalScopeNodeID(i),
				Type:       finalScopeEdgeType,
				Properties: map[string]any{"p": int64(2), "pad": "0123456789abcdef"},
			}}))
		}
	}
	return tx.Commit()
}

// finalScopeCacheImages is one snapshot of the physical caches a commit may
// revoke, read from their owning maps (not from a helper's own counter).
type finalScopeCacheImages struct {
	nodes  map[NodeID]*Node
	edges  map[EdgeID]*Edge
	adj    map[NodeID][]EdgeID
	types  []*Edge
	label  NodeID
	floors map[NodeID]uint64
	efloor map[EdgeID]uint64
	all    uint64
	latest uint64
}

func captureFinalScopeCaches(b *BadgerEngine, nodes []NodeID, edges []EdgeID, label, edgeType string) finalScopeCacheImages {
	images := finalScopeCacheImages{
		nodes:  make(map[NodeID]*Node, len(nodes)),
		edges:  make(map[EdgeID]*Edge, len(edges)),
		adj:    make(map[NodeID][]EdgeID, len(nodes)),
		floors: make(map[NodeID]uint64, len(nodes)),
		efloor: make(map[EdgeID]uint64, len(edges)),
	}
	b.nodeCacheMu.RLock()
	for _, id := range nodes {
		images.nodes[id] = b.nodeCache[id]
	}
	b.nodeCacheMu.RUnlock()
	b.edgeCacheMu.RLock()
	for _, id := range edges {
		images.edges[id] = b.edgeCache[id]
	}
	b.edgeCacheMu.RUnlock()
	b.adjCacheMu.RLock()
	for _, id := range nodes {
		images.adj[id] = b.outgoingAdjCache[id]
	}
	b.adjCacheMu.RUnlock()
	b.edgeTypeCacheMu.RLock()
	images.types = b.edgeTypeCache[edgeType]
	b.edgeTypeCacheMu.RUnlock()
	b.labelFirstNodeCacheMu.RLock()
	images.label = b.labelFirstNodeCache[label]
	b.labelFirstNodeCacheMu.RUnlock()
	b.durableCaches.mu.Lock()
	for _, id := range nodes {
		images.floors[id] = b.durableCaches.nodeFloor(id)
	}
	for _, id := range edges {
		images.efloor[id] = b.durableCaches.edgeFloor(id)
	}
	images.all = b.durableCaches.all
	images.latest = b.durableCaches.latest
	b.durableCaches.mu.Unlock()
	return images
}

// warmFinalScopeCaches fills every physical cache family from public reads and
// returns the sampled ids. edges[i] belongs to nodes[i].
func warmFinalScopeCaches(t *testing.T, b *BadgerEngine, nodes int) ([]NodeID, []EdgeID, NodeID) {
	t.Helper()
	samples := finalScopeSampleIndexes(nodes)
	ids := make([]NodeID, 0, len(samples))
	edges := make([]EdgeID, 0, len(samples))
	for _, i := range samples {
		ids = append(ids, finalScopeNodeID(i))
		if i > 0 {
			edges = append(edges, finalScopeEdgeID(i))
		}
	}
	for _, i := range samples {
		_, err := b.GetNode(context.Background(), finalScopeNodeID(i))
		require.NoError(t, err)
		if i == 0 {
			continue
		}
		out, err := b.GetOutgoingEdges(context.Background(), finalScopeNodeID(i-1))
		require.NoError(t, err)
		require.Len(t, out, 1, "the chain fixture must have one outgoing relationship per node")
	}
	all, err := b.GetEdgesByType(context.Background(), finalScopeEdgeType)
	require.NoError(t, err)
	require.Len(t, all, nodes-1)
	require.NoError(t, b.ForEachNodeIDByLabel(context.Background(), finalScopeLabel, func(NodeID) bool { return false }))
	images := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)
	for _, id := range ids {
		require.NotNil(t, images.nodes[id], "fixture must observe a warm node body for %s", id)
	}
	for _, id := range edges {
		require.NotNil(t, images.edges[id], "fixture must observe a warm edge body for %s", id)
	}
	require.NotNil(t, images.types, "fixture must observe a warm relationship-type population")
	require.NotEmpty(t, images.label, "fixture must observe a warm label-first entry")
	return ids, edges, finalScopeNodeID(finalScopeMissIndex(nodes))
}

// warmFinalScopeUnrelated fills the caches of the entities the commit never
// touches, so a preparation batch that wrongly took the whole-store
// invalidation branch would drop them.
func warmFinalScopeUnrelated(t *testing.T, b *BadgerEngine) {
	t.Helper()
	for _, id := range []NodeID{finalScopeKeepA(), finalScopeKeepB()} {
		_, err := b.GetNode(context.Background(), id)
		require.NoError(t, err)
	}
	_, err := b.GetOutgoingEdges(context.Background(), finalScopeKeepA())
	require.NoError(t, err)
	keep, err := b.GetEdgesByType(context.Background(), finalScopeKeepType)
	require.NoError(t, err)
	require.Len(t, keep, 1)
	require.NoError(t, b.ForEachNodeIDByLabel(context.Background(), finalScopeKeepLabel, func(NodeID) bool { return false }))
}

// largeCommitIntentVisible reports whether the commit's intent record is still
// in the store. The batch that deletes it is the commit's final batch.
func largeCommitIntentVisible(b *BadgerEngine) bool {
	read := b.db.DB.NewTransactionAt(b.db.DB.MaxVersion(), false)
	defer read.Discard()
	_, err := read.Get(largeCommitIntentKey)
	return err == nil
}

// finalScopeDurable reports whether key's newest stored version is newer than
// published, i.e. written by an unpublished preparation batch.
func finalScopeDurable(b *BadgerEngine, key []byte, published uint64) bool {
	read := b.db.DB.NewTransactionAt(b.db.DB.MaxVersion(), false)
	defer read.Discard()
	item, err := read.Get(key)
	if err != nil {
		return false
	}
	return item.Version() > published
}

// finalScopePause is what one batch boundary of the commit looks like.
type finalScopePause struct {
	batch     int
	final     bool
	published uint64
	images    finalScopeCacheImages
	keep      finalScopeCacheImages
	// durable nodes/edges are already physically written at this pause.
	durableNodes []NodeID
	durableEdges []EdgeID
	// public node values read during the pause, the pinned view's values and
	// the cache-miss read's result.
	public map[NodeID]any
	pinned map[NodeID]any
	miss   any
}

func observeFinalScopePause(t *testing.T, b *BadgerEngine, pinned *BadgerTransaction, batch int, ids []NodeID, edges []EdgeID, missID NodeID) finalScopePause {
	t.Helper()
	pause := finalScopePause{
		batch:     batch,
		final:     !largeCommitIntentVisible(b),
		published: b.db.oracle.published.Load(),
		public:    make(map[NodeID]any, len(ids)),
		pinned:    make(map[NodeID]any, len(ids)),
	}
	for _, id := range ids {
		if finalScopeDurable(b, nodeKey(id), pause.published) {
			pause.durableNodes = append(pause.durableNodes, id)
		}
	}
	for _, id := range edges {
		if finalScopeDurable(b, edgeKey(id), pause.published) {
			pause.durableEdges = append(pause.durableEdges, id)
		}
	}
	// Cache images are captured before any read below can fill them.
	pause.images = captureFinalScopeCaches(b, append(ids, missID), edges, finalScopeLabel, finalScopeEdgeType)
	pause.keep = captureFinalScopeCaches(b, []NodeID{finalScopeKeepA(), finalScopeKeepB()}, []EdgeID{finalScopeKeepE()}, finalScopeKeepLabel, finalScopeKeepType)
	for _, id := range ids {
		node, err := b.GetNode(context.Background(), id)
		require.NoError(t, err)
		pause.public[id] = node.Properties["v"]
		viewNode, err := pinned.GetNode(context.Background(), id)
		require.NoError(t, err)
		pause.pinned[id] = viewNode.Properties["v"]
	}
	miss, err := b.GetNode(context.Background(), missID)
	require.NoError(t, err)
	pause.miss = miss.Properties["v"]
	return pause
}

// requirePreimageCaches asserts a pause still exposes the preimage the
// published timestamp entitles readers to, and that no cache entry was revoked
// for the entities this batch has already written durably.
func requirePreimageCaches(t *testing.T, batch int, before, pause finalScopeCacheImages, durableNodes []NodeID, durableEdges []EdgeID) {
	t.Helper()
	for _, id := range durableNodes {
		require.Equal(t, before.floors[id], pause.floors[id], "preparation batch %d advanced the node floor of %s", batch, id)
		require.Same(t, before.nodes[id], pause.nodes[id], "preparation batch %d revoked the node body of %s", batch, id)
	}
	for _, id := range durableEdges {
		require.Equal(t, before.efloor[id], pause.efloor[id], "preparation batch %d advanced the edge floor of %s", batch, id)
		require.Same(t, before.edges[id], pause.edges[id], "preparation batch %d revoked the edge body of %s", batch, id)
	}
	require.Equal(t, before.all, pause.all, "preparation batch %d coarsened the durable cache floor", batch)
	require.Equal(t, before.types, pause.types, "preparation batch %d revoked the relationship-type population", batch)
	require.Equal(t, before.label, pause.label, "preparation batch %d revoked the label-first entry", batch)
	for id, ids := range before.adj {
		require.Equal(t, ids, pause.adj[id], "preparation batch %d revoked the adjacency of %s", batch, id)
	}
}

// TestLargeCommitPrepBatchesKeepPublishedPreimages is proof gate 1: after
// preparation batches of a large commit are durable, affected and unrelated
// caches still hold their preimages, a cache-miss read still returns the
// published preimage, and a pinned view keeps reading it.
func TestLargeCommitPrepBatchesKeepPublishedPreimages(t *testing.T) {
	b := openFinalScopeEngine(t)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	const nodes = finalScopeNodes
	seedFinalScopeGeneration(t, b, nodes)
	seedFinalScopeUnrelated(t, b)
	ids, edges, missID := warmFinalScopeCaches(t, b, nodes)
	warmFinalScopeUnrelated(t, b)
	before := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)
	keepIDs := []NodeID{finalScopeKeepA(), finalScopeKeepB()}
	keepEdges := []EdgeID{finalScopeKeepE()}
	keep := captureFinalScopeCaches(b, keepIDs, keepEdges, finalScopeKeepLabel, finalScopeKeepType)

	pinned, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pinned.Rollback() })

	var pauses []finalScopePause
	restore := setLargeCommitBatchHook(func(batch int) error {
		pauses = append(pauses, observeFinalScopePause(t, b, pinned, batch, ids, edges, missID))
		return nil
	})
	t.Cleanup(restore)
	err = finalScopeUpdateCommit(t, b, nodes, "")
	restore()
	require.NoError(t, err)

	require.Greater(t, len(pauses), 2, "the fixture must cross more than one Badger batch")
	preps, finals := 0, 0
	sawDurablePreimage := false
	for _, pause := range pauses {
		// Every reader of the published timestamp, whatever its cache state,
		// gets the preimage while the commit is unpublished.
		for _, id := range ids {
			require.EqualValues(t, 1, pause.public[id], "batch %d exposed an unpublished image for %s", pause.batch, id)
			require.EqualValues(t, 1, pause.pinned[id], "batch %d exposed an unpublished image through the pinned view for %s", pause.batch, id)
		}
		require.EqualValues(t, 1, pause.miss, "batch %d: a cache miss must still read the published preimage", pause.batch)
		// Unrelated caches are never revoked by this commit.
		require.Same(t, keep.nodes[finalScopeKeepA()], pause.keep.nodes[finalScopeKeepA()], "batch %d revoked an unrelated node body", pause.batch)
		require.Same(t, keep.nodes[finalScopeKeepB()], pause.keep.nodes[finalScopeKeepB()], "batch %d revoked an unrelated node body", pause.batch)
		require.Equal(t, keep.adj[finalScopeKeepA()], pause.keep.adj[finalScopeKeepA()], "batch %d revoked unrelated adjacency", pause.batch)
		require.Equal(t, keep.types, pause.keep.types, "batch %d revoked the unrelated relationship-type population", pause.batch)
		require.Equal(t, keep.label, pause.keep.label, "batch %d revoked the unrelated label-first entry", pause.batch)
		if pause.final {
			finals++
			// This is the commit's one revocation: every sampled entity that
			// is durably written now carries this batch's timestamp.
			require.GreaterOrEqual(t, len(pause.durableNodes), len(ids)/2, "the final batch must observe the commit's durable facts")
			for _, id := range pause.durableNodes {
				require.Equal(t, pause.images.latest, pause.images.floors[id], "the final batch must revoke %s at its own timestamp", id)
				require.Nil(t, pause.images.nodes[id], "the final batch must drop the preimage body of %s", id)
			}
			for _, id := range pause.durableEdges {
				require.Equal(t, pause.images.latest, pause.images.efloor[id], "the final batch must revoke %s at its own timestamp", id)
				require.Nil(t, pause.images.edges[id], "the final batch must drop the preimage body of %s", id)
			}
			require.Nil(t, pause.images.nodes[missID], "the final batch must drop the preimage a cache miss installed")
			continue
		}
		preps++
		if len(pause.durableNodes) > 0 {
			sawDurablePreimage = true
			requirePreimageCaches(t, pause.batch, before, pause.images, pause.durableNodes, pause.durableEdges)
		}
		// Cache-miss reads during preparation are admitted preimages: the fill
		// owner's source is the published timestamp, which no preparation batch
		// has advanced the floors past.
		for _, id := range pause.durableNodes {
			require.NotNil(t, pause.images.nodes[id], "preparation batch %d must let the preimage of %s be cached", pause.batch, id)
			require.EqualValues(t, 1, pause.images.nodes[id].Properties["v"])
		}
		if preps > 1 {
			require.NotNil(t, pause.images.nodes[missID], "preparation batch %d must keep the preimage a cache miss read", pause.batch)
		}
	}
	require.Greater(t, preps, 1, "the fixture must observe several preparation batches")
	require.Equal(t, 1, finals, "exactly one batch of a large commit revokes the commit's facts")
	require.True(t, sawDurablePreimage, "the fixture must observe a durably written preimage before the final batch")

	// Publication: every current reader sees the final image; the pinned view
	// keeps the preimage it was admitted with.
	for _, id := range ids {
		node, readErr := b.GetNode(context.Background(), id)
		require.NoError(t, readErr)
		require.EqualValues(t, 2, node.Properties["v"], "%s must publish the final image", id)
		pinnedNode, readErr := pinned.GetNode(context.Background(), id)
		require.NoError(t, readErr)
		require.EqualValues(t, 1, pinnedNode.Properties["v"], "the pinned view keeps its admitted preimage for %s", id)
	}
	tail, err := b.GetEdgesByType(context.Background(), finalScopeEdgeType)
	require.NoError(t, err)
	require.Len(t, tail, nodes-1)
	for _, edge := range tail {
		require.EqualValues(t, 2, edge.Properties["p"], "the relationship population must publish the final image")
	}
	first, err := b.GetFirstNodeByLabel(context.Background(), finalScopeLabel)
	require.NoError(t, err)
	require.EqualValues(t, 2, first.Properties["v"])
	// Unrelated caches were never revoked.
	after := captureFinalScopeCaches(b, keepIDs, keepEdges, finalScopeKeepLabel, finalScopeKeepType)
	require.Same(t, keep.nodes[finalScopeKeepA()], after.nodes[finalScopeKeepA()])
	require.Same(t, keep.nodes[finalScopeKeepB()], after.nodes[finalScopeKeepB()])
	require.Equal(t, keep.adj[finalScopeKeepA()], after.adj[finalScopeKeepA()])
	require.Equal(t, keep.types, after.types)
	require.Equal(t, keep.label, after.label)
}

// finalScopeSupersedeCommit is one large commit that: updates test:fs-a's body
// and relabels it, moves and retypes test:fs-ab, and creates a bulk population
// so the commit crosses several Badger batches.
func finalScopeSupersedeCommit(t *testing.T, b *BadgerEngine, bulk int) error {
	t.Helper()
	ctx := context.Background()
	tx, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{
		After:  &Node{ID: "test:fs-a", Labels: []string{finalScopeMovedLabel}, Properties: map[string]any{"v": int64(2)}},
		Policy: ClientNodeEdits,
	}))
	require.NoError(t, tx.UpdateEdge(ctx, EdgeUpdateRequest{After: &Edge{
		ID: "test:fs-ab", StartNode: "test:fs-c", EndNode: "test:fs-b", Type: "FSTYPE2", Properties: map[string]any{"p": int64(2)},
	}}))
	for i := 0; i < bulk; i++ {
		_, err := tx.CreateNode(ctx, &Node{
			ID:         NodeID(fmt.Sprintf("test:fsbulk-%d", i)),
			Labels:     []string{finalScopeBulkLabel},
			Properties: map[string]any{"pad": "0123456789abcdef"},
		})
		require.NoError(t, err)
	}
	return tx.Commit()
}

// TestLargeCommitDelayedPreimageFillsCannotReinstallStaleImages is proof gate
// 2: a fill that started before the commit published carries the older read
// timestamp and generation, and the final revocation's floors reject it, so it
// cannot reinstall a superseded node body, relationship body, adjacency
// population, relationship-type population or label-first entry.
func TestLargeCommitDelayedPreimageFillsCannotReinstallStaleImages(t *testing.T) {
	b := openFinalScopeEngine(t)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()
	// A small preimage population the commit below genuinely supersedes.
	for _, id := range []NodeID{"test:fs-a", "test:fs-b", "test:fs-c"} {
		_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{finalScopeLabel}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:fs-ab", StartNode: "test:fs-a", EndNode: "test:fs-b", Type: finalScopeEdgeType, Properties: map[string]any{"p": int64(1)}}))
	require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:fs-bc", StartNode: "test:fs-b", EndNode: "test:fs-c", Type: finalScopeEdgeType, Properties: map[string]any{"p": int64(1)}}))

	// Warm the caches, then capture what a read starting now carries.
	_, err := b.GetNode(ctx, "test:fs-a")
	require.NoError(t, err)
	_, err = b.GetStoredEdge(ctx, "test:fs-ab")
	require.NoError(t, err)
	out, err := b.GetOutgoingEdges(ctx, "test:fs-a")
	require.NoError(t, err)
	require.Len(t, out, 1)
	typeRows, err := b.GetEdgesByType(ctx, finalScopeEdgeType)
	require.NoError(t, err)
	require.Len(t, typeRows, 2)
	require.NoError(t, b.ForEachNodeIDByLabel(ctx, finalScopeLabel, func(NodeID) bool { return false }))

	source := b.db.oracle.published.Load()
	stale := struct {
		nodeGen, edgeGen, adjGen, typeGen, labelGen uint64
		node                                        *Node
		edge                                        *Edge
		adjacency                                   []EdgeID
		typeRows                                    []*Edge
		label                                       NodeID
	}{
		nodeGen: b.nodeCacheGen.current(), edgeGen: b.edgeCacheGen.current(),
		adjGen: b.adjCacheGen.current(), typeGen: b.edgeTypeCacheGen.current(),
		labelGen:  b.labelFirstCacheGen.current(),
		node:      &Node{ID: "test:fs-a", Labels: []string{finalScopeLabel}, Properties: map[string]any{"v": int64(1)}},
		edge:      &Edge{ID: "test:fs-ab", StartNode: "test:fs-a", EndNode: "test:fs-b", Type: finalScopeEdgeType, Properties: map[string]any{"p": int64(1)}},
		adjacency: []EdgeID{"test:fs-ab"},
		typeRows:  typeRows,
	}
	b.labelFirstNodeCacheMu.RLock()
	stale.label = b.labelFirstNodeCache[finalScopeLabel]
	b.labelFirstNodeCacheMu.RUnlock()
	require.NotEmpty(t, stale.label)

	preimage := captureFinalScopeCaches(b, []NodeID{"test:fs-a"}, []EdgeID{"test:fs-ab"}, finalScopeLabel, finalScopeEdgeType)
	require.NotNil(t, preimage.nodes["test:fs-a"])
	require.NotNil(t, preimage.edges["test:fs-ab"])

	require.NoError(t, finalScopeSupersedeCommit(t, b, finalScopeNodes))

	// The commit revoked every affected family at its final timestamp.
	afterward := captureFinalScopeCaches(b, []NodeID{"test:fs-a"}, []EdgeID{"test:fs-ab"}, finalScopeLabel, finalScopeEdgeType)
	require.Greater(t, afterward.floors["test:fs-a"], source)
	require.Greater(t, afterward.efloor["test:fs-ab"], source)
	if node := afterward.nodes["test:fs-a"]; node != nil {
		require.EqualValues(t, 2, node.Properties["v"], "the commit published a preimage body")
	}
	if edge := afterward.edges["test:fs-ab"]; edge != nil {
		require.EqualValues(t, 2, edge.Properties["p"], "the commit published a preimage relationship")
	}

	// The delayed preimage fills complete now, after the revocation.
	b.cacheFillNode(stale.nodeGen, stale.node, nodeReadCacheFull, source)
	b.cacheFillEdgeAtVersion(stale.edgeGen, stale.edge, 0, source)
	b.adjCacheStore(stale.adjGen, "test:fs-a", stale.adjacency, source, true)
	b.edgeTypeCacheFill(stale.typeGen, finalScopeEdgeType, stale.typeRows, source)
	b.labelCacheSetFirst(stale.labelGen, finalScopeLabel, stale.label, source)

	installed := captureFinalScopeCaches(b, []NodeID{"test:fs-a"}, []EdgeID{"test:fs-ab"}, finalScopeLabel, finalScopeEdgeType)
	if node := installed.nodes["test:fs-a"]; node != nil {
		require.EqualValues(t, 2, node.Properties["v"], "a delayed preimage fill reinstalled a superseded node body")
		require.Equal(t, []string{finalScopeMovedLabel}, node.Labels, "a delayed preimage fill reinstalled superseded node labels")
	}
	if edge := installed.edges["test:fs-ab"]; edge != nil {
		require.EqualValues(t, 2, edge.Properties["p"], "a delayed preimage fill reinstalled a superseded relationship body")
		require.Equal(t, "test:fs-c", string(edge.StartNode), "a delayed preimage fill reinstalled superseded relationship endpoints")
	}
	require.NotEqual(t, stale.adjacency, installed.adj["test:fs-a"], "a delayed preimage fill reinstalled superseded adjacency")
	for _, row := range installed.types {
		require.NotEqual(t, finalScopeEdgeType, row.Type, "a delayed preimage fill reinstalled a superseded relationship-type population")
	}
	require.NotEqual(t, stale.label, installed.label, "a delayed preimage fill reinstalled a superseded label-first entry")

	// The public outcome is the final image, never the delayed preimage.
	node, err := b.GetNode(ctx, "test:fs-a")
	require.NoError(t, err)
	require.EqualValues(t, 2, node.Properties["v"])
	moved, err := b.GetStoredEdge(ctx, "test:fs-ab")
	require.NoError(t, err)
	require.EqualValues(t, 2, moved.Properties["p"])
	require.Equal(t, "test:fs-c", string(moved.StartNode))
	require.Equal(t, "FSTYPE2", moved.Type)
	fromA, err := b.GetOutgoingEdges(ctx, "test:fs-a")
	require.NoError(t, err)
	require.Empty(t, fromA, "the moved relationship must no longer be listed under its old start node")
	oldType, err := b.GetEdgesByType(ctx, finalScopeEdgeType)
	require.NoError(t, err)
	require.Len(t, oldType, 1, "the relationship-type population must drop the moved relationship")
	require.Equal(t, EdgeID("test:fs-bc"), oldType[0].ID)
	newType, err := b.GetEdgesByType(ctx, "FSTYPE2")
	require.NoError(t, err)
	require.Len(t, newType, 1)
	require.Equal(t, EdgeID("test:fs-ab"), newType[0].ID)
	first, err := b.GetFirstNodeByLabel(ctx, finalScopeLabel)
	require.NoError(t, err)
	require.NotEqual(t, NodeID("test:fs-a"), first.ID, "the relabelled node cannot be the label's first node")
}

// TestLargeCommitFillsAreAdmittedAtTheCommitsOwnTimestamp shows the rejection
// above is staleness, not a disabled cache: the same owners accept a fill that
// carries the commit's own data timestamp.
func TestLargeCommitFillsAreAdmittedAtTheCommitsOwnTimestamp(t *testing.T) {
	b := openFinalScopeEngine(t)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	const nodes = finalScopeNodes
	seedFinalScopeGeneration(t, b, nodes)
	ids, edges, _ := warmFinalScopeCaches(t, b, nodes)
	require.NoError(t, finalScopeUpdateCommit(t, b, nodes, ""))

	images := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)
	require.NotZero(t, images.latest)
	fill := &Node{ID: ids[0], Labels: []string{finalScopeLabel}, Properties: map[string]any{"v": int64(2), "pad": "0123456789abcdef"}}
	b.cacheFillNode(b.nodeCacheGen.current(), fill, nodeReadCacheFull, images.latest)
	edgeFill := &Edge{ID: edges[2], StartNode: finalScopeNodeID(1), EndNode: finalScopeNodeID(2), Type: finalScopeEdgeType, Properties: map[string]any{"p": int64(2), "pad": "0123456789abcdef"}}
	b.cacheFillEdgeAtVersion(b.edgeCacheGen.current(), edgeFill, 0, images.latest)
	b.adjCacheStore(b.adjCacheGen.current(), finalScopeNodeID(1), []EdgeID{edges[2]}, images.latest, true)
	control := captureFinalScopeCaches(b, []NodeID{ids[0]}, []EdgeID{edges[2]}, finalScopeLabel, finalScopeEdgeType)
	require.NotNil(t, control.nodes[ids[0]], "the node fill owner must admit a fill at the commit's own timestamp")
	require.NotNil(t, control.edges[edges[2]], "the edge fill owner must admit a fill at the commit's own timestamp")
	require.Contains(t, control.adj, finalScopeNodeID(1))
}
