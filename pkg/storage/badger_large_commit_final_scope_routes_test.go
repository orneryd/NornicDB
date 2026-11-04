package storage

// PR #1682 proof gates 3-5: the large commit's complete fact set is evaluated
// exactly once, by the batch that deletes the intent, at the commit's latest
// data timestamp; failures keep the preimage and never publish the commit's
// private images; and every other large-commit route (transaction commit,
// buffered replay, retained snapshot DROP, an unknown scope and floor
// coarsening) keeps the same phase boundary.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// finalScopeStaticFacts is a fixed fact set for the batch-owner tests.
type finalScopeStaticFacts []AffectedFact

func (facts finalScopeStaticFacts) VisitAffectedFacts(visit func(AffectedFact)) {
	for _, fact := range facts {
		visit(fact)
	}
}

// finalScopeAction is one durable cache action of a commit: a preparation
// batch, the final batch, or a following ordinary commit.
type finalScopeAction struct {
	ts        uint64
	latest    uint64
	all       uint64
	visited   bool // this action visited the scope's facts
	invalidAt bool // this action took the whole-store invalidation
}

// recordFinalScopeActions records every durable cache action while the
// returned stop func is not called.
func recordFinalScopeActions(t *testing.T, b *BadgerEngine) (*[]finalScopeAction, func()) {
	actions := new([]finalScopeAction)
	restore := setDurableCacheHook(func(ts uint64) {
		b.durableCaches.mu.Lock()
		action := finalScopeAction{ts: ts, latest: b.durableCaches.latest, all: b.durableCaches.all}
		for _, floors := range []map[NodeID]uint64{b.durableCaches.nodes, b.durableCaches.adjacency} {
			for _, floor := range floors {
				if floor == ts {
					action.visited = true
				}
			}
		}
		for _, floors := range []map[EdgeID]uint64{b.durableCaches.edges} {
			for _, floor := range floors {
				if floor == ts {
					action.visited = true
				}
			}
		}
		for _, floors := range []map[string]uint64{b.durableCaches.types, b.durableCaches.labels} {
			for _, floor := range floors {
				if floor == ts {
					action.visited = true
				}
			}
		}
		action.invalidAt = b.durableCaches.all == ts
		b.durableCaches.mu.Unlock()
		*actions = append(*actions, action)
	})
	t.Cleanup(restore)
	return actions, restore
}

// finalScopeBatch is one large-commit batch boundary.
type finalScopeBatch struct {
	actions    int    // durable actions so far, this batch included
	ts         uint64 // this batch's durable cache action
	final      bool
	visited    bool // the batch's own action visited facts
	invalidate bool // the batch's own action took full invalidation
	all        uint64
	keepWarm   bool
}

// recordFinalScopeBatches records every batch boundary of a large commit.
func recordFinalScopeBatches(t *testing.T, b *BadgerEngine, actions *[]finalScopeAction, keep NodeID) (*[]finalScopeBatch, func()) {
	batches := new([]finalScopeBatch)
	restore := setLargeCommitBatchHook(func(batch int) error {
		require.NotEmpty(t, *actions)
		last := (*actions)[len(*actions)-1]
		b.durableCaches.mu.Lock()
		all := b.durableCaches.all
		b.durableCaches.mu.Unlock()
		b.nodeCacheMu.RLock()
		_, warm := b.nodeCache[keep]
		b.nodeCacheMu.RUnlock()
		*batches = append(*batches, finalScopeBatch{
			actions: len(*actions), ts: last.ts, final: !largeCommitIntentVisible(b),
			visited: last.visited, invalidate: all == last.ts, all: all, keepWarm: warm,
		})
		return nil
	})
	t.Cleanup(restore)
	return batches, restore
}

// requireSingleFinalRevocation asserts the phase contract: only the final
// batch revokes, exactly once, at its own timestamp.
func requireSingleFinalRevocation(t *testing.T, batches []finalScopeBatch, actions []finalScopeAction, minBatches int) finalScopeBatch {
	t.Helper()
	require.GreaterOrEqual(t, len(batches), minBatches, "the fixture must cross more than one Badger batch")
	final := batches[len(batches)-1]
	require.True(t, final.final, "the last batch must be the one that deletes the commit's intent")
	for _, batch := range batches[:len(batches)-1] {
		require.False(t, batch.final, "only the batch that deletes the intent is final")
		require.False(t, batch.visited, "preparation batch %d revoked the commit's facts", batch.actions)
		require.False(t, batch.invalidate, "preparation batch %d took the whole-store invalidation", batch.actions)
	}
	require.True(t, final.visited || final.invalidate, "the final batch must revoke named facts or the whole store")
	visited := 0
	for _, action := range actions {
		if action.visited || action.invalidAt {
			visited++
		}
	}
	require.Equal(t, 1, visited, "the commit's complete fact set must be evaluated exactly once")
	return final
}

// TestLargeCommitFinalScopeEvaluatedOnceAtFinalTimestamp is proof gate 3: with
// the statement's new property-key tokens persisted as preparation batches,
// the commit's complete fact set is evaluated exactly once, by the batch that
// deletes the intent, at the commit's latest data timestamp.
func TestLargeCommitFinalScopeEvaluatedOnceAtFinalTimestamp(t *testing.T) {
	b := openFinalScopeEngine(t)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	const nodes = finalScopeNodes
	seedFinalScopeGeneration(t, b, nodes)
	seedFinalScopeUnrelated(t, b)
	ids, edges, _ := warmFinalScopeCaches(t, b, nodes)
	warmFinalScopeUnrelated(t, b)
	before := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)

	actions, stopActions := recordFinalScopeActions(t, b)
	batches, stopBatches := recordFinalScopeBatches(t, b, actions, finalScopeKeepA())
	err := finalScopeUpdateCommit(t, b, nodes, "pr1682_final_scope_")
	stopBatches()
	stopActions()
	require.NoError(t, err)

	require.GreaterOrEqual(t, (*batches)[0].actions, 2, "the statement's new property-key tokens must be durable ahead of the first data batch")
	require.False(t, (*batches)[0].visited, "the property-key token batch revoked the commit's facts")
	final := requireSingleFinalRevocation(t, *batches, *actions, 3)
	// Counter persistence may commit metadata after the graph publication.
	// Its timestamp must not replace the final graph batch's cache floors.
	for _, action := range *actions {
		if action.ts > final.ts {
			require.False(t, action.visited, "later metadata must not revoke graph facts again")
			require.False(t, action.invalidAt, "later metadata must not invalidate the whole store")
		}
	}

	// The facts are retained until that batch: the final action's floors name
	// the commit's own entities at its own timestamp.
	after := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)
	for _, id := range ids {
		require.Equal(t, final.ts, after.floors[id], "the final batch must revoke %s at its own timestamp", id)
	}
	for _, id := range edges {
		require.Equal(t, final.ts, after.efloor[id], "the final batch must revoke %s at its own timestamp", id)
	}
	// Preparation batches left the preimage caches alone.
	for _, batch := range (*batches)[:len(*batches)-1] {
		require.True(t, batch.keepWarm, "preparation batch %d revoked an unrelated cached body", batch.actions)
	}
	require.False(t, before.latest == final.ts)

	// Publication: the same public reads now return the final image.
	for _, id := range ids {
		node, readErr := b.GetNode(context.Background(), id)
		require.NoError(t, readErr)
		require.EqualValues(t, 2, node.Properties["v"])
	}
}

// TestLargeCommitFinalBatchSelectionAfterIntentDeleteSpill is the spill half
// of proof gate 3: when the intent's deletion does not fit the open batch, the
// deletion's batch becomes the final one, and the commit's facts are revoked
// at that batch's timestamp, not at the full preparation batch's.
func TestLargeCommitFinalBatchSelectionAfterIntentDeleteSpill(t *testing.T) {
	b := openFinalScopeEngine(t)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()
	_, err := b.CreateNode(ctx, &Node{ID: "test:spill", Labels: []string{finalScopeLabel}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = b.GetNode(ctx, "test:spill")
	require.NoError(t, err)
	preimage := captureFinalScopeCaches(b, []NodeID{"test:spill"}, nil, finalScopeLabel, finalScopeEdgeType)
	require.NotNil(t, preimage.nodes["test:spill"])

	actions, stopActions := recordFinalScopeActions(t, b)
	batches, stopBatches := recordFinalScopeBatches(t, b, actions, "test:spill")

	lc, err := b.db.beginLargeCommitContext(ctx)
	require.NoError(t, err)
	w := &batchWriter{txn: lc.openBatch(), to: lc}
	defer w.discard()
	// Fill the open batch to Badger's limit, so the intent's deletion cannot
	// fit it and finish must spill a preparation batch first.
	filler := make([]byte, 1024)
	for i := 0; ; i++ {
		setErr := w.txn.Set([]byte(fmt.Sprintf("test:spillfill-%08d", i)), filler)
		if errors.Is(setErr, badger.ErrTxnTooBig) {
			break
		}
		require.NoError(t, setErr)
		require.Less(t, i, 1<<20, "the fixture must fill the open batch")
	}
	require.ErrorIs(t, w.txn.Set([]byte("test:spillprobe"), []byte("x")), badger.ErrTxnTooBig,
		"the open batch must be too full to take the intent's deletion")
	lc.cacheScope = func() cacheWriteScope {
		return cacheWriteScope{known: true, facts: finalScopeStaticFacts{{NodeID: "test:spill", Label: finalScopeLabel}}}
	}
	require.NoError(t, lc.finish(w))
	stopBatches()
	stopActions()

	require.Equal(t, 2, len(*batches), "the intent's deletion must have spilled one preparation batch")
	require.False(t, (*batches)[0].visited, "the spilled preparation batch revoked the commit's facts")
	require.True(t, (*batches)[1].visited, "the deletion's own batch must revoke the commit's facts")
	require.True(t, (*batches)[1].final)
	require.Equal(t, 1, countFinalScopeVisited(*actions), "the commit's facts must be revoked exactly once")
	require.Greater(t, (*batches)[1].ts, (*batches)[0].ts, "the deletion's batch must be newer than the spilled batch")
	after := captureFinalScopeCaches(b, []NodeID{"test:spill"}, nil, finalScopeLabel, finalScopeEdgeType)
	require.Equal(t, (*batches)[1].ts, after.floors["test:spill"], "the revocation must carry the deletion batch's timestamp")
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		_, getErr := txn.Get(largeCommitIntentKey)
		require.ErrorIs(t, getErr, badger.ErrKeyNotFound)
		_, fillErr := txn.Get([]byte("test:spillfill-00000000"))
		require.NoError(t, fillErr, "the published commit must expose the spilled preparation batch")
		return nil
	}))
	// The commit did not change the node, so the read after publication is
	// still the same body.
	node, err := b.GetNode(ctx, "test:spill")
	require.NoError(t, err)
	require.EqualValues(t, 1, node.Properties["v"])
}

func finalScopeFloorKeys(b *BadgerEngine) int {
	b.durableCaches.mu.Lock()
	defer b.durableCaches.mu.Unlock()
	return len(b.durableCaches.nodes) + len(b.durableCaches.edges) + len(b.durableCaches.adjacency) + len(b.durableCaches.types) + len(b.durableCaches.labels)
}

func countFinalScopeVisited(actions []finalScopeAction) int {
	visited := 0
	for _, action := range actions {
		if action.visited {
			visited++
		}
	}
	return visited
}

// TestLargeCommitFailureKeepsPreimagesAndRejectsPrivateImages is proof gate 4:
// a large commit that fails at a preparation batch or at its final batch rolls
// back, its unpublished images never reach a cache, and every public read
// still returns the preimage.
func TestLargeCommitFailureKeepsPreimagesAndRejectsPrivateImages(t *testing.T) {
	const nodes = finalScopeNodes
	probe := openFinalScopeEngine(t)
	seedFinalScopeGeneration(t, probe, nodes)
	batches := countBatches(t)
	require.NoError(t, finalScopeUpdateCommit(t, probe, nodes, ""))
	total := batches.Load()
	require.NoError(t, probe.Close())
	require.Greater(t, total, int64(2))

	for _, failAt := range []int64{1, total} {
		t.Run(fmt.Sprintf("fail at batch %d of %d", failAt, total), func(t *testing.T) {
			b := openFinalScopeEngine(t)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			seedFinalScopeGeneration(t, b, nodes)
			seedFinalScopeUnrelated(t, b)
			ids, edges, _ := warmFinalScopeCaches(t, b, nodes)
			warmFinalScopeUnrelated(t, b)
			preimage := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)
			require.NotNil(t, preimage.nodes[ids[0]], "the fixture must observe the preimage the failed commit supersedes")

			injected := errors.New("injected large-commit batch failure")
			restore := setLargeCommitBatchHook(func(batch int) error {
				if int64(batch) == failAt {
					return injected
				}
				return nil
			})
			t.Cleanup(restore)
			err := finalScopeUpdateCommit(t, b, nodes, "")
			restore()
			require.ErrorIs(t, err, injected)
			requireNoLargeCommitIntent(t, b)

			// Public reads still return the preimage.
			for _, id := range ids {
				node, readErr := b.GetNode(context.Background(), id)
				require.NoError(t, readErr)
				require.EqualValues(t, 1, node.Properties["v"], "a failed commit published %s", id)
			}
			tail, err := b.GetEdgesByType(context.Background(), finalScopeEdgeType)
			require.NoError(t, err)
			require.Len(t, tail, nodes-1)
			for _, edge := range tail {
				require.EqualValues(t, 1, edge.Properties["p"], "a failed commit published a relationship image")
			}
			first, err := b.GetFirstNodeByLabel(context.Background(), finalScopeLabel)
			require.NoError(t, err)
			require.EqualValues(t, 1, first.Properties["v"])

			// No cache holds the failed commit's private image.
			after := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)
			for _, id := range ids {
				if node := after.nodes[id]; node != nil {
					require.EqualValues(t, 1, node.Properties["v"], "the failed commit's private image reached the node cache")
				}
			}
			for _, id := range edges {
				if edge := after.edges[id]; edge != nil {
					require.EqualValues(t, 1, edge.Properties["p"], "the failed commit's private image reached the edge cache")
				}
			}
			for _, row := range after.types {
				require.EqualValues(t, 1, row.Properties["p"], "the failed commit's private image reached the relationship-type population")
			}
			for id, adj := range after.adj {
				for _, edgeID := range adj {
					require.Contains(t, edges, edgeID, "the failed commit's private adjacency reached the cache for %s", id)
				}
			}

			// The store remains usable and the same commit succeeds afterwards.
			_, err = b.CreateNode(context.Background(), &Node{ID: "test:fs-after-failure", Labels: []string{finalScopeKeepLabel}})
			require.NoError(t, err)
			require.NoError(t, finalScopeUpdateCommit(t, b, nodes, ""))
			node, err := b.GetNode(context.Background(), ids[0])
			require.NoError(t, err)
			require.EqualValues(t, 2, node.Properties["v"])
		})
	}
}

// TestWriteBehindLargeGenerationRevokesOnceAtFinalBatch is the buffered-replay
// part of proof gate 5: replaying a generation that needs several Badger
// batches revokes the generation's facts once, at the final batch, and leaves
// unrelated caches warm through every preparation batch.
func TestWriteBehindLargeGenerationRevokesOnceAtFinalBatch(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{lowMemory: true, wal: true})
	b.nodeCacheMaxEntries = 1 << 16
	b.edgeCacheMaxItems = 1 << 16
	b.adjCacheMaxNodes = 1 << 16
	ctx := context.Background()
	seedFinalScopeUnrelated(t, b)
	require.NoError(t, b.FlushWriteBehind(ctx))
	warmFinalScopeUnrelated(t, b)
	keep := captureFinalScopeCaches(b, []NodeID{finalScopeKeepA()}, []EdgeID{finalScopeKeepE()}, finalScopeKeepLabel, finalScopeKeepType)
	require.NotNil(t, keep.nodes[finalScopeKeepA()])

	// One buffered generation, large enough that its replay needs several
	// Badger batches.
	bufferedWrite(t, b, func(tx *BadgerTransaction) error {
		for i := 0; i < finalScopeNodes; i++ {
			if _, err := tx.CreateNode(ctx, &Node{ID: finalScopeNodeID(i), Labels: []string{finalScopeLabel}, Properties: map[string]any{"v": int64(2), "pad": "0123456789abcdef"}}); err != nil {
				return err
			}
			if i > 0 {
				if err := tx.CreateEdge(ctx, &Edge{ID: finalScopeEdgeID(i), StartNode: finalScopeNodeID(i - 1), EndNode: finalScopeNodeID(i), Type: finalScopeEdgeType, Properties: map[string]any{"p": int64(2)}}); err != nil {
					return err
				}
			}
		}
		return nil
	})

	actions, stopActions := recordFinalScopeActions(t, b)
	batches, stopBatches := recordFinalScopeBatches(t, b, actions, finalScopeKeepA())
	err := b.FlushWriteBehind(ctx)
	stopBatches()
	stopActions()
	require.NoError(t, err)

	requireSingleFinalRevocation(t, *batches, *actions, 3)
	for _, batch := range (*batches)[:len(*batches)-1] {
		require.True(t, batch.keepWarm, "preparation batch %d revoked an unrelated cached body", batch.actions)
	}
	// The generation is durable and publicly readable after the replay.
	node, err := b.GetNode(ctx, finalScopeNodeID(finalScopeNodes-1))
	require.NoError(t, err)
	require.EqualValues(t, 2, node.Properties["v"])
	tail, err := b.GetEdgesByType(ctx, finalScopeEdgeType)
	require.NoError(t, err)
	require.Len(t, tail, finalScopeNodes-1)
	after := captureFinalScopeCaches(b, []NodeID{finalScopeKeepA()}, []EdgeID{finalScopeKeepE()}, finalScopeKeepLabel, finalScopeKeepType)
	require.Same(t, keep.nodes[finalScopeKeepA()], after.nodes[finalScopeKeepA()], "the replay revoked an unrelated cached body")
	require.Equal(t, keep.adj[finalScopeKeepA()], after.adj[finalScopeKeepA()], "the replay revoked unrelated adjacency")
}

// TestRetainedSnapshotDropPrefixRevokesOnceAtFinalBatch is the retained
// snapshot part of proof gate 5: the atomic drop writer revokes at its final
// batch only, and a pinned reader keeps its admitted preimage.
func TestRetainedSnapshotDropPrefixRevokesOnceAtFinalBatch(t *testing.T) {
	b := openFinalScopeEngine(t)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()
	const dropped = 25000
	seed, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	for i := 0; i < dropped; i++ {
		_, err := seed.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("drop:n-%d", i)), Labels: []string{"Dropped"}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, seed.Commit())
	seedFinalScopeUnrelated(t, b)
	warmFinalScopeUnrelated(t, b)
	keep := captureFinalScopeCaches(b, []NodeID{finalScopeKeepA()}, []EdgeID{finalScopeKeepE()}, finalScopeKeepLabel, finalScopeKeepType)
	require.NotNil(t, keep.nodes[finalScopeKeepA()])

	reader, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	_, err = reader.GetNode(ctx, "drop:n-0")
	require.NoError(t, err)

	actions, stopActions := recordFinalScopeActions(t, b)
	batches, stopBatches := recordFinalScopeBatches(t, b, actions, finalScopeKeepA())
	prefix := append([]byte{prefixNode}, []byte("drop:")...)
	err = b.db.DropPrefix(ctx, prefixDropOptions{mode: retainPrefixSnapshots, engine: b}, prefix)
	stopBatches()
	stopActions()
	require.NoError(t, err)

	final := requireSingleFinalRevocation(t, *batches, *actions, 2)
	// The retained writer declares no graph facts, so its one revocation is
	// the whole-store invalidation, at that same final timestamp.
	require.True(t, final.invalidate, "the retained drop must take its whole-store invalidation at the final batch")
	for _, batch := range (*batches)[:len(*batches)-1] {
		require.True(t, batch.keepWarm, "preparation batch %d revoked an unrelated cached body", batch.actions)
	}
	// The pinned reader keeps the preimage it was admitted with.
	still, err := reader.GetNode(ctx, "drop:n-0")
	require.NoError(t, err, "the open snapshot must still see the dropped node")
	require.EqualValues(t, 1, still.Properties["v"])
	// Current readers no longer see the dropped rows, and the kept ones remain.
	_, err = b.GetNode(ctx, "drop:n-0")
	require.ErrorIs(t, err, ErrNotFound)
	kept, err := b.GetNode(ctx, finalScopeKeepA())
	require.NoError(t, err)
	require.EqualValues(t, 7, kept.Properties["v"])
}

// TestLargeCommitWithoutScopeProviderStillInvalidatesOnEveryBatch is the
// unknown-scope part of proof gate 5: a large commit with no scope provider
// (the zero cacheWriteScope) keeps its full invalidation, so no batch of it
// may assume it can name its facts.
func TestLargeCommitWithoutScopeProviderStillInvalidatesOnEveryBatch(t *testing.T) {
	b := openFinalScopeEngine(t)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()
	seedFinalScopeUnrelated(t, b)
	warmFinalScopeUnrelated(t, b)

	actions, stopActions := recordFinalScopeActions(t, b)
	batches, stopBatches := recordFinalScopeBatches(t, b, actions, finalScopeKeepA())
	lc, err := b.db.beginLargeCommitContext(ctx)
	require.NoError(t, err)
	require.Nil(t, lc.cacheScope, "the fixture must keep the commit without a scope provider")
	w := &batchWriter{txn: lc.openBatch(), to: lc}
	defer w.discard()
	require.NoError(t, w.write(ctx, func(txn *badger.Txn) error { return txn.Set([]byte("test:noscope-a"), []byte("a")) }))
	require.NoError(t, w.flush(ctx))
	require.NoError(t, w.write(ctx, func(txn *badger.Txn) error { return txn.Set([]byte("test:noscope-b"), []byte("b")) }))
	require.NoError(t, lc.finish(w))
	stopBatches()
	stopActions()

	require.Equal(t, 2, len(*batches))
	for _, batch := range *batches {
		require.True(t, batch.invalidate, "a commit without a scope provider must invalidate on batch %d", batch.actions)
		require.False(t, batch.keepWarm, "a commit without a scope provider must drop the caches on batch %d", batch.actions)
	}
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "test:noscope-a", "a")
		requireValue(t, txn, "test:noscope-b", "b")
		return nil
	}))
}

// TestLargeCommitFinalScopeCoarsensAtFinalTimestamp is the floor-coarsening
// part of proof gate 5: when the commit's complete fact set exceeds the
// engine's retention limit, the coarsening happens at the final batch's
// timestamp, never at a preparation batch's.
func TestLargeCommitFinalScopeCoarsensAtFinalTimestamp(t *testing.T) {
	b := openFinalScopeEngine(t)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	const nodes = finalScopeNodes
	seedFinalScopeGeneration(t, b, nodes)
	ids, edges, _ := warmFinalScopeCaches(t, b, nodes)
	// Every floor family keeps one entry, so any complete revocation pass
	// exceeds the retention limit and coarsens.
	b.nodeCacheMaxEntries = 1
	b.edgeCacheMaxItems = 1
	b.adjCacheMaxNodes = 1
	b.edgeTypeCacheMaxTypes = 1
	b.labelFirstCacheMax = 1
	before := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)

	actions, stopActions := recordFinalScopeActions(t, b)
	batches, stopBatches := recordFinalScopeBatches(t, b, actions, finalScopeKeepA())
	err := finalScopeUpdateCommit(t, b, nodes, "")
	stopBatches()
	stopActions()
	require.NoError(t, err)

	require.Greater(t, len(*batches), 1)
	for _, batch := range (*batches)[:len(*batches)-1] {
		require.Equal(t, before.all, batch.all, "preparation batch %d coarsened the durable floors", batch.actions)
	}
	final := (*batches)[len(*batches)-1]
	require.True(t, final.invalidate, "the final batch's complete fact set must coarsen at its own timestamp")
	after := captureFinalScopeCaches(b, ids, edges, finalScopeLabel, finalScopeEdgeType)
	require.Equal(t, final.ts, after.all, "the coarsened floor must carry the final batch's timestamp")
	require.Equal(t, final.ts, after.floors[ids[0]], "a coarsened floor covers every entity at the final batch's timestamp")
	require.Zero(t, finalScopeFloorKeys(b), "coarsening replaces the per-entity floors")
	// Coarsening does not change what readers see.
	for _, id := range ids {
		node, readErr := b.GetNode(context.Background(), id)
		require.NoError(t, readErr)
		require.EqualValues(t, 2, node.Properties["v"])
	}
}
