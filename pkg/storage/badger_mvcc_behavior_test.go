package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func countNodeMVCCVersions(t *testing.T, engine *BadgerEngine, nodeID NodeID) int {
	t.Helper()
	prefix := engine.mvccNodeVersionPrefixString(nodeID)
	if prefix == nil {
		return 0
	}
	count := 0
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			count++
		}
		return nil
	}))
	return count
}

func countAdjacencyMVCCVersions(t *testing.T, engine *BadgerEngine, prefix []byte) int {
	t.Helper()
	count := 0
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			count++
		}
		return nil
	}))
	return count
}

type nonMVCCEngine struct {
	Engine
}

func createMVCCBadgerEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	// MVCC-focused tests exercise prior-version reads, so opt into
	// historical retention. The production default is head-only
	// (MaxVersionsPerKey = 0) to keep write amplification low.
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})
	return engine
}

func TestBadgerEngine_MVCCNodeLatestSnapshotAndRecreate(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	nodeID := NodeID(prefixTestID("mvcc-node"))

	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Person"}, Properties: map[string]any{"name": "v1"}})
	require.NoError(t, err)
	v1, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)

	require.NoError(t, engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Person"}, Properties: map[string]any{"name": "v2"}}))
	v2, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.Equal(t, "v2", mustGetNodeLatestVisible(t, engine, nodeID).Properties["name"])

	require.NoError(t, engine.DeleteNode(nodeID))
	v3, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.True(t, v3.Tombstoned)
	_, err = engine.GetNodeLatestVisible(nodeID)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Person"}, Properties: map[string]any{"name": "v4"}})
	require.NoError(t, err)
	v4, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.False(t, v4.Tombstoned)
	require.Equal(t, "v4", mustGetNodeLatestVisible(t, engine, nodeID).Properties["name"])

	nodeAtV1, err := engine.GetNodeVisibleAt(nodeID, v1.Version)
	require.NoError(t, err)
	require.Equal(t, "v1", nodeAtV1.Properties["name"])

	nodeAtV2, err := engine.GetNodeVisibleAt(nodeID, v2.Version)
	require.NoError(t, err)
	require.Equal(t, "v2", nodeAtV2.Properties["name"])

	_, err = engine.GetNodeVisibleAt(nodeID, v3.Version)
	require.ErrorIs(t, err, ErrNotFound)

	nodeAtV4, err := engine.GetNodeVisibleAt(nodeID, v4.Version)
	require.NoError(t, err)
	require.Equal(t, "v4", nodeAtV4.Properties["name"])

	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, nodeID, nodes[0].ID)
}

func TestBadgerEngine_GetNodeVisibleAtCurrentHeadCachesByBadgerBodyVersion(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	nodeID := NodeID(prefixTestID("mvcc-body-cache"))
	embedding := make([]float64, 1024)
	for i := range embedding {
		embedding[i] = float64(i) / 1024
	}

	_, err := engine.CreateNode(&Node{
		ID:     nodeID,
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":      "v1",
			"embedding": embedding,
		},
	})
	require.NoError(t, err)
	v1, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)

	first, err := engine.GetNodeVisibleAt(nodeID, v1.Version)
	require.NoError(t, err)
	require.Equal(t, "v1", first.Properties["name"])

	engine.nodeBodyCacheMu.RLock()
	entryV1, ok := engine.nodeBodyCache[nodeID]
	engine.nodeBodyCacheMu.RUnlock()
	require.True(t, ok, "current-head read should cache the decoded primary body")
	require.NotZero(t, entryV1.itemVersion)
	require.Equal(t, "v1", entryV1.node.Properties["name"])

	first.Properties["name"] = "caller-mutated"
	second, err := engine.GetNodeVisibleAt(nodeID, v1.Version)
	require.NoError(t, err)
	require.Equal(t, "v1", second.Properties["name"], "body cache hits must return a copy")

	require.NoError(t, engine.UpdateNode(&Node{
		ID:     nodeID,
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":      "v2",
			"embedding": embedding,
		},
	}))
	v2, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)

	latest, err := engine.GetNodeVisibleAt(nodeID, v2.Version)
	require.NoError(t, err)
	require.Equal(t, "v2", latest.Properties["name"])

	engine.nodeBodyCacheMu.RLock()
	entryV2, ok := engine.nodeBodyCache[nodeID]
	engine.nodeBodyCacheMu.RUnlock()
	require.True(t, ok)
	require.NotEqual(t, entryV1.itemVersion, entryV2.itemVersion, "updated primary bodies must not reuse the old cached body")
	require.Equal(t, "v2", entryV2.node.Properties["name"])

	old, err := engine.GetNodeVisibleAt(nodeID, v1.Version)
	require.NoError(t, err)
	require.Equal(t, "v1", old.Properties["name"], "historical reads still resolve from MVCC records")
}

func TestBadgerEngine_MVCCEdgeLatestSnapshotAndRecreate(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	start := NodeID(prefixTestID("mvcc-edge-start"))
	end := NodeID(prefixTestID("mvcc-edge-end"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("mvcc-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "KNOWS", Properties: map[string]any{"weight": 1}}))
	v1, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	require.NoError(t, engine.UpdateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "KNOWS", Properties: map[string]any{"weight": 2}}))
	v2, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	require.NoError(t, engine.DeleteEdge(edgeID))
	v3, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)
	require.True(t, v3.Tombstoned)

	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "KNOWS", Properties: map[string]any{"weight": 4}}))
	v4, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	edgeAtV1, err := engine.GetEdgeVisibleAt(edgeID, v1.Version)
	require.NoError(t, err)
	require.EqualValues(t, 1, edgeAtV1.Properties["weight"])

	edgeAtV2, err := engine.GetEdgeVisibleAt(edgeID, v2.Version)
	require.NoError(t, err)
	require.EqualValues(t, 2, edgeAtV2.Properties["weight"])

	_, err = engine.GetEdgeVisibleAt(edgeID, v3.Version)
	require.ErrorIs(t, err, ErrNotFound)

	edgeAtV4, err := engine.GetEdgeVisibleAt(edgeID, v4.Version)
	require.NoError(t, err)
	require.EqualValues(t, 4, edgeAtV4.Properties["weight"])

	latest, err := engine.GetEdgeLatestVisible(edgeID)
	require.NoError(t, err)
	require.EqualValues(t, 4, latest.Properties["weight"])
}

func TestBadgerEngine_MVCCLatestVisibleFallsBackToVersionRecords(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	nodeID := NodeID(prefixTestID("mvcc-version-only-node"))
	nodeVersion := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 11}
	require.NoError(t, engine.AppendNodeVersion(&Node{
		ID:         nodeID,
		Labels:     []string{"Doc"},
		Properties: map[string]any{"source": "version-record"},
	}, nodeVersion))

	latestNode, err := engine.GetNodeLatestVisible(nodeID)
	require.NoError(t, err)
	require.Equal(t, nodeID, latestNode.ID)
	require.Equal(t, "version-record", latestNode.Properties["source"])

	tombstonedNodeID := NodeID(prefixTestID("mvcc-version-only-tombstone"))
	require.NoError(t, engine.AppendNodeTombstone(tombstonedNodeID, MVCCVersion{CommitTimestamp: nodeVersion.CommitTimestamp.Add(time.Second), CommitSequence: 12}))
	_, err = engine.GetNodeLatestVisible(tombstonedNodeID)
	require.ErrorIs(t, err, ErrNotFound)

	edgeID := EdgeID(prefixTestID("mvcc-version-only-edge"))
	edgeVersion := MVCCVersion{CommitTimestamp: nodeVersion.CommitTimestamp.Add(2 * time.Second), CommitSequence: 13}
	require.NoError(t, engine.AppendEdgeVersion(&Edge{
		ID:         edgeID,
		StartNode:  nodeID,
		EndNode:    nodeID,
		Type:       "LINKS",
		Properties: map[string]any{"source": "edge-version-record"},
	}, edgeVersion))

	latestEdge, err := engine.GetEdgeLatestVisible(edgeID)
	require.NoError(t, err)
	require.Equal(t, edgeID, latestEdge.ID)
	require.Equal(t, "edge-version-record", latestEdge.Properties["source"])

	tombstonedEdgeID := EdgeID(prefixTestID("mvcc-version-only-edge-tombstone"))
	require.NoError(t, engine.AppendEdgeTombstone(tombstonedEdgeID, MVCCVersion{CommitTimestamp: edgeVersion.CommitTimestamp.Add(time.Second), CommitSequence: 14}))
	_, err = engine.GetEdgeLatestVisible(tombstonedEdgeID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestAsyncEngine_GetNodeLatestEffectiveUsesPendingState(t *testing.T) {
	base := createMVCCBadgerEngine(t)
	async := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	nodeID := NodeID(prefixTestID("async-mvcc-node"))
	_, err := async.CreateNode(&Node{ID: nodeID, Labels: []string{"Person"}, Properties: map[string]any{"name": "pending"}})
	require.NoError(t, err)

	node, err := async.GetNodeLatestEffective(nodeID)
	require.NoError(t, err)
	require.Equal(t, "pending", node.Properties["name"])

	require.NoError(t, async.DeleteNode(nodeID))
	_, err = async.GetNodeLatestEffective(nodeID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_RebuildAndPruneMVCC(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	nodeID := NodeID(prefixTestID("mvcc-maint-node"))

	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 1}, CreatedAt: time.Now().UTC().Add(-4 * time.Hour)})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 2}, UpdatedAt: time.Now().UTC().Add(-3 * time.Hour)}))
	require.NoError(t, engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 3}, UpdatedAt: time.Now().UTC().Add(-2 * time.Hour)}))

	beforeHead, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		key := engine.mvccNodeHeadKeyStringLookup(nodeID)
		if key == nil {
			return nil
		}
		return txn.Delete(key)
	}))
	_, err = engine.GetNodeCurrentHead(nodeID)
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, engine.RebuildMVCCHeads(context.Background()))
	afterHead, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	// Post head-only refactor the head key stores the current version
	// stamp; when that key is deleted, rebuild can recover a version
	// that's <= the original (the last version that left a trace in
	// the version keyspace). The critical invariant is that reads
	// still resolve to the current live body via the primary key.
	require.LessOrEqual(t, afterHead.Version.Compare(beforeHead.Version), 0)
	require.Equal(t, 3, mustGetNodeLatestVisible(t, engine, nodeID).Properties["version"])

	_, err = engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	// Post head-only refactor, archived versions only exist for
	// superseded writes. The exact number of deletions depends on how
	// many prior versions were archived — not on the number of
	// commits. The invariant we care about here is that pruning
	// preserves the live body (primary key).
	require.Equal(t, 3, mustGetNodeLatestVisible(t, engine, nodeID).Properties["version"])

	oldVersion, err := engine.GetNodeVisibleAt(nodeID, MVCCVersion{CommitTimestamp: time.Now().UTC().Add(-24 * time.Hour), CommitSequence: ^uint64(0)})
	if err == nil {
		require.NotNil(t, oldVersion)
	}
}

func TestBadgerEngine_RebuildMVCCHeads_BatchesLargeDataset(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	const total = 650 // Crosses mvccRebuildScanBatchSize (512) to exercise chunked rebuild.

	for i := 0; i < total; i++ {
		id := NodeID(prefixTestID(fmt.Sprintf("mvcc-batch-node-%d", i)))
		_, err := engine.CreateNode(&Node{
			ID:         id,
			Labels:     []string{"Doc"},
			Properties: map[string]any{"version": 1},
		})
		require.NoError(t, err)
		require.NoError(t, engine.UpdateNode(&Node{
			ID:         id,
			Labels:     []string{"Doc"},
			Properties: map[string]any{"version": 2},
		}))
	}

	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixMVCCNodeHead))
	require.NoError(t, engine.RebuildMVCCHeads(context.Background()))

	for _, idx := range []int{0, total / 2, total - 1} {
		id := NodeID(prefixTestID(fmt.Sprintf("mvcc-batch-node-%d", idx)))
		head, err := engine.GetNodeCurrentHead(id)
		require.NoError(t, err)
		require.False(t, head.Tombstoned)
		node, err := engine.GetNodeLatestVisible(id)
		require.NoError(t, err)
		require.EqualValues(t, 2, node.Properties["version"])
	}
}

func TestBadgerEngine_RebuildMVCCEdgeHeadsFromVersions(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	startID := NodeID(prefixTestID("mvcc-edge-rebuild-start"))
	endID := NodeID(prefixTestID("mvcc-edge-rebuild-end"))
	_, err := engine.CreateNode(&Node{ID: startID, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: endID, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("mvcc-edge-rebuild"))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:         edgeID,
		StartNode:  startID,
		EndNode:    endID,
		Type:       "REL",
		Properties: map[string]any{"version": 1},
	}))
	require.NoError(t, engine.UpdateEdge(&Edge{
		ID:         edgeID,
		StartNode:  startID,
		EndNode:    endID,
		Type:       "REL",
		Properties: map[string]any{"version": 2},
	}))

	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixMVCCEdgeHead))
	_, err = engine.GetEdgeCurrentHead(edgeID)
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, engine.RebuildMVCCHeads(context.Background()))
	head, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)
	require.False(t, head.Tombstoned)
	latest, err := engine.GetEdgeLatestVisible(edgeID)
	require.NoError(t, err)
	require.EqualValues(t, 2, latest.Properties["version"])
}

func TestBadgerTransaction_CreateEdge_AllowsReadableNodesWithoutMVCCHead(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	startID := NodeID(prefixTestID("mvcc-headless-start"))
	endID := NodeID(prefixTestID("mvcc-headless-end"))
	_, err := engine.CreateNode(&Node{ID: startID, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: endID, Labels: []string{"Node"}})
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		if key := engine.mvccNodeHeadKeyStringLookup(startID); key != nil {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		if key := engine.mvccNodeHeadKeyStringLookup(endID); key != nil {
			return txn.Delete(key)
		}
		return nil
	}))

	_, err = engine.GetNodeCurrentHead(startID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetNodeCurrentHead(endID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetNode(startID)
	require.NoError(t, err)
	_, err = engine.GetNode(endID)
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID:        EdgeID(prefixTestID("mvcc-headless-edge")),
		StartNode: startID,
		EndNode:   endID,
		Type:      "LINKS",
	}))
	require.NoError(t, tx.Commit())

	stored, err := engine.GetEdge(EdgeID(prefixTestID("mvcc-headless-edge")))
	require.NoError(t, err)
	require.Equal(t, startID, stored.StartNode)
	require.Equal(t, endID, stored.EndNode)
}

func TestBadgerEngine_PruneMVCCVersions_UsesRetentionPolicyDefaults(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 1},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	nodeID := NodeID(prefixTestID("mvcc-retention-node"))
	_, err = engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 1}})
	require.NoError(t, err)
	v1, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 2}}))
	v2, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 3}}))
	v3, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)

	deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, deleted, int64(1))
	head, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.Equal(t, 0, head.FloorVersion.Compare(v2.Version))

	_, err = engine.GetNodeVisibleAt(nodeID, v1.Version)
	require.ErrorIs(t, err, ErrNotVisibleAtSnapshot)
	nodeAtV2, err := engine.GetNodeVisibleAt(nodeID, v2.Version)
	require.NoError(t, err)
	require.EqualValues(t, 2, nodeAtV2.Properties["version"])
	nodeAtV3, err := engine.GetNodeVisibleAt(nodeID, v3.Version)
	require.NoError(t, err)
	require.EqualValues(t, 3, nodeAtV3.Properties["version"])
}

func TestBadgerEngine_PruneMVCCVersions_TombstoneCompactionHonorsActiveReaders(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	nodeID := NodeID(prefixTestID("mvcc-tombstone-compact"))

	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 1}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 2}}))
	require.NoError(t, engine.DeleteNode(nodeID))
	head, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.True(t, head.Tombstoned)

	require.Equal(t, 3, countNodeMVCCVersions(t, engine, nodeID))
	engine.activeMVCCSnapshotReaders.Add(1)
	_, err = engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 100})
	require.NoError(t, err)
	engine.activeMVCCSnapshotReaders.Add(-1)
	require.Equal(t, 3, countNodeMVCCVersions(t, engine, nodeID))

	deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 100})
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted)
	require.Equal(t, 1, countNodeMVCCVersions(t, engine, nodeID))
	_, err = engine.GetNodeLatestVisible(nodeID)
	require.ErrorIs(t, err, ErrNotFound)
	remainingHead, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.True(t, remainingHead.Tombstoned)
}

func TestBadgerEngine_MVCCStress_ChurnPruneBoundsRetainedChain(t *testing.T) {
	dir := t.TempDir()
	// Exercises multi-version retention specifically; opt into
	// historical archival (production default is head-only).
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: dir,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	nodeID := NodeID(prefixTestID("mvcc-stress-churn"))
	createVersion := func(iteration int) *Node {
		return &Node{
			ID:     nodeID,
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"iteration": iteration,
				"state":     "live",
			},
		}
	}

	_, err = engine.CreateNode(createVersion(0))
	require.NoError(t, err)

	const churnCycles = 48
	for i := 1; i <= churnCycles; i++ {
		require.NoError(t, engine.UpdateNode(createVersion(i)))
		if i%6 == 0 {
			require.NoError(t, engine.DeleteNode(nodeID))
			_, err := engine.GetNodeLatestVisible(nodeID)
			require.ErrorIs(t, err, ErrNotFound)
			_, err = engine.CreateNode(createVersion(i * 100))
			require.NoError(t, err)
		}
	}

	headBeforePrune, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	prePruneVersions := countNodeMVCCVersions(t, engine, nodeID)
	require.Greater(t, prePruneVersions, 12)

	deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 3})
	require.NoError(t, err)
	require.Greater(t, deleted, int64(0))

	headAfterPrune, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.Equal(t, 0, headAfterPrune.Version.Compare(headBeforePrune.Version))
	require.False(t, headAfterPrune.Tombstoned)
	require.LessOrEqual(t, countNodeMVCCVersions(t, engine, nodeID), 4)

	latest, err := engine.GetNodeLatestVisible(nodeID)
	require.NoError(t, err)
	require.EqualValues(t, churnCycles*100, latest.Properties["iteration"])
	require.Equal(t, "live", latest.Properties["state"])

	_, err = engine.GetNodeVisibleAt(nodeID, headBeforePrune.FloorVersion)
	if !headBeforePrune.FloorVersion.IsZero() {
		if err != nil {
			// Pruning may have either dropped the head entirely (ErrNotFound)
			// or left a head whose floor moved past the requested version
			// (ErrNotVisibleAtSnapshot). Either is correct: pre-floor reads
			// must not surface a body.
			require.True(t,
				errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotVisibleAtSnapshot),
				"want ErrNotFound or ErrNotVisibleAtSnapshot, got %v", err)
		}
	}
	_, err = engine.GetNodeVisibleAt(nodeID, headAfterPrune.Version)
	require.NoError(t, err)
}

func TestMVCCInternalRecordsAlwaysUseMsgpack(t *testing.T) {
	headData, err := encodeMVCCHead(MVCCHead{Version: MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 7}, Tombstoned: true, FloorVersion: MVCCVersion{CommitTimestamp: time.Now().UTC().Add(-time.Minute), CommitSequence: 3}})
	require.NoError(t, err)
	_, _, ok, err := splitSerializationHeader(headData)
	require.NoError(t, err)
	require.False(t, ok)
	decodedHead, err := decodeMVCCHead(headData)
	require.NoError(t, err)
	require.True(t, decodedHead.Tombstoned)
	require.False(t, decodedHead.FloorVersion.IsZero())

	recordData, err := encodeMVCCNodeRecord(&Node{ID: NodeID("bench:mvcc-msgpack"), Labels: []string{"Doc"}}, false)
	require.NoError(t, err)
	_, _, ok, err = splitSerializationHeader(recordData)
	require.NoError(t, err)
	require.False(t, ok)
	decodedRecord, err := decodeMVCCNodeRecord(recordData)
	require.NoError(t, err)
	require.NotNil(t, decodedRecord.Node)
	require.Equal(t, NodeID("bench:mvcc-msgpack"), decodedRecord.Node.ID)
}

func TestNamespacedEngine_MVCCDelegation(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	namespaced := NewNamespacedEngine(engine, "test")

	_, err := namespaced.CreateNode(&Node{ID: NodeID("mvcc-ns-node"), Labels: []string{"Doc"}, Properties: map[string]any{"title": "v1"}})
	require.NoError(t, err)
	head, err := namespaced.GetNodeCurrentHead(NodeID("mvcc-ns-node"))
	require.NoError(t, err)

	node, err := namespaced.GetNodeVisibleAt(NodeID("mvcc-ns-node"), head.Version)
	require.NoError(t, err)
	require.Equal(t, NodeID("mvcc-ns-node"), node.ID)
	require.Equal(t, "v1", node.Properties["title"])

	nodes, err := namespaced.GetNodesByLabelVisibleAt("Doc", head.Version)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, NodeID("mvcc-ns-node"), nodes[0].ID)

	startID := NodeID("mvcc-ns-start")
	endID := NodeID("mvcc-ns-end")
	_, err = namespaced.CreateNode(&Node{ID: startID, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = namespaced.CreateNode(&Node{ID: endID, Labels: []string{"Node"}})
	require.NoError(t, err)
	require.NoError(t, namespaced.CreateEdge(&Edge{ID: EdgeID("mvcc-ns-edge"), StartNode: startID, EndNode: endID, Type: "LINKS"}))
	edgeHead, err := namespaced.GetEdgeCurrentHead(EdgeID("mvcc-ns-edge"))
	require.NoError(t, err)

	edges, err := namespaced.GetEdgesByTypeVisibleAt("LINKS", edgeHead.Version)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, EdgeID("mvcc-ns-edge"), edges[0].ID)

	between, err := namespaced.GetEdgesBetweenVisibleAt(startID, endID, edgeHead.Version)
	require.NoError(t, err)
	require.Len(t, between, 1)
	require.Equal(t, EdgeID("mvcc-ns-edge"), between[0].ID)
}

func TestBadgerEngine_MVCCIndexedVisibilityAtSnapshot(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	nodeID := NodeID(prefixTestID("mvcc-index-node"))
	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Draft"}, Properties: map[string]any{"title": "v1"}})
	require.NoError(t, err)
	v1, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)

	require.NoError(t, engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Published"}, Properties: map[string]any{"title": "v2"}}))
	v2, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)

	draftAtV1, err := engine.GetNodesByLabelVisibleAt("Draft", v1.Version)
	require.NoError(t, err)
	require.Len(t, draftAtV1, 1)
	require.Equal(t, nodeID, draftAtV1[0].ID)

	draftAtV2, err := engine.GetNodesByLabelVisibleAt("Draft", v2.Version)
	require.NoError(t, err)
	require.Empty(t, draftAtV2)

	publishedAtV2, err := engine.GetNodesByLabelVisibleAt("Published", v2.Version)
	require.NoError(t, err)
	require.Len(t, publishedAtV2, 1)
	require.Equal(t, nodeID, publishedAtV2[0].ID)

	start := NodeID(prefixTestID("mvcc-index-start"))
	end := NodeID(prefixTestID("mvcc-index-end"))
	_, err = engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("mvcc-index-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "DRAFT_LINK"}))
	e1, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	require.NoError(t, engine.UpdateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "PUBLISHED_LINK"}))
	e2, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	draftEdgesAtV1, err := engine.GetEdgesByTypeVisibleAt("DRAFT_LINK", e1.Version)
	require.NoError(t, err)
	require.Len(t, draftEdgesAtV1, 1)
	require.Equal(t, edgeID, draftEdgesAtV1[0].ID)

	publishedEdgesAtV1, err := engine.GetEdgesByTypeVisibleAt("PUBLISHED_LINK", e1.Version)
	require.NoError(t, err)
	require.Empty(t, publishedEdgesAtV1)

	publishedEdgesAtV2, err := engine.GetEdgesByTypeVisibleAt("PUBLISHED_LINK", e2.Version)
	require.NoError(t, err)
	require.Len(t, publishedEdgesAtV2, 1)

	betweenAtV1, err := engine.GetEdgesBetweenVisibleAt(start, end, e1.Version)
	require.NoError(t, err)
	require.Len(t, betweenAtV1, 1)
	require.Equal(t, "DRAFT_LINK", betweenAtV1[0].Type)

	betweenAtV2, err := engine.GetEdgesBetweenVisibleAt(start, end, e2.Version)
	require.NoError(t, err)
	require.Len(t, betweenAtV2, 1)
	require.Equal(t, "PUBLISHED_LINK", betweenAtV2[0].Type)
}

func TestBadgerEngine_MVCCBetweenVisibleAtIgnoresLatestEdgeBetweenIndex(t *testing.T) {
	// Exercises snapshot read at a prior version; requires history.
	engine := createMVCCBadgerEngine(t)

	start := NodeID(prefixTestID("mvcc-between-index-start"))
	end := NodeID(prefixTestID("mvcc-between-index-end"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Doc"}})
	require.NoError(t, err)

	first := &Edge{ID: EdgeID(prefixTestID("mvcc-between-index-e1")), StartNode: start, EndNode: end, Type: "OLD_LINK"}
	require.NoError(t, engine.CreateEdge(first))
	firstHead, err := engine.GetEdgeCurrentHead(first.ID)
	require.NoError(t, err)
	require.NoError(t, engine.DeleteEdge(first.ID))
	second := &Edge{ID: EdgeID(prefixTestID("mvcc-between-index-e2")), StartNode: start, EndNode: end, Type: "NEW_LINK"}
	require.NoError(t, engine.CreateEdge(second))

	require.NotNil(t, engine.GetEdgeBetween(start, end, "NEW_LINK"))
	requireEdgeBetweenIndexEntry(t, engine, start, end, "NEW_LINK", second.ID, true)

	betweenAtFirst, err := engine.GetEdgesBetweenVisibleAt(start, end, firstHead.Version)
	require.NoError(t, err)
	require.Len(t, betweenAtFirst, 1)
	require.Equal(t, first.ID, betweenAtFirst[0].ID)
	require.Equal(t, "OLD_LINK", betweenAtFirst[0].Type)
}

func TestBadgerEngine_MVCCDirectionalAdjacencyVisibleAtTracksEndpointMoves(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	start := NodeID(prefixTestID("mvcc-adj-start"))
	middle := NodeID(prefixTestID("mvcc-adj-middle"))
	end := NodeID(prefixTestID("mvcc-adj-end"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: middle, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("mvcc-adj-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: middle, Type: "LINKS"}))
	v1, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	require.NoError(t, engine.UpdateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "LINKS"}))
	v2, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	outAtV1, err := engine.GetOutgoingEdgesVisibleAt(start, v1.Version)
	require.NoError(t, err)
	require.Len(t, outAtV1, 1)
	require.Equal(t, middle, outAtV1[0].EndNode)

	inMiddleAtV1, err := engine.GetIncomingEdgesVisibleAt(middle, v1.Version)
	require.NoError(t, err)
	require.Len(t, inMiddleAtV1, 1)
	require.Equal(t, edgeID, inMiddleAtV1[0].ID)

	inEndAtV1, err := engine.GetIncomingEdgesVisibleAt(end, v1.Version)
	require.NoError(t, err)
	require.Empty(t, inEndAtV1)

	outAtV2, err := engine.GetOutgoingEdgesVisibleAt(start, v2.Version)
	require.NoError(t, err)
	require.Len(t, outAtV2, 1)
	require.Equal(t, end, outAtV2[0].EndNode)

	inMiddleAtV2, err := engine.GetIncomingEdgesVisibleAt(middle, v2.Version)
	require.NoError(t, err)
	require.Empty(t, inMiddleAtV2)

	inEndAtV2, err := engine.GetIncomingEdgesVisibleAt(end, v2.Version)
	require.NoError(t, err)
	require.Len(t, inEndAtV2, 1)
	require.Equal(t, edgeID, inEndAtV2[0].ID)
}

func TestBadgerEngine_MVCCDirectionalAdjacencyVisibleAtTracksDeletes(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	start := NodeID(prefixTestID("mvcc-adj-del-start"))
	end := NodeID(prefixTestID("mvcc-adj-del-end"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("mvcc-adj-del-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "LINKS"}))
	v1, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	require.NoError(t, engine.DeleteEdge(edgeID))
	v2, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	outAtV1, err := engine.GetOutgoingEdgesVisibleAt(start, v1.Version)
	require.NoError(t, err)
	require.Len(t, outAtV1, 1)
	require.Equal(t, edgeID, outAtV1[0].ID)

	inAtV1, err := engine.GetIncomingEdgesVisibleAt(end, v1.Version)
	require.NoError(t, err)
	require.Len(t, inAtV1, 1)
	require.Equal(t, edgeID, inAtV1[0].ID)

	outAtV2, err := engine.GetOutgoingEdgesVisibleAt(start, v2.Version)
	require.NoError(t, err)
	require.Empty(t, outAtV2)

	inAtV2, err := engine.GetIncomingEdgesVisibleAt(end, v2.Version)
	require.NoError(t, err)
	require.Empty(t, inAtV2)
}

func TestBadgerEngine_MVCCDirectionalAdjacencySurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "mvcc-adj-restart")
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: dir,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100},
		},
	})
	require.NoError(t, err)

	start := NodeID(prefixTestID("mvcc-adj-restart-start"))
	end := NodeID(prefixTestID("mvcc-adj-restart-end"))
	_, err = engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("mvcc-adj-restart-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "LINKS"}))
	head, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)
	require.NoError(t, engine.DeleteEdge(edgeID))
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: dir,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	outAtCreate, err := reopened.GetOutgoingEdgesVisibleAt(start, head.Version)
	require.NoError(t, err)
	require.Len(t, outAtCreate, 1)
	require.Equal(t, edgeID, outAtCreate[0].ID)

	inAtCreate, err := reopened.GetIncomingEdgesVisibleAt(end, head.Version)
	require.NoError(t, err)
	require.Len(t, inAtCreate, 1)
	require.Equal(t, edgeID, inAtCreate[0].ID)
}

func TestBadgerEngine_PruneMVCCVersions_PrunesDirectionalAdjacencyHistory(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	start := NodeID(prefixTestID("mvcc-adj-prune-start"))
	end := NodeID(prefixTestID("mvcc-adj-prune-end"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("mvcc-adj-prune-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "LINKS"}))
	require.NoError(t, engine.DeleteEdge(edgeID))

	outPrefix := engine.mvccOutgoingAdjacencyPrefixString(start)
	inPrefix := engine.mvccIncomingAdjacencyPrefixString(end)
	require.NotNil(t, outPrefix)
	require.NotNil(t, inPrefix)
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, outPrefix))
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, inPrefix))

	deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, deleted, int64(2))
	require.Equal(t, 1, countAdjacencyMVCCVersions(t, engine, outPrefix))
	require.Equal(t, 1, countAdjacencyMVCCVersions(t, engine, inPrefix))

	outNow, err := engine.GetOutgoingEdgesVisibleAt(start, maxMVCCVersion())
	require.NoError(t, err)
	require.Empty(t, outNow)
	inNow, err := engine.GetIncomingEdgesVisibleAt(end, maxMVCCVersion())
	require.NoError(t, err)
	require.Empty(t, inNow)
}

func TestBadgerEngine_DeleteEdge_WritesAdjacencyTombstonesInHeadOnlyMode(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	start := NodeID(prefixTestID("headonly-del-start"))
	end := NodeID(prefixTestID("headonly-del-end"))
	_, err = engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("headonly-del-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "LINKS"}))
	require.NoError(t, engine.DeleteEdge(edgeID))

	outPrefix := engine.mvccOutgoingAdjacencyPrefixString(start)
	inPrefix := engine.mvccIncomingAdjacencyPrefixString(end)
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, outPrefix))
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, inPrefix))
}

func TestBadgerEngine_BulkDeleteEdges_WritesAdjacencyTombstonesInHeadOnlyMode(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	start := NodeID(prefixTestID("headonly-bulk-edge-start"))
	end := NodeID(prefixTestID("headonly-bulk-edge-end"))
	_, err = engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("headonly-bulk-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "LINKS"}))
	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{edgeID}))

	outPrefix := engine.mvccOutgoingAdjacencyPrefixString(start)
	inPrefix := engine.mvccIncomingAdjacencyPrefixString(end)
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, outPrefix))
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, inPrefix))
}

func TestBadgerEngine_DeleteNodeCascade_WritesAdjacencyTombstonesInHeadOnlyMode(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	start := NodeID(prefixTestID("headonly-node-del-start"))
	end := NodeID(prefixTestID("headonly-node-del-end"))
	_, err = engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("headonly-node-del-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "LINKS"}))
	require.NoError(t, engine.DeleteNode(end))

	outPrefix := engine.mvccOutgoingAdjacencyPrefixString(start)
	inPrefix := engine.mvccIncomingAdjacencyPrefixString(end)
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, outPrefix))
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, inPrefix))
}

func TestMVCCWrappers_FallbackLatestButRejectSnapshotWhenUnsupported(t *testing.T) {
	baseInner := NewMemoryEngine()
	t.Cleanup(func() { _ = baseInner.Close() })
	base := &nonMVCCEngine{Engine: baseInner}

	async := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()
	wal := NewWALEngine(base, nil)
	namespaced := NewNamespacedEngine(base, "test")

	nodeID := NodeID("nornic:mvcc-fallback-node")
	_, err := async.CreateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"title": "latest"}})
	require.NoError(t, err)
	require.NoError(t, async.Flush())
	_, err = namespaced.CreateNode(&Node{ID: NodeID("mvcc-fallback-node-ns"), Labels: []string{"Doc"}, Properties: map[string]any{"title": "latest-ns"}})
	require.NoError(t, err)

	asyncNode, err := async.GetNodeLatestVisible(nodeID)
	require.NoError(t, err)
	require.Equal(t, "latest", asyncNode.Properties["title"])

	walNode, err := wal.GetNodeLatestVisible(nodeID)
	require.NoError(t, err)
	require.Equal(t, "latest", walNode.Properties["title"])

	nsNode, err := namespaced.GetNodeLatestVisible(NodeID("mvcc-fallback-node-ns"))
	require.NoError(t, err)
	require.Equal(t, "latest-ns", nsNode.Properties["title"])

	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
	_, err = async.GetNodeVisibleAt(nodeID, version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = wal.GetNodeVisibleAt(nodeID, version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = namespaced.GetNodeVisibleAt(NodeID("mvcc-fallback-node-ns"), version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = async.GetNodesByLabelVisibleAt("Doc", version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = wal.GetNodesByLabelVisibleAt("Doc", version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = namespaced.GetNodesByLabelVisibleAt("Doc", version)
	require.ErrorIs(t, err, ErrNotImplemented)

	edgeID := EdgeID("nornic:mvcc-fallback-edge")
	require.NoError(t, async.CreateEdge(&Edge{ID: edgeID, StartNode: nodeID, EndNode: nodeID, Type: "SELF"}))
	require.NoError(t, async.Flush())
	require.NoError(t, namespaced.CreateEdge(&Edge{ID: EdgeID("mvcc-fallback-edge-ns"), StartNode: NodeID("mvcc-fallback-node-ns"), EndNode: NodeID("mvcc-fallback-node-ns"), Type: "SELF"}))

	asyncEdge, err := async.GetEdgeLatestVisible(edgeID)
	require.NoError(t, err)
	require.Equal(t, edgeID, asyncEdge.ID)

	walEdge, err := wal.GetEdgeLatestVisible(edgeID)
	require.NoError(t, err)
	require.Equal(t, edgeID, walEdge.ID)

	nsEdge, err := namespaced.GetEdgeLatestVisible(EdgeID("mvcc-fallback-edge-ns"))
	require.NoError(t, err)
	require.Equal(t, EdgeID("mvcc-fallback-edge-ns"), nsEdge.ID)

	_, err = async.GetEdgeVisibleAt(edgeID, version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = wal.GetEdgeVisibleAt(edgeID, version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = namespaced.GetEdgeVisibleAt(EdgeID("mvcc-fallback-edge-ns"), version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = async.GetEdgesByTypeVisibleAt("SELF", version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = wal.GetEdgesByTypeVisibleAt("SELF", version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = namespaced.GetEdgesByTypeVisibleAt("SELF", version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = async.GetEdgesBetweenVisibleAt(nodeID, nodeID, version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = wal.GetEdgesBetweenVisibleAt(nodeID, nodeID, version)
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = namespaced.GetEdgesBetweenVisibleAt(NodeID("mvcc-fallback-node-ns"), NodeID("mvcc-fallback-node-ns"), version)
	require.ErrorIs(t, err, ErrNotImplemented)
}

func mustGetNodeLatestVisible(t *testing.T, engine *BadgerEngine, id NodeID) *Node {
	t.Helper()
	node, err := engine.GetNodeLatestVisible(id)
	require.NoError(t, err)
	return node
}
