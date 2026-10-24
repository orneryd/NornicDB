package storage

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These are the non-query writer entrances from the mechanical census. Keep
// the measured read outside writer-internal lookups, and inspect eligibility
// before that read can conceal a miss by filling the cache again.
func TestNodeCacheMaintenanceWriterWarmth(t *testing.T) {
	cases := []string{
		"append-node", "append-node-tombstone", "node-head", "node-head-batch", "node-bootstrap",
		"append-edge", "append-edge-tombstone", "edge-head", "edge-head-batch", "edge-bootstrap",
		"lifecycle-head", "lifecycle-delete-version", "mvcc-rebuild", "mvcc-prune",
		"edge-index-ready", "edge-index-rebuild", "label-index-ready", "label-index-rebuild",
		"policy-index-rebuild", "legacy-policy", "temporal-rebuild", "temporal-clear",
		"schema-version", "shutdown-marker", "consume-shutdown-marker", "deleted-entry-cleanup",
		"node-reconcile", "edge-reconcile", "pending-refresh", "count-fold",
		"v0-migration", "v1-node-migration", "v1-edge-migration", "v1-label-rebuild", "v1-edge-rebuild",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			b := NewMemoryEngine().BadgerEngine
			t.Cleanup(func() { _ = b.Close() })
			target := &Node{ID: "test:maintenance-untouched", Properties: map[string]any{"v": int64(7)}}
			other := &Node{ID: "test:maintenance-owner", Properties: map[string]any{"v": int64(1)}}
			peer := &Node{ID: "test:maintenance-peer", Properties: map[string]any{"v": int64(2)}}
			for _, node := range []*Node{target, other, peer} {
				_, err := b.CreateNode(node)
				require.NoError(t, err)
			}
			edge := &Edge{ID: "test:maintenance-edge", StartNode: other.ID, EndNode: peer.ID, Type: "R"}
			require.NoError(t, b.CreateEdge(edge))
			nodeHead, err := b.GetNodeCurrentHead(other.ID)
			require.NoError(t, err)
			edgeHead, err := b.GetEdgeCurrentHead(edge.ID)
			require.NoError(t, err)
			version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 100}
			_, err = b.GetNode(target.ID)
			require.NoError(t, err)
			beforeFence := b.nodeCacheInvalidationTs
			ctx := context.Background()
			switch name {
			case "append-node":
				err = b.AppendNodeVersion(other, version)
			case "append-node-tombstone":
				err = b.AppendNodeTombstone(other.ID, version)
			case "node-head":
				err = b.UpdateNodeCurrentHead(other.ID, nodeHead.Version, false)
			case "node-head-batch":
				err = b.applyNodeHeadBatch([]nodeHeadWrite{{id: other.ID, version: nodeHead.Version}})
			case "node-bootstrap":
				err = b.applyNodeBootstrapBatch([]*Node{other})
			case "append-edge":
				err = b.AppendEdgeVersion(edge, version)
			case "append-edge-tombstone":
				err = b.AppendEdgeTombstone(edge.ID, version)
			case "edge-head":
				err = b.UpdateEdgeCurrentHead(edge.ID, edgeHead.Version, false)
			case "edge-head-batch":
				err = b.applyEdgeHeadBatch([]edgeHeadWrite{{id: edge.ID, version: edgeHead.Version}})
			case "edge-bootstrap":
				err = b.applyEdgeBootstrapBatch([]*Edge{edge})
			case "lifecycle-head", "lifecycle-delete-version":
				num, ok := b.idDict.lookupNodeNumID(other.ID)
				require.True(t, ok)
				key := binary.BigEndian.AppendUint64([]byte{prefixMVCCNode}, num)
				if name == "lifecycle-head" {
					err = b.WriteMVCCHead(ctx, key, nodeHead)
				} else {
					err = b.DeleteMVCCVersion(ctx, key, nodeHead.Version)
				}
			case "mvcc-rebuild":
				err = b.RebuildMVCCHeads(ctx)
			case "mvcc-prune":
				_, err = b.PruneMVCCVersions(ctx, MVCCPruneOptions{})
			case "edge-index-ready":
				err = b.markEdgeBetweenIndexReady()
			case "edge-index-rebuild":
				_, err = b.rebuildEdgeBetweenIndex(ctx)
			case "label-index-ready":
				err = b.markLabelIndexReady()
			case "label-index-rebuild":
				_, err = b.rebuildLabelIndex(ctx)
			case "policy-index-rebuild":
				err = b.rebuildCaseSensitiveIndexes()
			case "legacy-policy":
				err = b.migrateLegacyPolicyMetadata(ctx)
			case "temporal-rebuild":
				err = b.RebuildTemporalIndexes(ctx)
			case "temporal-clear":
				err = b.clearBadgerPrefix(ctx, prefixTemporalIndex)
			case "schema-version":
				err = b.writeSchemaVersion(3)
			case "shutdown-marker":
				err = b.MarkCleanShutdown(ctx)
			case "consume-shutdown-marker":
				_, err = b.ConsumeCleanShutdownMarker(ctx)
			case "deleted-entry-cleanup":
				err = b.compactDeletedEntries()
			case "node-reconcile":
				_, err = b.EnqueueDeindexIfSuppressed(string(other.ID), false)
			case "edge-reconcile":
				_, err = b.EnqueueDeindexIfSuppressed(string(edge.ID), true)
			case "pending-refresh":
				b.RefreshPendingEmbeddingsIndex()
			case "count-fold":
				b.foldRemainingCountDeltasOnClose(b.db)
			case "v0-migration":
				err = b.migrateV0ToV1()
			case "v1-node-migration":
				_, err = b.migrateV1ToV2Nodes()
			case "v1-edge-migration":
				_, err = b.migrateV1ToV2Edges()
			case "v1-label-rebuild":
				err = b.rebuildLabelIndexForV2(&v1ToV2IndexStats{})
			case "v1-edge-rebuild":
				err = b.rebuildEdgeIndexesForV2(&v1ToV2IndexStats{})
			}
			require.NoError(t, err)
			b.nodeCacheMu.RLock()
			_, eligible := b.cachedNodeLocked(target.ID, false, 0)
			b.nodeCacheMu.RUnlock()
			if name != "mvcc-rebuild" {
				require.True(t, eligible, "known maintenance writer cooled an unrelated node")
			}
			require.Equal(t, beforeFence, b.nodeCacheInvalidationTs, "known scope raised a global node fence")
			misses := atomic.LoadInt64(&b.cacheMisses)
			got, err := b.GetNode(target.ID)
			require.NoError(t, err)
			require.Equal(t, int64(7), got.Properties["v"])
			if eligible {
				require.Equal(t, misses, atomic.LoadInt64(&b.cacheMisses))
			} else {
				require.Equal(t, misses+1, atomic.LoadInt64(&b.cacheMisses))
			}
		})
	}
}
