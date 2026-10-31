package multidb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedUsageReservationOwnsPendingAllowance(t *testing.T) {
	l := DatabaseInfo{usageNodes: 2, nodeSize: 418, totalSize: 418, sizeInitialized: true, usageInitialized: true}
	limit := StorageLimits{MaxNodes: 3, MaxBytes: 458}
	growth := storageUsage{nodeBytes: 40}
	require.NoError(t, l.reserveStorageUsage("test", "late", growth, limit))
	// The waiter may disappear, but only storage publication or a definite
	// noncommit may retire its allowance.
	require.ErrorIs(t, l.reserveStorageUsage("test", "competitor", growth, limit), ErrStorageLimitExceeded)
	l.publishStorageUsage("late", growth)
	require.Equal(t, int64(458), l.nodeSize)
	require.Empty(t, l.pending)
	require.Equal(t, reservedUsage{}, l.reserved)
	require.ErrorIs(t, l.reserveStorageUsage("test", "competitor", growth, limit), ErrStorageLimitExceeded)
	// Pending deletion cannot fund another accepted write before it applies.
	require.NoError(t, l.reserveStorageUsage("test", "delete", storageUsage{nodes: -1, nodeBytes: -40}, limit))
	require.ErrorIs(t, l.reserveStorageUsage("test", "competitor", growth, limit), ErrStorageLimitExceeded)
	l.discardStorageUsage("delete")
	require.Equal(t, int64(458), l.nodeSize)
	l.discardStorageUsage("delete")
	require.Equal(t, reservedUsage{}, l.reserved)
	l.publishStorageUsage("peer-delete", storageUsage{nodes: -1, nodeBytes: -40})
	require.NoError(t, l.reserveStorageUsage("test", "competitor", growth, limit))
	l.discardStorageUsage("competitor")
	require.Empty(t, l.pending)
	require.Equal(t, int64(1), l.usageNodes)
	require.Equal(t, int64(418), l.nodeSize)
	require.Equal(t, int64(418), l.totalSize)
}

func TestManagedUsageConcurrentReservationsDoNotDoubleSpend(t *testing.T) {
	l := DatabaseInfo{}
	var wg sync.WaitGroup
	results := make(chan error, 64)
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- l.reserveStorageUsage("test", fmt.Sprint(i), storageUsage{nodes: 1, edges: 1, nodeBytes: 20, edgeBytes: 30}, StorageLimits{MaxNodes: 1, MaxEdges: 1, MaxBytes: 50})
		}()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, ErrStorageLimitExceeded)
		}
	}
	require.Equal(t, 1, accepted)
	require.Len(t, l.pending, 1)
	require.Equal(t, reservedUsage{nodes: 1, edges: 1, bytes: 50}, l.reserved)
	for id := range l.pending {
		l.publishStorageUsage(id, storageUsage{nodes: 1, edges: 1, nodeBytes: 20, edgeBytes: 30})
	}
	require.Empty(t, l.pending)
	require.Equal(t, reservedUsage{}, l.reserved)
	require.Equal(t, int64(1), l.usageNodes)
	require.Equal(t, int64(1), l.usageEdges)
	require.Equal(t, int64(20), l.nodeSize)
	require.Equal(t, int64(30), l.edgeSize)
	require.Equal(t, int64(50), l.totalSize)
}

func TestStoredChangesUsagePreservesStoredImages(t *testing.T) {
	before := &storage.Node{ID: "test:n", Properties: map[string]any{"v": int64(1)}, ChunkEmbeddings: [][]float32{{1, 2}}, EmbedMeta: map[string]any{"model": "m"}}
	before.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: "query-provenance"})
	after := storage.CopyNode(before)
	after.Properties["v"] = "larger stored value"
	edge := &storage.Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "LINK"}
	changes := []storage.StoredEntityChange{{BeforeNode: before, AfterNode: after}, {BeforeEdge: edge}}
	wantBefore := *before
	wantBefore.ID = "n"
	wantAfter := *after
	wantAfter.ID = "n"
	wantEdge := *edge
	wantEdge.ID = "e"
	wantEdge.StartNode = "n"
	wantEdge.EndNode = "m"
	previousBytes, err := calculateNodeSize(&wantBefore)
	require.NoError(t, err)
	nextBytes, err := calculateNodeSize(&wantAfter)
	require.NoError(t, err)
	edgeBytes, err := calculateEdgeSize(&wantEdge)
	require.NoError(t, err)
	usage, err := storageChangesUsage("test", changes, true)
	require.NoError(t, err)
	require.Equal(t, storageUsage{edges: -1, nodeBytes: nextBytes - previousBytes, edgeBytes: -edgeBytes}, usage)
	require.Equal(t, storage.NodeID("test:n"), before.ID)
	require.Equal(t, storage.NodeID("test:n"), after.ID)
	require.Equal(t, storage.NodeID("test:n"), edge.StartNode)
	require.Equal(t, storage.NodeID("test:m"), edge.EndNode)
	require.Equal(t, "query-provenance", before.RuntimeMetadata().Database)
	require.Equal(t, [][]float32{{1, 2}}, before.ChunkEmbeddings)
	// Count-only admission must not invoke the byte codec at all.
	after.Properties["unsupported"] = make(chan int)
	usage, err = storageChangesUsage("test", changes, false)
	require.NoError(t, err)
	require.Equal(t, storageUsage{edges: -1}, usage)
	_, err = storageChangesUsage("test", changes, true)
	require.Error(t, err)
}

func TestManagedUsagePublicationUpdatesCanonicalByteCache(t *testing.T) {
	info := &DatabaseInfo{usageNodes: 2, nodeSize: 418, totalSize: 418, sizeInitialized: true, usageInitialized: true}
	manager := &DatabaseManager{databases: map[string]*DatabaseInfo{"test": info}}
	require.NoError(t, info.reserveStorageUsage("test", "accepted", storageUsage{nodeBytes: 40}, StorageLimits{MaxBytes: 458}))
	info.publishStorageUsage("accepted", storageUsage{nodeBytes: 40})
	total, nodes, edges := manager.GetStorageSize(context.Background(), "test")
	require.Equal(t, int64(458), total)
	require.Equal(t, int64(458), nodes)
	require.Zero(t, edges)
	require.Empty(t, info.pending)
	require.Equal(t, reservedUsage{}, info.reserved)
	// A peer publication has no local reservation and is nevertheless actual usage.
	info.publishStorageUsage("peer", storageUsage{nodes: 1, edges: 1, nodeBytes: 10, edgeBytes: 20})
	total, nodes, edges = manager.GetStorageSize(context.Background(), "test")
	require.Equal(t, int64(488), total)
	require.Equal(t, int64(468), nodes)
	require.Equal(t, int64(20), edges)
	require.Equal(t, int64(3), info.usageNodes)
	require.Equal(t, int64(1), info.usageEdges)
}

func TestManagedUsageDuplicateReservationAndLoweredLimits(t *testing.T) {
	info := &DatabaseInfo{usageNodes: 2, usageEdges: 2, nodeSize: 418, totalSize: 418, sizeInitialized: true, usageInitialized: true}
	grow := storageUsage{nodes: 1, edges: 1, nodeBytes: 40}
	require.NoError(t, info.reserveStorageUsage("test", "accepted", grow, StorageLimits{MaxNodes: 3, MaxEdges: 3, MaxBytes: 458}))
	err := info.reserveStorageUsage("test", "accepted", grow, StorageLimits{MaxNodes: 3, MaxEdges: 3, MaxBytes: 458})
	require.ErrorAs(t, err, new(storageReservationExists))
	require.Len(t, info.pending, 1)
	require.Equal(t, reservedUsage{nodes: 1, edges: 1, bytes: 40}, info.reserved)
	// Lowered quotas do not reject nongrowth, but unpublished deletions cannot
	// fund new growth and discarding them does not change any actual field.
	lowered := StorageLimits{MaxNodes: 1, MaxEdges: 1, MaxBytes: 100}
	require.NoError(t, info.reserveStorageUsage("test", "shrink", storageUsage{nodes: -1, edges: -1, nodeBytes: -40}, lowered))
	require.ErrorIs(t, info.reserveStorageUsage("test", "growth", grow, lowered), ErrStorageLimitExceeded)
	info.discardStorageUsage("shrink")
	info.discardStorageUsage("accepted")
	info.discardStorageUsage("accepted")
	require.Empty(t, info.pending)
	require.Equal(t, reservedUsage{}, info.reserved)
	require.Equal(t, int64(2), info.usageNodes)
	require.Equal(t, int64(2), info.usageEdges)
	require.Equal(t, int64(418), info.nodeSize)
	require.Equal(t, int64(418), info.totalSize)
}
