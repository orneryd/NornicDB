package multidb

import (
	"errors"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestManagedUsageObserverPreparesImmutableDelta(t *testing.T) {
	before := &storage.Node{ID: "test:n", Properties: map[string]any{"value": "small"}}
	after := storage.CopyNode(before)
	after.Properties["value"] = "a larger stored value"
	delta, err := storageChangesUsage("test", []storage.StoredEntityChange{{BeforeNode: before, AfterNode: after}}, true)
	require.NoError(t, err)
	info := &DatabaseInfo{usageNodes: 1, nodeSize: 500, totalSize: 500, sizeInitialized: true, usageInitialized: true}
	require.NoError(t, info.reserveStorageUsage("test", "accepted", delta, StorageLimits{MaxBytes: 500 + delta.nodeBytes}))
	publish, err := info.storageUsageObserver("test", true)(storage.StoredEntityPublication{Namespace: "test", LogicalID: "accepted", Changes: []storage.StoredEntityChange{{BeforeNode: before, AfterNode: after}}})
	require.NoError(t, err)
	require.NotNil(t, publish)
	require.Equal(t, int64(500), info.totalSize)
	require.Len(t, info.pending, 1)
	// Post-prepare mutable writer input cannot alter the captured scalar usage.
	after.Properties["value"] = "changed again after sizing"
	publish()
	require.Equal(t, int64(500)+delta.nodeBytes, info.nodeSize)
	require.Equal(t, info.nodeSize, info.totalSize)
	require.Empty(t, info.pending)
	require.Equal(t, reservedUsage{}, info.reserved)
	// Peer commits publish actual usage without running local quota admission.
	peer := &storage.Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "LINK"}
	peerDelta, err := storageChangesUsage("test", []storage.StoredEntityChange{{AfterEdge: peer}}, true)
	require.NoError(t, err)
	publish, err = info.storageUsageObserver("test", true)(storage.StoredEntityPublication{Namespace: "test", LogicalID: "peer", Changes: []storage.StoredEntityChange{{AfterEdge: peer}}})
	require.NoError(t, err)
	publish()
	require.Equal(t, int64(1), info.usageEdges)
	require.Equal(t, peerDelta.edgeBytes, info.edgeSize)
	require.Equal(t, info.nodeSize+info.edgeSize, info.totalSize)
}

func TestManagedUsageObserverSizingFailureHasNoPublication(t *testing.T) {
	info := &DatabaseInfo{usageNodes: 1, nodeSize: 500, totalSize: 500, sizeInitialized: true, usageInitialized: true}
	require.NoError(t, info.reserveStorageUsage("test", "accepted", storageUsage{nodeBytes: 10}, StorageLimits{MaxBytes: 510}))
	bad := &storage.Node{ID: "test:bad", Properties: map[string]any{"unsupported": make(chan int)}}
	publish, err := info.storageUsageObserver("test", true)(storage.StoredEntityPublication{Namespace: "test", LogicalID: "accepted", Changes: []storage.StoredEntityChange{{AfterNode: bad}}})
	require.Error(t, err)
	require.Nil(t, publish)
	require.Equal(t, int64(500), info.totalSize)
	require.Equal(t, int64(1), info.usageNodes)
	require.Len(t, info.pending, 1)
	require.Equal(t, reservedUsage{bytes: 10}, info.reserved)
	info.discardStorageUsage("accepted")
	require.Empty(t, info.pending)
	require.Equal(t, reservedUsage{}, info.reserved)
	// Count-only installation preserves lazy byte encoding, even if measuring an
	// arbitrary helper input would fail. Storage validation remains its own owner.
	publish, err = info.storageUsageObserver("test", false)(storage.StoredEntityPublication{Namespace: "test", LogicalID: "count-only", Changes: []storage.StoredEntityChange{{AfterNode: bad}}})
	require.NoError(t, err)
	publish()
	require.Equal(t, int64(2), info.usageNodes)
	require.Equal(t, int64(500), info.totalSize)
}

func TestManagedUsageSnapshotUpgradeAndNativePublication(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	for _, id := range []storage.NodeID{"test:n", "test:m"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"value": "initial"}})
		require.NoError(t, err)
	}
	info := &DatabaseInfo{}
	manager := &DatabaseManager{inner: engine, databases: map[string]*DatabaseInfo{"test": info}}
	require.NoError(t, manager.initializeStorageUsage("test", engine, false, nil))
	require.True(t, info.usageInitialized)
	require.False(t, info.sizeInitialized)
	require.Equal(t, int64(2), info.usageNodes)
	require.Zero(t, info.totalSize)
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "test:n", Properties: map[string]any{"value": "larger value before bytes are requested"}}))
	require.Equal(t, int64(2), info.usageNodes)
	require.False(t, info.sizeInitialized)
	require.Zero(t, info.totalSize)
	// Upgrade counts and bytes from one exact storage cut, keeping active
	// reservations rather than resetting accepted work's positive allowance.
	require.NoError(t, info.reserveStorageUsage("test", "still-pending", storageUsage{nodes: 1}, StorageLimits{MaxNodes: 3}))
	require.NoError(t, manager.initializeStorageUsage("test", engine, true, nil))
	require.True(t, info.sizeInitialized)
	require.Len(t, info.pending, 1)
	require.Equal(t, reservedUsage{nodes: 1}, info.reserved)
	check := func(nodes, edges int64) {
		t.Helper()
		wantNodeBytes, wantEdgeBytes, err := manager.calculateStorageSizeFromEngine(storage.NewNamespacedEngine(engine, "test"))
		require.NoError(t, err)
		total, nodeBytes, edgeBytes := manager.GetStorageSize("test")
		require.Equal(t, wantNodeBytes, nodeBytes)
		require.Equal(t, wantEdgeBytes, edgeBytes)
		require.Equal(t, wantNodeBytes+wantEdgeBytes, total)
		require.Equal(t, nodes, info.usageNodes)
		require.Equal(t, edges, info.usageEdges)
	}
	check(2, 0)
	// Native/peer-origin publication is accounted without local reservation or
	// post-consensus quota rejection, even though the local quota is now full.
	_, err = engine.CreateNode(&storage.Node{ID: "test:peer", Properties: map[string]any{"value": "peer"}})
	require.NoError(t, err)
	check(3, 0)
	require.ErrorIs(t, info.reserveStorageUsage("test", "competitor", storageUsage{nodes: 1}, StorageLimits{MaxNodes: 3}), ErrStorageLimitExceeded)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "LINK", Properties: map[string]any{"value": "first"}}))
	check(3, 1)
	require.NoError(t, engine.UpdateEdge(&storage.Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "LINK", Properties: map[string]any{"value": "larger stored edge"}}))
	check(3, 1)
	require.NoError(t, engine.DeleteNode("test:n"))
	check(2, 0)
	info.discardStorageUsage("still-pending")
	require.Empty(t, info.pending)
	// A later count-only request cannot downgrade the installed byte observer.
	require.NoError(t, manager.initializeStorageUsage("test", engine, false, nil))
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "test:m", Properties: map[string]any{"value": "updated after byte initialization"}}))
	check(2, 0)
}

func TestManagedUsageConcurrentInitializationAndPublication(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err = engine.CreateNode(&storage.Node{ID: "test:seed", Properties: map[string]any{"value": "seed"}})
	require.NoError(t, err)
	info := &DatabaseInfo{}
	manager := &DatabaseManager{inner: engine, databases: map[string]*DatabaseInfo{"test": info}}
	const writers = 16
	start := make(chan struct{})
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := manager.initializeStorageUsage("test", engine, i%2 == 0, nil); err != nil {
				results <- err
				return
			}
			_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:n%d", i)), Properties: map[string]any{"value": fmt.Sprintf("value-%d", i)}})
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.True(t, info.usageInitialized)
	require.True(t, info.sizeInitialized)
	require.Equal(t, int64(writers+1), info.usageNodes)
	require.Zero(t, info.usageEdges)
	wantNodes, wantEdges, err := manager.calculateStorageSizeFromEngine(storage.NewNamespacedEngine(engine, "test"))
	require.NoError(t, err)
	total, nodes, edges := manager.GetStorageSize("test")
	require.Equal(t, wantNodes, nodes)
	require.Equal(t, wantEdges, edges)
	require.Equal(t, wantNodes+wantEdges, total)
	require.Empty(t, info.pending)
	require.Equal(t, reservedUsage{}, info.reserved)
}

func TestManagedUsageBufferedPublicationRetiresReservationOnce(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	for _, id := range []storage.NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	info := &DatabaseInfo{}
	manager := &DatabaseManager{inner: engine, databases: map[string]*DatabaseInfo{"test": info}}
	require.NoError(t, manager.initializeStorageUsage("test", engine, true, nil))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&storage.Node{ID: "test:c", Properties: map[string]any{"value": "buffered"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&storage.Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:c", Type: "LINK"}))
	require.NoError(t, info.reserveStorageUsage("test", tx.ID, storageUsage{nodes: 1, edges: 1}, StorageLimits{MaxNodes: 3, MaxEdges: 1}))
	require.NoError(t, tx.Commit())
	require.Empty(t, info.pending, "ACK publication retires the same logical reservation")
	require.Equal(t, reservedUsage{}, info.reserved)
	require.Equal(t, int64(3), info.usageNodes)
	require.Equal(t, int64(1), info.usageEdges)
	wantNodes, wantEdges, err := manager.calculateStorageSizeFromEngine(storage.NewNamespacedEngine(engine, "test"))
	require.NoError(t, err)
	total, nodeBytes, edgeBytes := manager.GetStorageSize("test")
	require.Equal(t, wantNodes, nodeBytes)
	require.Equal(t, wantEdges, edgeBytes)
	require.Equal(t, wantNodes+wantEdges, total)
	require.NoError(t, engine.FlushWriteBehind())
	afterTotal, afterNodes, afterEdges := manager.GetStorageSize("test")
	require.Equal(t, total, afterTotal)
	require.Equal(t, nodeBytes, afterNodes)
	require.Equal(t, edgeBytes, afterEdges)
	require.Equal(t, int64(3), info.usageNodes)
	require.Equal(t, int64(1), info.usageEdges)
	require.Empty(t, info.pending)
	require.Equal(t, reservedUsage{}, info.reserved)
}

func TestManagedUsageActualReadBoundaryFailures(t *testing.T) {
	for _, mode := range []storagefault.ReadOperation{storagefault.NodeRead, storagefault.EdgeRead, storagefault.NodeCountRead, storagefault.EdgeCountRead, storagefault.NodeStreamRead} {
		t.Run(string(mode), func(t *testing.T) {
			engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			edgeCase := mode == storagefault.EdgeRead || mode == storagefault.EdgeCountRead
			if edgeCase {
				for _, id := range []storage.NodeID{"test:a", "test:b"} {
					_, err := engine.CreateNode(&storage.Node{ID: id})
					require.NoError(t, err)
				}
			}
			injected := errors.New("actual precommit reader failed")
			faults := storagefault.NewReadFailureEngine(engine, engine, injected)
			info := &DatabaseInfo{}
			manager := &DatabaseManager{inner: engine, databases: map[string]*DatabaseInfo{"test": info}}
			pointRead := mode == storagefault.NodeRead || mode == storagefault.EdgeRead
			if pointRead {
				require.NoError(t, manager.initializeStorageUsage("test", engine, false, faults))
			}
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			if edgeCase {
				require.NoError(t, tx.CreateEdge(&storage.Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
			} else {
				_, err = tx.CreateNode(&storage.Node{ID: "test:rejected"})
				require.NoError(t, err)
			}
			faults.SetFailure(mode)
			if pointRead {
				err = tx.Commit()
			} else {
				err = manager.initializeStorageUsage("test", engine, mode == storagefault.NodeStreamRead, faults)
				require.NoError(t, tx.Rollback())
			}
			require.ErrorIs(t, err, injected)
			require.False(t, tx.IsActive())
			faults.SetFailure("")
			nodes, err := engine.NodeCount()
			require.NoError(t, err)
			if edgeCase {
				require.EqualValues(t, 2, nodes)
			} else {
				require.Zero(t, nodes)
			}
			edges, err := engine.EdgeCount()
			require.NoError(t, err)
			require.Zero(t, edges)
			require.Empty(t, info.pending)
			require.Equal(t, reservedUsage{}, info.reserved)
		})
	}
}

func TestManagedUsageLazyCountThenByteUpgradeIncludesStoredVectorsAndHiddenEntities(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	vector := make([]float32, 16384)
	for i := range vector {
		vector[i] = float32(i%97) / 97
	}
	for _, id := range []storage.NodeID{"test:a", "test:b", "test:hidden"} {
		node := &storage.Node{ID: id, ChunkEmbeddings: [][]float32{vector}}
		node.SetRuntimeMetadata(storage.NodeRuntimeMetadata{VisibilitySuppressed: id == "test:hidden"})
		_, err = engine.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "test:hidden-edge", StartNode: "test:a", EndNode: "test:b", Type: "LINK", VisibilitySuppressed: true}))
	info := &DatabaseInfo{}
	manager := &DatabaseManager{inner: engine, databases: map[string]*DatabaseInfo{"test": info}}
	require.NoError(t, manager.initializeStorageUsage("test", engine, false, nil))
	require.Equal(t, int64(3), info.usageNodes)
	require.Equal(t, int64(1), info.usageEdges)
	require.False(t, info.sizeInitialized)
	require.Zero(t, info.totalSize)
	require.NoError(t, info.reserveStorageUsage("test", "pending", storageUsage{nodes: 1}, StorageLimits{MaxNodes: 4}))
	require.NoError(t, manager.initializeStorageUsage("test", engine, true, nil))
	require.Equal(t, int64(3), info.usageNodes)
	require.Equal(t, int64(1), info.usageEdges)
	require.Equal(t, reservedUsage{nodes: 1}, info.reserved)
	require.Len(t, info.pending, 1)
	wantNodes, wantEdges, err := manager.calculateStorageSizeFromEngine(storage.NewNamespacedEngine(engine, "test"))
	require.NoError(t, err)
	require.Equal(t, wantNodes, info.nodeSize)
	require.Equal(t, wantEdges, info.edgeSize)
	require.Equal(t, wantNodes+wantEdges, info.totalSize)
	require.Greater(t, info.nodeSize, int64(3*len(vector)*4))
	hidden, err := engine.GetStoredNode("test:hidden")
	require.NoError(t, err)
	require.True(t, hidden.RuntimeMetadata().VisibilitySuppressed)
	require.Equal(t, vector, hidden.ChunkEmbeddings[0])
}
