package multidb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionQuotaAdmissionReadFailureRollsBack(t *testing.T) {
	for _, at := range []string{"node", "edge", "nodes", "edges", "bytes"} {
		t.Run(at, func(t *testing.T) {
			manager, db := setupTestManager(t)
			engine, err := manager.GetStorage(db)
			require.NoError(t, err)
			edgeCase := at == "edge" || at == "edges"
			if edgeCase {
				_, err = engine.CreateNode(&storage.Node{ID: "a"})
				require.NoError(t, err)
				_, err = engine.CreateNode(&storage.Node{ID: "b"})
				require.NoError(t, err)
			}
			limits := &Limits{Storage: StorageLimits{MaxNodes: 10, MaxEdges: 10}}
			if at == "bytes" {
				limits = &Limits{Storage: StorageLimits{MaxBytes: 10000}}
			}
			require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, limits))
			tracker := engine.(*sizeTrackingEngine)
			underlying := tracker.Engine
			injected := errors.New("test storage read failed")
			stored, ok := storage.StoredReader(underlying)
			require.True(t, ok)
			faults := storagefault.NewReadFailureEngine(underlying.(storagefault.ReadEngine), stored, injected)
			faults.SetFailure(storagefault.ReadOperation(at))
			tracker.Engine = faults
			tx, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace(db))
			if edgeCase {
				require.NoError(t, tx.CreateEdge(&storage.Edge{ID: storage.EdgeID(db + ":e"), StartNode: storage.NodeID(db + ":a"), EndNode: storage.NodeID(db + ":b"), Type: "R"}))
			} else {
				_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(db + ":rejected")})
				require.NoError(t, err)
			}
			commitErr := tracker.CommitTransaction(context.Background(), tx)
			require.ErrorIs(t, commitErr, injected)
			require.True(t, nornicerrors.IsCommitRolledBack(commitErr))
			require.False(t, tx.IsActive())
			count, err := underlying.NodeCount()
			require.NoError(t, err)
			if edgeCase {
				require.EqualValues(t, 2, count)
			} else {
				require.Zero(t, count)
			}
			edges, err := underlying.EdgeCount()
			require.NoError(t, err)
			require.Zero(t, edges)
		})
	}
}

func TestTransactionQuotaNamespaceAndDatabaseLifecycle(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	tracker := engine.(*sizeTrackingEngine)
	wrong, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
	require.NoError(t, err)
	_, err = wrong.CreateNode(&storage.Node{ID: "other:rejected"})
	require.NoError(t, err)
	err = tracker.CommitTransaction(context.Background(), wrong)
	require.ErrorIs(t, err, storage.ErrCrossNamespaceTransaction)
	require.True(t, nornicerrors.IsCommitRolledBack(err))
	staged := stagedQuotaNode(t, manager, db, "dropped")
	require.NoError(t, manager.DropDatabase(t.Context(), db))
	err = tracker.CommitTransaction(context.Background(), staged)
	require.ErrorIs(t, err, ErrDatabaseNotFound)
	require.False(t, staged.IsActive())
}

func TestTransactionQuotaReadOnlyAndNilContext(t *testing.T) {
	manager, db := setupTestManagerWithLimits(t, &Limits{Storage: StorageLimits{MaxNodes: 1}})
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	tracker := engine.(*sizeTrackingEngine)
	require.NoError(t, tracker.CommitTransaction(nil, stagedQuotaNode(t, manager, db, "nil-context")))
	read, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
	require.NoError(t, err)
	tracker.writeMu.Lock()
	done := make(chan error, 1)
	go func() { done <- tracker.CommitTransaction(context.Background(), read) }()
	select {
	case err := <-done:
		tracker.writeMu.Unlock()
		require.NoError(t, err)
	case <-time.After(time.Second):
		tracker.writeMu.Unlock()
		<-done
		t.Fatal("read-only commit waited for write admission")
	}
}

func TestTransactionQuotaSizeInitializationWithConcurrentUnlimitedCommits(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	tracker := engine.(*sizeTrackingEngine)
	const writers = 16
	transactions := make([]*storage.BadgerTransaction, writers)
	for i := range transactions {
		transactions[i] = stagedQuotaNode(t, manager, db, string(rune('a'+i)))
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, writers)
	for _, tx := range transactions {
		wg.Add(1)
		go func(tx *storage.BadgerTransaction) {
			defer wg.Done()
			<-start
			results <- tracker.CommitTransaction(context.Background(), tx)
		}(tx)
	}
	close(start)
	manager.GetStorageSize(context.Background(), db)
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	cached, _, _ := manager.GetStorageSize(context.Background(), db)
	nodes, edges, err := manager.calculateStorageSizeFromEngine(context.Background(), engine)
	require.NoError(t, err)
	require.Equal(t, nodes+edges, cached, "lazy initialization must not miss or double-count concurrent publications")
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, writers, count)
}

func TestTransactionQuotaCanceledMergeAdmissionIsNotAConflict(t *testing.T) {
	var admission writeAdmission
	admission.Lock()
	defer admission.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := admission.acquire(ctx, true, true)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, storage.ErrConflict)
}

func TestTransactionQuotaCanonicalPropertylessSize(t *testing.T) {
	node := &storage.Node{ID: "n"}
	nilSize, err := calculateNodeSize(node)
	require.NoError(t, err)
	node.Properties = map[string]any{}
	emptySize, err := calculateNodeSize(node)
	require.NoError(t, err)
	require.Equal(t, nilSize, emptySize)
	require.NotNil(t, node.Properties, "sizing must not mutate the caller")
	edge := &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R"}
	nilSize, err = calculateEdgeSize(edge)
	require.NoError(t, err)
	edge.Properties = map[string]any{}
	emptySize, err = calculateEdgeSize(edge)
	require.NoError(t, err)
	require.Equal(t, nilSize, emptySize)
	require.NotNil(t, edge.Properties)
}

func TestTransactionQuotaCanceledReadOnlyReleasesTransaction(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	read, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = engine.(*sizeTrackingEngine).CommitTransaction(ctx, read)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, read.IsActive(), "canceled read-only commit must close its snapshot")
	require.True(t, nornicerrors.IsCommitRolledBack(err))
}
