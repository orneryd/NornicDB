package multidb

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Direct updates must use the same stored prior/after pair as transactional
// replacements: hidden does not mean absent, and UpdateNode can insert.
func TestManagedDirectWriteQuotaUsesStoredPair(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint("buffered=", buffered), func(t *testing.T) {
			base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered})
			require.NoError(t, err)
			manager, err := NewDatabaseManager(base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			for _, entity := range []string{"node", "edge"} {
				t.Run(entity, func(t *testing.T) {
					db := "growth_" + entity
					require.NoError(t, manager.CreateDatabase(db))
					engine, err := manager.GetStorage(db)
					require.NoError(t, err)
					for _, id := range []storage.NodeID{"a", "b"} {
						_, err = engine.CreateNode(&storage.Node{ID: id})
						require.NoError(t, err)
					}
					if entity == "node" {
						_, err = engine.CreateNode(&storage.Node{ID: "n", Properties: map[string]any{"payload": "small"}})
						require.NoError(t, err)
						require.NoError(t, base.SetNodeManuallySuppressed(storage.NodeID(db+":n"), true))
					} else {
						require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "small"}}))
						require.NoError(t, base.SetEdgeManuallySuppressed(storage.EdgeID(db+":e"), true))
					}
					require.NoError(t, base.FlushWriteBehind())
					before, _, _ := manager.GetStorageSize(db)
					require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxBytes: before}}))
					if entity == "node" {
						node, err := storage.GetStoredNode(engine, "n")
						require.NoError(t, err)
						node.Properties["payload"] = strings.Repeat("x", 4096)
						require.ErrorIs(t, engine.UpdateNode(node), ErrStorageLimitExceeded)
					} else {
						edge, err := storage.GetStoredEdge(engine, "e")
						require.NoError(t, err)
						edge.Properties["payload"] = strings.Repeat("x", 4096)
						require.ErrorIs(t, engine.UpdateEdge(edge), ErrStorageLimitExceeded)
					}
					after, _, _ := manager.GetStorageSize(db)
					require.Equal(t, before, after)
				})
			}
			t.Run("upsert-count", func(t *testing.T) {
				db := "upsert_count"
				require.NoError(t, manager.CreateDatabase(db))
				engine, err := manager.GetStorage(db)
				require.NoError(t, err)
				_, err = engine.CreateNode(&storage.Node{ID: "existing"})
				require.NoError(t, err)
				require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: 1}}))
				require.ErrorIs(t, engine.UpdateNode(&storage.Node{ID: "new"}), ErrStorageLimitExceeded)
				_, err = storage.GetStoredNode(engine, "new")
				require.ErrorIs(t, err, storage.ErrNotFound)
			})
		})
	}
}

type managedAccountingReadProbe struct {
	storage.Engine
	storage.StoredEntityReader
	failure      error
	nodes, edges int
}

func (p *managedAccountingReadProbe) GetInnerEngine() storage.Engine { return p.Engine }
func (p *managedAccountingReadProbe) GetStoredNode(id storage.NodeID) (*storage.Node, error) {
	p.nodes++
	if p.failure != nil || p.nodes > 1 {
		return nil, errors.New("stored prior node read failed")
	}
	return p.StoredEntityReader.GetStoredNode(id)
}
func (p *managedAccountingReadProbe) GetStoredEdge(id storage.EdgeID) (*storage.Edge, error) {
	p.edges++
	if p.failure != nil || p.edges > 1 {
		return nil, errors.New("stored prior edge read failed")
	}
	return p.StoredEntityReader.GetStoredEdge(id)
}

// The old direct wrapper re-read after committing and marked accounting dirty
// on failure. The common owner needs only one prior read, fails before commit
// if that read fails, and publishes the already admitted stored-image batch.
func TestManagedWritesUseOnePriorReadWithoutPostCommitSizing(t *testing.T) {
	for _, entity := range []string{"node", "edge"} {
		for _, op := range []string{"create", "update", "delete"} {
			for _, failRead := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/fail=%v", entity, op, failRead), func(t *testing.T) {
					manager, db := setupTestManager(t)
					engine, err := manager.GetStorage(db)
					require.NoError(t, err)
					for _, id := range []storage.NodeID{"a", "b"} {
						_, err = engine.CreateNode(&storage.Node{ID: id})
						require.NoError(t, err)
					}
					if op != "create" {
						if entity == "node" {
							_, err = engine.CreateNode(&storage.Node{ID: "n"})
							require.NoError(t, err)
						} else {
							require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}))
						}
					}
					before, _, _ := manager.GetStorageSize(db)
					tracker := engine.(*sizeTrackingEngine)
					underlying := tracker.Engine
					stored, ok := storage.StoredReader(underlying)
					require.True(t, ok)
					probe := &managedAccountingReadProbe{Engine: underlying, StoredEntityReader: stored}
					if failRead {
						probe.failure = errors.New("inject stored read failure")
					}
					tracker.Engine = probe
					if entity == "node" {
						switch op {
						case "create":
							_, err = engine.CreateNode(&storage.Node{ID: "n"})
						case "update":
							err = engine.UpdateNode(&storage.Node{ID: "n", Properties: map[string]any{"payload": "small"}})
						case "delete":
							err = engine.DeleteNode("n")
						}
						require.Equal(t, 1, probe.nodes)
						require.Zero(t, probe.edges)
					} else {
						edge := &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}
						switch op {
						case "create":
							err = engine.CreateEdge(edge)
						case "update":
							edge.Properties = map[string]any{"payload": "small"}
							err = engine.UpdateEdge(edge)
						case "delete":
							err = engine.DeleteEdge("e")
						}
						require.Zero(t, probe.nodes)
						require.Equal(t, 1, probe.edges)
					}
					if failRead {
						require.ErrorContains(t, err, "stored prior")
						require.True(t, nornicerrors.IsCommitRolledBack(err))
						after, _, _ := manager.GetStorageSize(db)
						require.Equal(t, before, after)
					} else {
						require.NoError(t, err)
						require.True(t, storageSizeInitialized(t, manager, db))
					}
				})
			}
		}
	}
}

type managedBeginFailure struct {
	storage.Engine
	err error
}

func (b *managedBeginFailure) BeginTransaction() (*storage.BadgerTransaction, error) {
	return nil, b.err
}

func TestManagedWritesPropagateStorageStartFailure(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	injected := errors.New("test transaction start failed")
	engine.(*sizeTrackingEngine).Engine = &managedBeginFailure{Engine: engine.(*sizeTrackingEngine).Engine, err: injected}
	for _, write := range []func() error{
		func() error { _, err := engine.CreateNode(&storage.Node{ID: "n"}); return err },
		func() error { return engine.UpdateNode(&storage.Node{ID: "n"}) },
		func() error { return engine.DeleteNode("n") },
		func() error { return engine.CreateEdge(&storage.Edge{ID: "e"}) },
		func() error { return engine.UpdateEdge(&storage.Edge{ID: "e"}) },
		func() error { return engine.DeleteEdge("e") },
	} {
		err := write()
		require.ErrorIs(t, err, injected)
		require.True(t, nornicerrors.IsCommitRolledBack(err))
	}
}

func TestManagedDirectWritesPreserveStorageContract(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	stamp := time.Now().Add(-time.Hour).UTC()
	_, err = engine.CreateNode(&storage.Node{ID: "a", CreatedAt: stamp, UpdatedAt: stamp})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b"})
	require.NoError(t, err)
	base := manager.inner.(*storage.MemoryEngine)
	require.NoError(t, base.SetNodeManuallySuppressed(storage.NodeID(db+":a"), true))
	_, err = engine.CreateNode(&storage.Node{ID: "a"})
	require.ErrorIs(t, err, storage.ErrAlreadyExists, "direct create remains strict for hidden IDs")
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "a", Properties: map[string]any{"payload": "small"}}))
	stored, err := storage.GetStoredNode(engine, "a")
	require.NoError(t, err)
	require.True(t, stamp.Equal(stored.CreatedAt), "zero creation time preserves the stored value on update")
	require.False(t, stored.UpdatedAt.IsZero())
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}), "direct creation accepts a stored hidden endpoint")
	require.NoError(t, base.SetEdgeManuallySuppressed(storage.EdgeID(db+":e"), true))
	require.ErrorIs(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}), storage.ErrAlreadyExists)
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "upsert"}))
	_, err = storage.GetStoredNode(engine, "upsert")
	require.NoError(t, err)
	manager.GetStorageSize(db)
	require.NoError(t, engine.DeleteNode("a"), "delete cascades the hidden relationship")
	_, err = storage.GetStoredEdge(engine, "e")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.ErrorIs(t, engine.UpdateEdge(&storage.Edge{ID: "missing", StartNode: "a", EndNode: "b"}), storage.ErrNotFound)
	require.ErrorIs(t, engine.DeleteEdge("e"), storage.ErrNotFound)
}

func TestManagedBulkRateRejectionNeedsNoCompensatingWrite(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	before, _, _ := manager.GetStorageSize(db)
	require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Rate: RateLimits{MaxWritesPerSecond: 1}}))
	err = engine.BulkCreateNodes([]*storage.Node{{ID: "a"}, {ID: "b"}})
	require.ErrorIs(t, err, ErrRateLimitExceeded)
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count)
	after, _, _ := manager.GetStorageSize(db)
	require.Equal(t, before, after)
}
