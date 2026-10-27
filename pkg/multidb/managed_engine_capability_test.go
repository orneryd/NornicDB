package multidb

import (
	"context"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var _ storage.ManagedEntityEngine = (*storage.MemoryEngine)(nil)
var _ storage.ManagedEntityEngine = (*storage.BadgerEngine)(nil)

type opaqueEntityEngine struct {
	storage.Engine
	createCalls atomic.Int32
}

func (e *opaqueEntityEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	e.createCalls.Add(1)
	return e.Engine.CreateNode(node)
}

func TestManagedEngineCapabilityFailurePrecedesEveryWrite(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	opaque := &opaqueEntityEngine{Engine: base}
	_, err := NewDatabaseManager(t.Context(), opaque, nil)
	require.ErrorIs(t, err, storage.ErrInvalidData, "catalog itself now requires receipt capability")
	require.Zero(t, opaque.createCalls.Load(), "unsupported catalog cannot issue a probing write")
	manager, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	const db = "opaque"
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: db}))
	opaque.createCalls.Store(0)
	// The catalog has a real receipt owner; this independent graph proxy hides
	// managed graph capabilities to preserve the original write-boundary test.
	tracker := newSizeTrackingEngine(storage.NewNamespacedEngine(opaque, db), manager, db).(*sizeTrackingEngine)
	first := storage.NodeID("unlimited")
	id, err := tracker.CreateNode(&storage.Node{ID: first})
	require.NoError(t, err)
	require.Equal(t, first, id)
	require.EqualValues(t, 1, opaque.createCalls.Load())
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, &Limits{Storage: StorageLimits{MaxNodes: 10}}))
	_, err = tracker.CreateNode(&storage.Node{ID: storage.NodeID(db + ":limited")})
	require.ErrorIs(t, err, storage.ErrInvalidData)
	require.ErrorContains(t, err, "ManagedEntityEngine")
	require.ErrorIs(t, tracker.BulkCreateNodes([]*storage.Node{{ID: storage.NodeID(db + ":bulk-a")}, {ID: storage.NodeID(db + ":bulk-b")}}), storage.ErrInvalidData)
	require.EqualValues(t, 1, opaque.createCalls.Load(), "capability validation must not probe a different store or invoke a write")
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace(db))
	_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(db + ":explicit")})
	require.NoError(t, err)
	err = tracker.CommitTransaction(context.Background(), tx)
	require.ErrorIs(t, err, storage.ErrInvalidData)
	require.True(t, nornicerrors.IsCommitRolledBack(err))
	require.False(t, tx.IsActive())
	nodes, err := storage.NewNamespacedEngine(base, db).NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, nodes)
	stored, err := base.GetStoredNode(storage.NodeID(db + ":" + string(first)))
	require.NoError(t, err)
	require.Equal(t, storage.NodeID(db+":"+string(first)), stored.ID)
}
