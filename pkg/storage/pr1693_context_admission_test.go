package storage

import (
	"context"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// canceledContext is the caller that has already given up. These cases pin the
// context-first admission of the metadata and schema owners: a refused call
// reports its own error and leaves no partially applied state, while the same
// entry point still accepts the work under a live caller.
func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// TestWriteAccessMetaRefusesCanceledAdmission proves the metadata write owner
// does not persist an entry for a canceled caller, and that the retry path is
// the same canonical entry point.
func TestWriteAccessMetaRefusesCanceledAdmission(t *testing.T) {
	engine := newTestEngine(t)
	entry := &knowledgepolicy.AccessMetaEntry{TargetID: "test:cancel", TargetScope: knowledgepolicy.ScopeNode}
	request := knowledgepolicy.AccessMetaWriteRequest{
		Mode:     knowledgepolicy.RecordState,
		EntityID: "test:cancel",
		Entry:    entry,
	}

	written, err := engine.WriteAccessMeta(canceledContext(), request)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, written, "a refused write reports no acceptance")

	stored, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: "test:cancel"})
	require.NoError(t, err)
	require.Nil(t, stored.Entry, "a canceled caller leaves no published entry")

	written, err = engine.WriteAccessMeta(context.Background(), request)
	require.NoError(t, err)
	require.True(t, written)

	stored, err = engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: "test:cancel"})
	require.NoError(t, err)
	require.NotNil(t, stored.Entry)
	require.Equal(t, "test:cancel", stored.Entry.TargetID)
}

// TestPutIndexEntryCatalogRefusesCanceledAdmission proves the catalog write is
// admitted under the caller's lifetime, not a detached one.
func TestPutIndexEntryCatalogRefusesCanceledAdmission(t *testing.T) {
	engine := newTestEngine(t)
	cat := &IndexEntryCatalog{IndexKeys: [][]byte{labelIndexKey("person", 1)}}

	require.ErrorIs(t, engine.PutIndexEntryCatalog(canceledContext(), "test:cancel", cat), context.Canceled)
	stored, err := engine.GetIndexEntryCatalog("test:cancel")
	require.NoError(t, err)
	require.Nil(t, stored, "a canceled caller publishes no catalog")

	require.NoError(t, engine.PutIndexEntryCatalog(context.Background(), "test:cancel", cat))
	stored, err = engine.GetIndexEntryCatalog("test:cancel")
	require.NoError(t, err)
	require.NotNil(t, stored)
}

// TestSetIndexTombstonesRefusesCanceledAdmission proves a refused tombstone
// batch writes no visibility marker, and that the accepted batch still does.
func TestSetIndexTombstonesRefusesCanceledAdmission(t *testing.T) {
	engine := newTestEngine(t)
	key := labelIndexKey("person", 1)

	require.ErrorIs(t, engine.SetIndexTombstones(canceledContext(), [][]byte{key}, true), context.Canceled)
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		if hasIndexTombstone(txn, key) {
			t.Error("a canceled caller must not write a tombstone")
		}
		return nil
	}))

	require.NoError(t, engine.SetIndexTombstones(context.Background(), [][]byte{key}, true))
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		if !hasIndexTombstone(txn, key) {
			t.Error("the accepted batch writes its tombstone")
		}
		return nil
	}))
}

// TestSetManualSuppressionRefusesCanceledAdmission proves the manual-visibility
// write is admitted under the caller's lifetime: a canceled caller is refused
// before its stored-body read, so a missing entity and a canceled caller report
// different outcomes.
func TestSetManualSuppressionRefusesCanceledAdmission(t *testing.T) {
	engine := newTestEngine(t)

	require.ErrorIs(t, engine.SetManualSuppression(canceledContext(), StoredEntityNodeKind, "test:absent", true), context.Canceled)
	require.ErrorIs(t, engine.SetManualSuppression(context.Background(), StoredEntityNodeKind, "test:absent", true), ErrNotFound)
}

// TestEnqueueDeindexIfSuppressedRefusesCanceledAdmission proves the on-access
// resuppression write is admitted under the caller's lifetime.
func TestEnqueueDeindexIfSuppressedRefusesCanceledAdmission(t *testing.T) {
	engine := newTestEngine(t)
	engine.SetDecayEnabled(true)

	transitioned, err := engine.EnqueueDeindexIfSuppressed(canceledContext(), "test:absent", false)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, transitioned, "a refused call reports no transition")
}

// TestPruneChangeEventsRefusesCanceledAdmission proves the retention pass runs
// under the job's admitted lifetime: a canceled run deletes nothing.
func TestPruneChangeEventsRefusesCanceledAdmission(t *testing.T) {
	engine := newTestEngine(t)

	pruned, err := engine.PruneChangeEvents(canceledContext(), "test", time.Now().Add(time.Hour))
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, pruned)
}

// TestDeindexJobLifetimeOwnsItsMetadataWrites proves the cleanup job's own
// admitted lifetime reaches the metadata writes it owns: a canceled run leaves
// the work item pending and writes no tombstone, and the next accepted run
// completes exactly that item.
func TestDeindexJobLifetimeOwnsItsMetadataWrites(t *testing.T) {
	engine := newTestEngine(t)
	key := labelIndexKey("person", 7)
	require.NoError(t, engine.PutIndexEntryCatalog(context.Background(), "test:deindex", &IndexEntryCatalog{IndexKeys: [][]byte{key}}))
	require.NoError(t, engine.PutDeindexWorkItem(t.Context(), &DeindexWorkItem{WorkItemID: "test:deindex", TargetID: "test:deindex", Status: "pending"}))

	job := NewDeindexCleanupJob(engine, time.Hour)

	item, err := engine.GetDeindexWorkItem("test:deindex")
	require.NoError(t, err)
	require.NotNil(t, item)

	require.ErrorIs(t, job.processWorkItem(canceledContext(), item), context.Canceled)
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		if hasIndexTombstone(txn, key) {
			t.Error("a canceled job run writes no tombstone")
		}
		return nil
	}))
	pending, err := engine.GetDeindexWorkItem("test:deindex")
	require.NoError(t, err)
	require.NotNil(t, pending, "a canceled run retains the work item for the next run")

	deindexed, err := job.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, deindexed)
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		if !hasIndexTombstone(txn, key) {
			t.Error("the accepted run writes the tombstone")
		}
		return nil
	}))
	remaining, err := engine.GetDeindexWorkItem("test:deindex")
	require.NoError(t, err)
	require.Nil(t, remaining, "the accepted run retires the work item")
}

// TestSchemaDefinitionSnapshotRefusesCanceledCaller proves the detached schema
// snapshot, the constraint-contract read and the definition install are all
// admitted under the caller's lifetime, and that an accepted snapshot stays
// detached from later live changes.
func TestSchemaDefinitionSnapshotRefusesCanceledCaller(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("c_uid", "Rec", "uid"))

	definition, err := schema.ExportDefinition(canceledContext())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, definition, "a canceled caller receives no partial definition")

	contracts, err := schema.GetAllConstraintContracts(canceledContext())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, contracts, "a canceled caller receives no partial contract list")

	definition, err = schema.ExportDefinition(context.Background())
	require.NoError(t, err)
	require.NotNil(t, definition)
	accepted := len(definition.ConstraintContracts)

	contracts, err = schema.GetAllConstraintContracts(context.Background())
	require.NoError(t, err)
	require.Equal(t, accepted, len(contracts))

	require.NoError(t, schema.AddRangeIndex("later_range", "Rec", "score"))
	require.Len(t, definition.ConstraintContracts, accepted,
		"the accepted snapshot is detached from later live changes")
	fresh := schemaDefinitionForTest(t, schema)
	rangeNames := make([]string, 0, len(fresh.RangeIndexes))
	for _, index := range fresh.RangeIndexes {
		rangeNames = append(rangeNames, index.Name)
	}
	require.Contains(t, rangeNames, "later_range", "the next snapshot observes the later live change")
}
