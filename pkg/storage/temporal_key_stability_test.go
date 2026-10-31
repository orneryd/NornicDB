package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Temporal history keys are persisted, so their key hash must not depend on
// the process: NewCompositeKey's HMAC key is random per process, and a clean
// restart (which skips the rebuild) couldn't find any entry written before it.
// A golden value pins the hash; a per-process key would change it every run.
func TestTemporalKeyHash_IsStableAcrossProcesses(t *testing.T) {
	single, ok := splitTemporalKeySpec([]string{"k", "vf", "vt"})
	require.True(t, ok)
	composite, ok := splitTemporalKeySpec([]string{"record_id", "attribute", "valid_from", "valid_to"})
	require.True(t, ok)
	require.Equal(t, goldenSingle, single.keyHash("x"))
	require.Equal(t, goldenComposite, composite.keyHash([]interface{}{int64(7), "name"}))
	require.Equal(t, single.keyHash(int64(1)), single.keyHash(float64(1)), "numerically equal keys share a history")
}

// A store whose temporal index predates the stable key format (no format
// marker) is rebuilt on a clean start before the index is trusted.
func TestTemporalIndexFormat_CleanStartRebuildsOldFormat(t *testing.T) {
	engine, dir := createTestBadgerEngineOnDisk(t)
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factWindow))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{factWindow}))
	commitNodes(t, engine, temporalFact("a", int64(1), 0, 10))
	// Simulate an old store: no format marker, an entry under a foreign hash.
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		if err := txn.Delete(temporalIndexFormatKey); err != nil {
			return err
		}
		return nil
	}))
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixTemporalIndex))
	require.NoError(t, engine.MarkCleanShutdown(context.Background()))
	require.NoError(t, engine.Close())

	reopened, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	defer reopened.Close()
	clean, err := reopened.ConsumeCleanShutdownMarker(context.Background())
	require.NoError(t, err)
	require.True(t, clean)
	current, err := reopened.temporalIndexFormatIsCurrent()
	require.NoError(t, err)
	require.True(t, current, "the clean start rebuilt the old-format index")
	require.True(t, reopened.temporalIndexReady.Load())

	tx, err := reopened.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(temporalFact("b", int64(1), 5, 15))
	require.Error(t, err, "the rebuilt index lists the stored period")
	require.NoError(t, tx.Rollback())
}

const (
	goldenSingle    = "04ee7de12e412be0b97c4e157eee9f1ba05053a1341bdc7dbb028f45484fc968"
	goldenComposite = "e3c410237d53c78a40d468600ccc0f979db262768e2542236ef734a3beea5ced"
)
