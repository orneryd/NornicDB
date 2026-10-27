package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// txLogEnrichment reaches storage as the database's change capture mode:
// when it is set, when the manager loads, and off when the database is
// dropped (#937).
func TestTxLogEnrichmentSetsChangeCapture(t *testing.T) {
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	manager, err := NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "cdc"}))
	require.NoError(t, manager.SetDatabaseOption(t.Context(), "cdc", OptionTxLogEnrichment, "FULL"))
	require.Equal(t, storage.ChangeCaptureFull, engine.ChangeCapture("cdc"))
	require.NoError(t, manager.SetDatabaseOption(t.Context(), "cdc", OptionTxLogEnrichment, ""))
	require.Equal(t, storage.ChangeCaptureOff, engine.ChangeCapture("cdc"))
	require.NoError(t, manager.SetDatabaseOption(t.Context(), "cdc", OptionTxLogEnrichment, "DIFF"))

	engine.SetChangeCapture("cdc", storage.ChangeCaptureOff)
	reloaded, err := NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	require.Equal(t, storage.ChangeCaptureDiff, engine.ChangeCapture("cdc"))
	require.Error(t, reloaded.SetDatabaseOption(t.Context(), "missing", OptionTxLogEnrichment, "FULL"))
	_, err = reloaded.DatabaseOptions("missing")
	require.Error(t, err)

	require.NoError(t, reloaded.DropDatabase(t.Context(), "cdc"))
	require.Equal(t, storage.ChangeCaptureOff, engine.ChangeCapture("cdc"))
}

// Options of a database that doesn't exist: ErrDatabaseNotFound.
func TestDatabaseOptionsOfMissingDatabase(t *testing.T) {
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	manager, err := NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	_, err = manager.DatabaseOptions("missing")
	require.ErrorIs(t, err, ErrDatabaseNotFound)
	require.ErrorIs(t, manager.SetDatabaseOption(t.Context(), "missing", OptionTxLogEnrichment, "FULL"), ErrDatabaseNotFound)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "opts"}))
	require.NoError(t, manager.SetDatabaseOption(t.Context(), "opts", OptionTxLogEnrichment, "DIFF"))
	options, err := manager.DatabaseOptions("opts")
	require.NoError(t, err)
	require.Equal(t, map[string]string{OptionTxLogEnrichment: "DIFF"}, options)
}
