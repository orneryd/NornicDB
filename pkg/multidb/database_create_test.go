package multidb

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDatabaseCreateInitialOptions(t *testing.T) {
	inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
	require.NoError(t, err)
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	options := map[string]string{OptionTxLogEnrichment: " full "}
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "initial", Options: options}))
	options[OptionTxLogEnrichment] = "OFF"
	actual, err := manager.DatabaseOptions("initial")
	require.NoError(t, err)
	require.Equal(t, map[string]string{OptionTxLogEnrichment: "FULL"}, actual)
	require.Equal(t, storage.ChangeCaptureFull, inner.ChangeCapture("initial"))
	for _, invalid := range []map[string]string{{"unknown": "FULL"}, {OptionTxLogEnrichment: "invalid"}, {OptionTxLogEnrichment: ""}} {
		require.ErrorIs(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "invalid", Options: invalid}), storage.ErrInvalidData)
		require.False(t, manager.Exists("invalid"))
	}
	restored, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	require.False(t, restored.Exists("invalid"))
	actual, err = restored.DatabaseOptions("initial")
	require.NoError(t, err)
	require.Equal(t, map[string]string{OptionTxLogEnrichment: "FULL"}, actual)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "without_options"}))
	actual, err = manager.DatabaseOptions("without_options")
	require.NoError(t, err)
	require.Empty(t, actual)
	require.Equal(t, storage.ChangeCaptureOff, inner.ChangeCapture("without_options"))
}
