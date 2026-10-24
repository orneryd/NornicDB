package server

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The server adapter forwards database options to the manager (#937).
func TestServerAdapterForwardsDatabaseOptions(t *testing.T) {
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	manager, err := multidb.NewDatabaseManager(engine, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("opts"))
	adapter := &databaseManagerAdapter{manager: manager}
	require.NoError(t, adapter.SetDatabaseOption("opts", multidb.OptionTxLogEnrichment, "FULL"))
	options, err := adapter.DatabaseOptions("opts")
	require.NoError(t, err)
	require.Equal(t, map[string]string{multidb.OptionTxLogEnrichment: "FULL"}, options)
}
