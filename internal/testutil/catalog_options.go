package testutil

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// RequireCatalogOptionContext exercises an actual adapter against native catalog
// storage: a normal option persists, while canceled admission preserves it.
// For example, an adapter package supplies its private constructor as factory.
func RequireCatalogOptionContext(t *testing.T, factory func(*multidb.DatabaseManager) cypher.DatabaseOptionsManager) {
	t.Helper()
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	fixtureowner.Own(t, manager)
	const database = "catalog_context_options"
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: database}))
	adapter := factory(manager)
	creator, ok := adapter.(cypher.DatabaseManagerInterface)
	require.True(t, ok, "catalog adapter must preserve the typed creation contract")
	require.NoError(t, creator.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "adapter_initial", Options: map[string]string{multidb.OptionTxLogEnrichment: "DIFF"}}))
	initial, err := manager.DatabaseOptions("adapter_initial")
	require.NoError(t, err)
	require.Equal(t, map[string]string{multidb.OptionTxLogEnrichment: "DIFF"}, initial, "adapter must forward initial options with the name")
	require.NoError(t, adapter.SetDatabaseOption(t.Context(), database, multidb.OptionTxLogEnrichment, "FULL"))
	options, err := adapter.DatabaseOptions(database)
	require.NoError(t, err)
	require.Equal(t, map[string]string{multidb.OptionTxLogEnrichment: "FULL"}, options)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, creator.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "adapter_canceled", Options: map[string]string{multidb.OptionTxLogEnrichment: "FULL"}}), context.Canceled)
	require.False(t, manager.Exists("adapter_canceled"))
	require.ErrorIs(t, adapter.SetDatabaseOption(ctx, database, multidb.OptionTxLogEnrichment, "DIFF"), context.Canceled)
	options, err = adapter.DatabaseOptions(database)
	require.NoError(t, err)
	require.Equal(t, map[string]string{multidb.OptionTxLogEnrichment: "FULL"}, options, "canceled request must not publish an option")
}
