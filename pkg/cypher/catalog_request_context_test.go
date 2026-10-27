package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type catalogRequestContextKey struct{}
type catalogRequestCall struct {
	operation string
	ctx       context.Context
}

type catalogRequestContextAdapter struct {
	optionsTestManager
	calls           []catalogRequestCall
	beforeComposite context.CancelFunc
	beforeCreate    func(context.Context)
}

func (a *catalogRequestContextAdapter) CreateDatabase(ctx context.Context, request multidb.DatabaseCreateRequest) error {
	a.calls = append(a.calls, catalogRequestCall{"create", ctx})
	if a.beforeCreate != nil {
		a.beforeCreate(ctx)
	}
	return a.manager.CreateDatabase(ctx, request)
}

func (a *catalogRequestContextAdapter) SetDatabaseOption(ctx context.Context, name, key, value string) error {
	a.calls = append(a.calls, catalogRequestCall{"option", ctx})
	return a.manager.SetDatabaseOption(ctx, name, key, value)
}

func (a *catalogRequestContextAdapter) CreateCompositeDatabase(ctx context.Context, name string, constituents []interface{}) error {
	a.calls = append(a.calls, catalogRequestCall{"composite", ctx})
	if a.beforeComposite != nil {
		a.beforeComposite()
	}
	return a.testDatabaseManagerAdapter.CreateCompositeDatabase(ctx, name, constituents)
}

func TestCatalogCommandsPreserveRequestContext(t *testing.T) {
	inner := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	adapter := &catalogRequestContextAdapter{optionsTestManager: optionsTestManager{&testDatabaseManagerAdapter{manager: manager}}}
	executor := NewStorageExecutor(storage.NewNamespacedEngine(inner, "nornic"))
	executor.SetDatabaseManager(adapter)
	marker := &struct{ request string }{"catalog request"}
	ctx := context.WithValue(t.Context(), catalogRequestContextKey{}, marker)

	_, err = executor.Execute(ctx, "CREATE DATABASE context_options OPTIONS {txLogEnrichment: 'FULL'}", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "CREATE COMPOSITE DATABASE context_composite ALIAS local FOR DATABASE nornic TYPE local ACCESS read", nil)
	require.NoError(t, err)
	require.Len(t, adapter.calls, 2)
	for i, operation := range []string{"create", "composite"} {
		require.Equal(t, operation, adapter.calls[i].operation)
		require.Same(t, marker, adapter.calls[i].ctx.Value(catalogRequestContextKey{}), "request ancestry must survive preparation/options helpers")
	}
	options, err := manager.DatabaseOptions("context_options")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"txLogEnrichment": "FULL"}, options)
	require.True(t, manager.IsCompositeDatabase("context_composite"))

	// Cancel at the prepared-command mutation boundary, after query admission.
	// A background/lifetime substitution would otherwise publish this database.
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	adapter.beforeComposite = cancel
	_, err = executor.Execute(canceled, "CREATE COMPOSITE DATABASE context_canceled ALIAS local FOR DATABASE nornic TYPE local ACCESS read", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, manager.Exists("context_canceled"))
	require.Len(t, adapter.calls, 3)
	require.Same(t, marker, adapter.calls[2].ctx.Value(catalogRequestContextKey{}))
	require.ErrorIs(t, adapter.calls[2].ctx.Err(), context.Canceled)
}
