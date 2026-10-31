package catalogadapter_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixture is one real multidb.DatabaseManager over a tracked in-memory base
// engine. Every boundary assertion below runs against the native manager, not
// a fake, so the manager's own error and context precedence is what the
// assertions observe.
type fixture struct {
	manager *multidb.DatabaseManager
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	fixtureowner.Own(t, manager)
	return fixture{manager: manager}
}

func (f fixture) catalog() *catalogadapter.Catalog {
	return &catalogadapter.Catalog{Manager: f.manager}
}

func (f fixture) create(t *testing.T, name string) {
	t.Helper()
	require.NoError(t, f.manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
}

// TestCatalogSetDatabaseLimitsAdmissionPrecedesManager asserts the canonical
// admission order: an input that is not a *multidb.Limits is rejected with the
// exact "invalid limits type" error BEFORE any manager lookup, database
// resolution or context cancellation handling runs.
func TestCatalogSetDatabaseLimitsAdmissionPrecedesManager(t *testing.T) {
	f := newFixture(t)
	f.create(t, "tenant")

	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	badInputs := map[string]interface{}{
		"untyped nil":  nil,
		"limits value": multidb.Limits{},
		"empty map":    map[string]interface{}{},
		"nil map":      map[string]interface{}(nil),
		"string":       "max_nodes=1",
	}
	targets := map[string]struct {
		name string
		ctx  context.Context
	}{
		"existing database, live context": {"tenant", t.Context()},
		"missing database, live context":  {"absent", t.Context()},
		"existing database, canceled":     {"tenant", canceled},
		"missing database, canceled":      {"absent", canceled},
	}
	for inputName, input := range badInputs {
		for targetName, target := range targets {
			t.Run(inputName+"/"+targetName, func(t *testing.T) {
				err := f.catalog().SetDatabaseLimits(target.ctx, target.name, input)
				require.Error(t, err)
				// Exact text, and neither native error: the assertion ran first.
				assert.EqualError(t, err, "invalid limits type")
				assert.NotErrorIs(t, err, multidb.ErrDatabaseNotFound)
				assert.NotErrorIs(t, err, context.Canceled)
			})
		}
	}

	// The rejected calls published nothing.
	limits, err := f.manager.GetDatabaseLimits("tenant")
	require.NoError(t, err)
	assert.Nil(t, limits)
}

// TestCatalogSetDatabaseLimitsAcceptedPointerReachesManager asserts that valid
// and typed-nil pointers pass admission and keep the native manager's own
// behavior: the typed nil clears the limits, a missing database is the
// manager's ErrDatabaseNotFound, and a canceled operation is context.Canceled.
func TestCatalogSetDatabaseLimitsAcceptedPointerReachesManager(t *testing.T) {
	f := newFixture(t)
	f.create(t, "tenant")
	adapter := f.catalog()

	t.Run("valid pointer persists", func(t *testing.T) {
		require.NoError(t, adapter.SetDatabaseLimits(t.Context(), "tenant", &multidb.Limits{
			Storage: multidb.StorageLimits{MaxNodes: 42},
		}))
		boxed, err := adapter.GetDatabaseLimits("tenant")
		require.NoError(t, err)
		limits, ok := boxed.(*multidb.Limits)
		require.True(t, ok, "GetDatabaseLimits must return the manager's boxed *multidb.Limits")
		assert.Equal(t, int64(42), limits.Storage.MaxNodes)
	})

	t.Run("typed nil is accepted and clears", func(t *testing.T) {
		typedNil := (*multidb.Limits)(nil)
		require.NoError(t, adapter.SetDatabaseLimits(t.Context(), "tenant", typedNil))
		boxed, err := adapter.GetDatabaseLimits("tenant")
		require.NoError(t, err)
		assert.Nil(t, boxed, "the native manager returns an untyped nil for cleared limits")
	})

	t.Run("missing database is the native error", func(t *testing.T) {
		err := adapter.SetDatabaseLimits(t.Context(), "absent", &multidb.Limits{})
		assert.ErrorIs(t, err, multidb.ErrDatabaseNotFound)
	})

	t.Run("canceled operation is the native context error", func(t *testing.T) {
		require.NoError(t, adapter.SetDatabaseLimits(t.Context(), "tenant", &multidb.Limits{
			Storage: multidb.StorageLimits{MaxNodes: 7},
		}))
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		err := adapter.SetDatabaseLimits(canceled, "tenant", &multidb.Limits{
			Storage: multidb.StorageLimits{MaxNodes: 99},
		})
		assert.ErrorIs(t, err, context.Canceled)
		boxed, getErr := adapter.GetDatabaseLimits("tenant")
		require.NoError(t, getErr)
		limits, ok := boxed.(*multidb.Limits)
		require.True(t, ok)
		assert.Equal(t, int64(7), limits.Storage.MaxNodes, "a canceled transition must not publish limits")
	})
}

// TestCatalogZeroValueAndNilDirectReceiverBadTypeBehavior asserts the decided
// receiver boundary for the limits admission itself: a nonnil adapter whose
// embedded Manager is nil still rejects a bad input type before touching the
// manager, and a direct call on a nil *Catalog keeps that same rejection
// because the receiver is never dereferenced before the type assertion.
func TestCatalogZeroValueAndNilDirectReceiverBadTypeBehavior(t *testing.T) {
	t.Run("zero-value nonnil catalog with nil manager", func(t *testing.T) {
		adapter := &catalogadapter.Catalog{}
		err := adapter.SetDatabaseLimits(t.Context(), "tenant", map[string]interface{}{})
		assert.EqualError(t, err, "invalid limits type")
	})

	t.Run("nil *Catalog direct call", func(t *testing.T) {
		var adapter *catalogadapter.Catalog
		err := adapter.SetDatabaseLimits(t.Context(), "tenant", uint8(1))
		assert.EqualError(t, err, "invalid limits type")
	})
}

// TestCatalogRequiredOperationsAgainstNativeManager exercises every canonical
// operation once against the real manager so the shared owner is proven at
// the native boundary, not only compiled.
func TestCatalogRequiredOperationsAgainstNativeManager(t *testing.T) {
	f := newFixture(t)
	adapter := f.catalog()

	require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db_a"}))
	assert.True(t, adapter.Exists("db_a"))
	require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db_b"}))

	require.NoError(t, adapter.CreateAlias(t.Context(), "alias_a", "db_a"))
	assert.Equal(t, map[string]string{"alias_a": "db_a"}, adapter.ListAliases("db_a"))
	resolved, err := adapter.ResolveDatabase("alias_a")
	require.NoError(t, err)
	assert.Equal(t, "db_a", resolved)
	require.NoError(t, adapter.DropAlias(t.Context(), "alias_a"))
	assert.Empty(t, adapter.ListAliases("db_a"))

	engine, err := adapter.GetStorageForUse("db_a", "")
	require.NoError(t, err)
	assert.NotNil(t, engine)

	require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "comp", []interface{}{
		multidb.ConstituentRef{DatabaseName: "db_a", Alias: "a", Type: "local", AccessMode: "read_write"},
		multidb.ConstituentRef{DatabaseName: "db_b", Alias: "b", Type: "local", AccessMode: "read_write"},
	}))
	assert.True(t, adapter.IsCompositeDatabase("comp"))
	require.NoError(t, adapter.RemoveConstituent(t.Context(), "comp", "b"))
	refs, err := adapter.GetCompositeConstituents("comp")
	require.NoError(t, err)
	require.Len(t, refs, 1)

	require.NoError(t, adapter.DropCompositeDatabase(t.Context(), "comp"))
	assert.False(t, adapter.IsCompositeDatabase("comp"))
	require.NoError(t, adapter.DropDatabase(t.Context(), "db_b"))
	assert.False(t, adapter.Exists("db_b"))

	assert.ErrorIs(t, adapter.DropDatabase(t.Context(), "db_b"), multidb.ErrDatabaseNotFound)
}

// TestCatalogMethodSetPreservesPointerReceiverContract asserts that the
// required operations use pointer receivers: *Catalog implements the required
// contract and the Catalog value does not, matching the adapter method sets
// this wave preserves.
func TestCatalogMethodSetPreservesPointerReceiverContract(t *testing.T) {
	var pointer interface{} = &catalogadapter.Catalog{}
	_, pointerOK := pointer.(cypher.DatabaseManagerInterface)
	assert.True(t, pointerOK, "*Catalog must implement the required manager contract")

	var value interface{} = catalogadapter.Catalog{}
	_, valueOK := value.(cypher.DatabaseManagerInterface)
	assert.False(t, valueOK, "Catalog value must not implement the required manager contract")
}
