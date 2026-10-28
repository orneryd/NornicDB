package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoltAdapterRequiredContractAndCapabilityMatrix(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	adapter := &boltDatabaseManagerAdapter{IdentifiedExecutionCatalog: catalogadapter.ForIdentifiedExecution(manager)}

	t.Run("required contract is preserved", func(t *testing.T) {
		var anyAdapter any = adapter
		_, ok := anyAdapter.(cypher.DatabaseManagerInterface)
		require.True(t, ok, "*boltDatabaseManagerAdapter implements cypher.DatabaseManagerInterface")
	})

	t.Run("value does not implement the required contract", func(t *testing.T) {
		var anyValue any = *adapter
		_, ok := anyValue.(cypher.DatabaseManagerInterface)
		assert.False(t, ok, "the adapter value type must not implement the required contract")
	})

	t.Run("optional capability matrix", func(t *testing.T) {
		var anyAdapter any = adapter
		_, optionsOK := anyAdapter.(cypher.DatabaseOptionsManager)
		_, scopeOK := anyAdapter.(interface {
			StatementCacheScope(string) (*multidb.StatementCacheScope, error)
		})
		_, identityOK := anyAdapter.(cypher.DatabaseIdentityProvider)
		_, systemOK := anyAdapter.(interface{ SystemDatabaseName() string })
		assert.True(t, optionsOK, "Bolt keeps DatabaseOptions/SetDatabaseOption")
		assert.True(t, scopeOK, "Bolt keeps StatementCacheScope")
		assert.True(t, identityOK, "Bolt keeps ServerIdentity/DatabaseIdentity")
		assert.True(t, systemOK, "Bolt keeps SystemDatabaseName")
	})

	t.Run("promoted limits admission precedes the manager", func(t *testing.T) {
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "bolt_limits"}))
		require.EqualError(t, adapter.SetDatabaseLimits(t.Context(), "bolt_limits", map[string]interface{}{}), "invalid limits type")
		require.NoError(t, adapter.SetDatabaseLimits(t.Context(), "bolt_limits", &multidb.Limits{
			Storage: multidb.StorageLimits{MaxNodes: 11},
		}))
		boxed, err := adapter.GetDatabaseLimits("bolt_limits")
		require.NoError(t, err)
		limits, ok := boxed.(*multidb.Limits)
		require.True(t, ok)
		assert.Equal(t, int64(11), limits.Storage.MaxNodes)
	})

	t.Run("nil private outer adapter receiver panics on promotion", func(t *testing.T) {
		var nilOuter *boltDatabaseManagerAdapter
		require.Panics(t, func() {
			_ = nilOuter.SetDatabaseLimits(t.Context(), "bolt_limits", map[string]interface{}{})
		})
		zero := &boltDatabaseManagerAdapter{}
		require.EqualError(t, zero.SetDatabaseLimits(t.Context(), "bolt_limits", uint8(0)), "invalid limits type")
	})
}
