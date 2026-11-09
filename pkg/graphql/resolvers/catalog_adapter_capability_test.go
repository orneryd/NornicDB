package resolvers

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphQLAdapterRequiredContractAndCapabilityMatrix(t *testing.T) {
	manager := testutil.DatabaseManager(t, testDB(t))
	adapter := &graphqlDatabaseManagerAdapter{ExecutionCatalog: catalogadapter.ForExecution(manager)}

	t.Run("required contract is preserved", func(t *testing.T) {
		var anyAdapter any = adapter
		_, ok := anyAdapter.(cypher.DatabaseManagerInterface)
		require.True(t, ok, "*graphqlDatabaseManagerAdapter implements cypher.DatabaseManagerInterface")
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
		assert.True(t, optionsOK, "GraphQL keeps DatabaseOptions/SetDatabaseOption")
		assert.True(t, scopeOK, "GraphQL keeps StatementCacheScope")
		assert.False(t, identityOK, "GraphQL must not gain identity/system capabilities")
	})
}
