package cypher

// Author: credentials lane (Codex). Public administration identity regression;
// grammar selection must preserve the existing procedure mode contract.
import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeAdministrationProcedureModes(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "system"))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, c := range []struct {
		name, declaration string
		reject            bool
	}{
		{"custom.r73ReadComposite", "MODE READ AS CREATE COMPOSITE DATABASE procedure_catalog_write ALIAS r FOR DATABASE nornic", true},
		{"custom.r73ReadDatabase", "MODE READ AS CREATE DATABASE procedure_database_write", true},
		{"custom.r73ReadControl", "MODE READ AS RETURN 1 AS value", false},
		{"custom.r73WriteComposite", "MODE WRITE AS CREATE COMPOSITE DATABASE procedure_catalog_write ALIAS r FOR DATABASE nornic", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			query := "CREATE PROCEDURE " + c.name + "() " + c.declaration
			_, err := executor.Execute(t.Context(), query, nil)
			t.Logf("PROCEDURE_ADMISSION name=%s rejected=%t expected_rejection=%t", c.name, err != nil, c.reject)
			if (err != nil) != c.reject {
				t.Errorf("wrong public procedure admission: error=%v", err)
			}
			if err == nil {
				_, dropErr := executor.Execute(t.Context(), "DROP PROCEDURE "+c.name, nil)
				require.NoError(t, dropErr)
			}
			require.False(t, manager.Exists("procedure_catalog_write"))
			require.False(t, manager.Exists("procedure_database_write"))
		})
	}
}
