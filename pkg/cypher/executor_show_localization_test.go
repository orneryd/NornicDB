package cypher

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func requireCypherAdminLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCypherAdminManagerUnavailableErrorsHaveTypedIdentity(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	testCases := []struct {
		name    string
		command string
		run     func() error
	}{
		{name: "show databases", command: "SHOW DATABASES", run: func() error { _, err := exec.executeShowDatabases(ctx, "SHOW DATABASES"); return err }},
		{name: "create database", command: "CREATE DATABASE", run: func() error {
			_, err := exec.executeDatabaseCreate(ctx, "CREATE DATABASE tenant", createNewDatabase)
			return err
		}},
		{name: "drop database", command: "DROP DATABASE", run: func() error {
			_, err := exec.executeDatabaseDrop(ctx, "DROP DATABASE tenant", dropStandardDatabase)
			return err
		}},
		{name: "create alias", command: "CREATE ALIAS", run: func() error {
			_, err := exec.executeCreateAlias(ctx, "CREATE ALIAS alias FOR DATABASE tenant")
			return err
		}},
		{name: "drop alias", command: "DROP ALIAS", run: func() error { _, err := exec.executeDropAlias(ctx, "DROP ALIAS alias"); return err }},
		{name: "alter database", command: "ALTER DATABASE", run: func() error {
			_, err := exec.executeAlterDatabase(ctx, "ALTER DATABASE tenant SET LIMIT max_nodes = 1")
			return err
		}},
		{name: "show limits", command: "SHOW LIMITS", run: func() error { _, err := exec.executeShowLimits(ctx, "SHOW LIMITS FOR DATABASE tenant"); return err }},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			text := "database manager not available - " + testCase.command + " requires multi-database support"
			localizedErr := requireCypherAdminLocalizedError(t, testCase.run(), localization.MessageCypherAdminDatabaseManagerUnavailable, text)
			require.Equal(t, testCase.command, localizedErr.Message.Data["Command"])
		})
	}
}

// newCypherAdminErrorManager configures the canonical command fixture before
// execution, retaining the former localization fixture's exact resolution and
// per-operation cause injection without another manager implementation.
func newCypherAdminErrorManager(t testing.TB, operation testCatalogOperation, cause error) *mockDatabaseManager {
	t.Helper()
	manager := newMockDatabaseManager(t)
	manager.resolutionOverride = "tenant"
	if cause != nil {
		manager.catalogFailures[operation] = cause
	}
	return manager
}

func TestCypherAdminLocalizedErrorsPreserveCausesAndMachineValues(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	cause := errors.New("forced admin failure")

	t.Run("create database", func(t *testing.T) {
		manager := newCypherAdminErrorManager(t, testCatalogCreateDatabase, cause)
		exec.SetDatabaseManager(manager)
		_, err := exec.executeDatabaseCreate(ctx, "CREATE DATABASE tenant_create", createNewDatabase)
		localizedErr := requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminCreateDatabaseFailed, "failed to create database 'tenant_create': forced admin failure")
		require.ErrorIs(t, err, cause)
		require.Equal(t, "tenant_create", localizedErr.Message.Data["Database"])
	})

	t.Run("drop database", func(t *testing.T) {
		manager := newCypherAdminErrorManager(t, testCatalogDropDatabase, cause)
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant_drop"}))
		exec.SetDatabaseManager(manager)
		_, err := exec.executeDatabaseDrop(ctx, "DROP DATABASE tenant_drop", dropStandardDatabase)
		requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminDropDatabaseFailed, "failed to drop database 'tenant_drop': forced admin failure")
		require.ErrorIs(t, err, cause)
	})

	t.Run("create alias", func(t *testing.T) {
		manager := newCypherAdminErrorManager(t, testCatalogCreateAlias, cause)
		exec.SetDatabaseManager(manager)
		_, err := exec.executeCreateAlias(ctx, "CREATE ALIAS tenant_alias FOR DATABASE tenant_target")
		localizedErr := requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminCreateAliasFailed, "failed to create alias 'tenant_alias' for database 'tenant_target': forced admin failure")
		require.ErrorIs(t, err, cause)
		require.Equal(t, "tenant_alias", localizedErr.Message.Data["Alias"])
		require.Equal(t, "tenant_target", localizedErr.Message.Data["Database"])
	})

	t.Run("drop alias", func(t *testing.T) {
		manager := newCypherAdminErrorManager(t, testCatalogDropAlias, cause)
		exec.SetDatabaseManager(manager)
		_, err := exec.executeDropAlias(ctx, "DROP ALIAS tenant_alias")
		requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminDropAliasFailed, "failed to drop alias 'tenant_alias': forced admin failure")
		require.ErrorIs(t, err, cause)
	})

	t.Run("database limit lookup", func(t *testing.T) {
		manager := newCypherAdminErrorManager(t, testCatalogGetLimits, cause)
		exec.SetDatabaseManager(manager)
		_, err := exec.executeShowLimits(ctx, "SHOW LIMITS FOR DATABASE tenant_limits")
		localizedErr := requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminDatabaseNotFound, "database 'tenant_limits' not found: forced admin failure")
		require.ErrorIs(t, err, cause)
		require.Equal(t, "tenant_limits", localizedErr.Message.Data["Database"])
	})

	t.Run("database limit update", func(t *testing.T) {
		manager := newCypherAdminErrorManager(t, testCatalogSetLimits, cause)
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant_limits"}))
		exec.SetDatabaseManager(manager)
		_, err := exec.executeAlterDatabase(ctx, "ALTER DATABASE tenant_limits SET LIMIT max_nodes = 42")
		localizedErr := requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminSetDatabaseLimitsFailed, "failed to set limits for database 'tenant_limits': forced admin failure")
		require.ErrorIs(t, err, cause)
		require.Equal(t, "tenant_limits", localizedErr.Message.Data["Database"])
	})

	t.Run("invalid limit value", func(t *testing.T) {
		manager := newCypherAdminErrorManager(t, testCatalogFailureCount, nil)
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant_limits"}))
		manager.limits["tenant_limits"] = &multidb.Limits{}
		exec.SetDatabaseManager(manager)
		_, err := exec.executeAlterDatabase(ctx, "ALTER DATABASE tenant_limits SET LIMIT max_nodes = nope")
		localizedErr := requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminInvalidLimitValue, "invalid max_nodes value: strconv.ParseInt: parsing \"nope\": invalid syntax")
		require.ErrorIs(t, err, strconv.ErrSyntax)
		require.Equal(t, "max_nodes", localizedErr.Message.Data["Limit"])
	})
}
