package multidb

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func requireMultidbOperationLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestMultidbCompositeAndManagerErrorsHaveTypedIdentity(t *testing.T) {
	manager, dbName := setupTestManager(t)

	err := manager.CreateCompositeDatabase("composite", []ConstituentRef{{
		Alias:        "missing",
		DatabaseName: "missing",
		Type:         "local",
		AccessMode:   "read",
	}})
	localizedErr := requireMultidbOperationLocalizedError(t, err, "multidb.composite.constituent_database_not_found", "constituent database 'missing' not found: database not found")
	require.ErrorIs(t, err, ErrDatabaseNotFound)
	require.Equal(t, "missing", localizedErr.Message.Data["Database"])

	err = manager.DropCompositeDatabase(dbName)
	localizedErr = requireMultidbOperationLocalizedError(t, err, "multidb.composite.database_not_composite", "database 'testdb' is not a composite database")
	require.Equal(t, dbName, localizedErr.Message.Data["Database"])

	err = manager.SetDatabaseStatus(dbName, "paused")
	localizedErr = requireMultidbOperationLocalizedError(t, err, "multidb.manager.invalid_status", "invalid status: paused (must be 'online' or 'offline')")
	require.Equal(t, "paused", localizedErr.Message.Data["Status"])

	err = manager.validateAliasName("bad alias")
	localizedErr = requireMultidbOperationLocalizedError(t, err, "multidb.manager.alias_contains_whitespace", "invalid alias name: 'bad alias' (cannot contain whitespace)")
	require.ErrorIs(t, err, ErrInvalidAliasName)
	require.Equal(t, "bad alias", localizedErr.Message.Data["Alias"])
}

func TestMultidbStorageErrorsPreserveCauses(t *testing.T) {
	manager, _ := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })

	nodesErr := errors.New("nodes unavailable")
	_, _, err := manager.calculateStorageSizeFromEngine(&storageSizingErrorEngine{Engine: base, nodesErr: nodesErr})
	localizedErr := requireMultidbOperationLocalizedError(t, err, "multidb.storage.get_all_nodes_for_size_calculation_failed", "failed to get all nodes for size calculation: nodes unavailable")
	require.ErrorIs(t, err, nodesErr)
	require.Equal(t, nodesErr.Error(), localizedErr.Message.Data["Cause"])

	badNode := &storage.Node{ID: "bad-node", Properties: map[string]any{"bad": make(chan int)}}
	_, err = calculateNodeSize(badNode)
	localizedErr = requireMultidbOperationLocalizedError(t, err, "multidb.storage.encode_node_failed", "failed to encode node: gob: type not registered for interface: chan int")
	require.Contains(t, localizedErr.Message.Data["Cause"], "chan int")
}
