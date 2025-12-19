package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDatabaseManagerAdapter wraps multidb.DatabaseManager to implement DatabaseManagerInterface
type testDatabaseManagerAdapter struct {
	manager *multidb.DatabaseManager
}

func (a *testDatabaseManagerAdapter) CreateDatabase(name string) error {
	return a.manager.CreateDatabase(name)
}

func (a *testDatabaseManagerAdapter) DropDatabase(name string) error {
	return a.manager.DropDatabase(name)
}

func (a *testDatabaseManagerAdapter) ListDatabases() []DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	result := make([]DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &testDatabaseInfoAdapter{info: db}
	}
	return result
}

func (a *testDatabaseManagerAdapter) Exists(name string) bool {
	return a.manager.Exists(name)
}

func (a *testDatabaseManagerAdapter) CreateAlias(alias, databaseName string) error {
	return a.manager.CreateAlias(alias, databaseName)
}

func (a *testDatabaseManagerAdapter) DropAlias(alias string) error {
	return a.manager.DropAlias(alias)
}

func (a *testDatabaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.manager.ListAliases(databaseName)
}

func (a *testDatabaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.manager.ResolveDatabase(nameOrAlias)
}

func (a *testDatabaseManagerAdapter) SetDatabaseLimits(databaseName string, limits interface{}) error {
	if limitsPtr, ok := limits.(*multidb.Limits); ok {
		return a.manager.SetDatabaseLimits(databaseName, limitsPtr)
	}
	return fmt.Errorf("invalid limits type")
}

func (a *testDatabaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(databaseName)
}

func (a *testDatabaseManagerAdapter) CreateCompositeDatabase(name string, constituents []interface{}) error {
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, c := range constituents {
		if m, ok := c.(map[string]interface{}); ok {
			refs[i] = multidb.ConstituentRef{
				Alias:        getStringFromMap(m, "alias"),
				DatabaseName: getStringFromMap(m, "database_name"),
				Type:         getStringFromMap(m, "type"),
				AccessMode:   getStringFromMap(m, "access_mode"),
			}
		} else {
			return fmt.Errorf("invalid constituent type at index %d", i)
		}
	}
	return a.manager.CreateCompositeDatabase(name, refs)
}

func (a *testDatabaseManagerAdapter) DropCompositeDatabase(name string) error {
	return a.manager.DropCompositeDatabase(name)
}

func (a *testDatabaseManagerAdapter) AddConstituent(compositeName string, constituent interface{}) error {
	var ref multidb.ConstituentRef
	if m, ok := constituent.(map[string]interface{}); ok {
		ref = multidb.ConstituentRef{
			Alias:        getStringFromMap(m, "alias"),
			DatabaseName: getStringFromMap(m, "database_name"),
			Type:         getStringFromMap(m, "type"),
			AccessMode:   getStringFromMap(m, "access_mode"),
		}
	} else {
		return fmt.Errorf("invalid constituent type")
	}
	return a.manager.AddConstituent(compositeName, ref)
}

func (a *testDatabaseManagerAdapter) RemoveConstituent(compositeName string, alias string) error {
	return a.manager.RemoveConstituent(compositeName, alias)
}

func (a *testDatabaseManagerAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	constituents, err := a.manager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(constituents))
	for i, c := range constituents {
		result[i] = map[string]interface{}{
			"alias":         c.Alias,
			"database_name": c.DatabaseName,
			"type":          c.Type,
			"access_mode":   c.AccessMode,
		}
	}
	return result, nil
}

func (a *testDatabaseManagerAdapter) ListCompositeDatabases() []DatabaseInfoInterface {
	dbs := a.manager.ListCompositeDatabases()
	result := make([]DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &testDatabaseInfoAdapter{info: db}
	}
	return result
}

func (a *testDatabaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}

type testDatabaseInfoAdapter struct {
	info *multidb.DatabaseInfo
}

func (a *testDatabaseInfoAdapter) Name() string {
	return a.info.Name
}

func (a *testDatabaseInfoAdapter) Type() string {
	return a.info.Type
}

func (a *testDatabaseInfoAdapter) Status() string {
	return a.info.Status
}

func (a *testDatabaseInfoAdapter) IsDefault() bool {
	return a.info.IsDefault
}

func (a *testDatabaseInfoAdapter) CreatedAt() time.Time {
	return a.info.CreatedAt
}

func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func TestExecuteAlterCompositeDatabase_AddAlias(t *testing.T) {
	store := storage.NewMemoryEngine()
	inner := storage.NewMemoryEngine()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Create constituent databases
	err := adapter.CreateDatabase("db1")
	require.NoError(t, err)
	err = adapter.CreateDatabase("db2")
	require.NoError(t, err)
	err = adapter.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database with 2 constituents
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         "db1",
			"database_name": "db1",
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         "db2",
			"database_name": "db2",
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Add constituent using ALTER COMPOSITE DATABASE
	query := `ALTER COMPOSITE DATABASE composite1
		ADD ALIAS db3 FOR DATABASE db3`
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.Rows))

	// Verify constituent was added
	constituentsList, err := adapter.GetCompositeConstituents("composite1")
	require.NoError(t, err)
	assert.Equal(t, 3, len(constituentsList))
}

func TestExecuteAlterCompositeDatabase_DropAlias(t *testing.T) {
	store := storage.NewMemoryEngine()
	inner := storage.NewMemoryEngine()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database with 2 constituents
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         "db1",
			"database_name": "db1",
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         "db2",
			"database_name": "db2",
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Drop constituent using ALTER COMPOSITE DATABASE
	query := `ALTER COMPOSITE DATABASE composite1
		DROP ALIAS db2`
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.Rows))

	// Verify constituent was removed
	constituentsList, err := adapter.GetCompositeConstituents("composite1")
	require.NoError(t, err)
	assert.Equal(t, 1, len(constituentsList))
	assert.Equal(t, "db1", constituentsList[0].(map[string]interface{})["alias"])
}

func TestExecuteAlterCompositeDatabase_InvalidSyntax(t *testing.T) {
	store := storage.NewMemoryEngine()
	inner := storage.NewMemoryEngine()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Test invalid syntax
	query := `ALTER COMPOSITE DATABASE`
	_, err := exec.Execute(ctx, query, nil)
	assert.Error(t, err)
}
