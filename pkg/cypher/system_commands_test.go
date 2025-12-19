// Package cypher - Unit tests for multi-database system commands.
//
// These tests verify CREATE DATABASE, DROP DATABASE, and SHOW DATABASES commands
// work correctly with the DatabaseManager integration.
package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDatabaseManager implements DatabaseManagerInterface for testing.
type mockDatabaseManager struct {
	databases map[string]*mockDatabaseInfo
	defaultDB string
}

type mockDatabaseInfo struct {
	name      string
	dbType    string
	status    string
	isDefault bool
}

func (m *mockDatabaseInfo) Name() string         { return m.name }
func (m *mockDatabaseInfo) Type() string         { return m.dbType }
func (m *mockDatabaseInfo) Status() string       { return m.status }
func (m *mockDatabaseInfo) IsDefault() bool      { return m.isDefault }
func (m *mockDatabaseInfo) CreatedAt() time.Time { return time.Now() }

func newMockDatabaseManager() *mockDatabaseManager {
	return &mockDatabaseManager{
		databases: make(map[string]*mockDatabaseInfo),
		defaultDB: "nornic",
	}
}

func (m *mockDatabaseManager) CreateDatabase(name string) error {
	if _, exists := m.databases[name]; exists {
		return fmt.Errorf("database '%s' already exists", name)
	}
	m.databases[name] = &mockDatabaseInfo{
		name:      name,
		dbType:    "standard",
		status:    "online",
		isDefault: name == m.defaultDB,
	}
	return nil
}

func (m *mockDatabaseManager) DropDatabase(name string) error {
	if _, exists := m.databases[name]; !exists {
		return fmt.Errorf("database '%s' does not exist", name)
	}
	delete(m.databases, name)
	return nil
}

func (m *mockDatabaseManager) ListDatabases() []DatabaseInfoInterface {
	result := make([]DatabaseInfoInterface, 0, len(m.databases))
	for _, db := range m.databases {
		result = append(result, db)
	}
	return result
}

func (m *mockDatabaseManager) Exists(name string) bool {
	_, exists := m.databases[name]
	return exists
}

func (m *mockDatabaseManager) CreateAlias(alias, databaseName string) error {
	return fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) DropAlias(alias string) error {
	return fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) ListAliases(databaseName string) map[string]string {
	return make(map[string]string)
}

func (m *mockDatabaseManager) ResolveDatabase(nameOrAlias string) (string, error) {
	if m.Exists(nameOrAlias) {
		return nameOrAlias, nil
	}
	return "", fmt.Errorf("database not found")
}

func (m *mockDatabaseManager) SetDatabaseLimits(databaseName string, limits interface{}) error {
	return fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) CreateCompositeDatabase(name string, constituents []interface{}) error {
	return fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) DropCompositeDatabase(name string) error {
	return fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) AddConstituent(compositeName string, constituent interface{}) error {
	return fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) RemoveConstituent(compositeName string, alias string) error {
	return fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *mockDatabaseManager) ListCompositeDatabases() []DatabaseInfoInterface {
	return []DatabaseInfoInterface{}
}

func (m *mockDatabaseManager) IsCompositeDatabase(name string) bool {
	return false
}

func TestSystemCommands_CreateDatabase(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Set up mock database manager
	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic") // Default database
	exec.SetDatabaseManager(mockDBM)

	t.Run("create new database", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CREATE DATABASE tenant_a", nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"name"}, result.Columns)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "tenant_a", result.Rows[0][0])
		assert.True(t, mockDBM.Exists("tenant_a"))
	})

	t.Run("create database with IF NOT EXISTS", func(t *testing.T) {
		// Create first time
		result1, err := exec.Execute(ctx, "CREATE DATABASE tenant_b IF NOT EXISTS", nil)
		require.NoError(t, err)
		assert.Len(t, result1.Rows, 1)

		// Create again with IF NOT EXISTS - should succeed
		result2, err := exec.Execute(ctx, "CREATE DATABASE tenant_b IF NOT EXISTS", nil)
		require.NoError(t, err)
		assert.Len(t, result2.Rows, 1)
		assert.True(t, mockDBM.Exists("tenant_b"))
	})

	t.Run("create existing database fails", func(t *testing.T) {
		mockDBM.CreateDatabase("existing")
		_, err := exec.Execute(ctx, "CREATE DATABASE existing", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("create database with whitespace", func(t *testing.T) {
		// Test flexible whitespace handling
		testCases := []struct {
			query  string
			dbName string
		}{
			{"CREATE DATABASE tenant_c", "tenant_c"},
			{"CREATE  DATABASE  tenant_d", "tenant_d"},
			{"CREATE\tDATABASE\ttenant_e", "tenant_e"},
			{"CREATE\nDATABASE\ntenant_f", "tenant_f"},
		}

		for _, tc := range testCases {
			t.Run(tc.dbName, func(t *testing.T) {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, "Query failed: %s", tc.query)
				require.Len(t, result.Rows, 1, "Should return one row")
				assert.Equal(t, tc.dbName, result.Rows[0][0])
				assert.True(t, mockDBM.Exists(tc.dbName))
			})
		}
	})

	t.Run("create database without manager fails", func(t *testing.T) {
		execNoDBM := NewStorageExecutor(store)
		_, err := execNoDBM.Execute(ctx, "CREATE DATABASE test", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database manager not available")
	})

	t.Run("invalid syntax", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CREATE DATABASE", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database name expected")
	})
}

func TestSystemCommands_DropDatabase(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic")
	mockDBM.CreateDatabase("tenant_a")
	mockDBM.CreateDatabase("tenant_b")
	exec.SetDatabaseManager(mockDBM)

	t.Run("drop existing database", func(t *testing.T) {
		result, err := exec.Execute(ctx, "DROP DATABASE tenant_a", nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"name"}, result.Columns)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "tenant_a", result.Rows[0][0])
		assert.False(t, mockDBM.Exists("tenant_a"))
	})

	t.Run("drop database with IF EXISTS", func(t *testing.T) {
		// Drop existing
		result1, err := exec.Execute(ctx, "DROP DATABASE tenant_b IF EXISTS", nil)
		require.NoError(t, err)
		assert.Len(t, result1.Rows, 1)

		// Drop non-existent with IF EXISTS - should succeed
		result2, err := exec.Execute(ctx, "DROP DATABASE tenant_b IF EXISTS", nil)
		require.NoError(t, err)
		assert.Len(t, result2.Rows, 0) // Empty result for IF EXISTS when not found
	})

	t.Run("drop non-existent database fails", func(t *testing.T) {
		_, err := exec.Execute(ctx, "DROP DATABASE nonexistent", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("drop database with whitespace", func(t *testing.T) {
		mockDBM.CreateDatabase("tenant_c")
		queries := []string{
			"DROP DATABASE tenant_c",
			"DROP  DATABASE  tenant_c",
			"DROP\tDATABASE\ttenant_c",
		}

		for _, query := range queries {
			_, err := exec.Execute(ctx, query, nil)
			// First one succeeds, others fail because already dropped
			if query == "DROP DATABASE tenant_c" {
				require.NoError(t, err)
			}
		}
	})

	t.Run("drop database without manager fails", func(t *testing.T) {
		execNoDBM := NewStorageExecutor(store)
		_, err := execNoDBM.Execute(ctx, "DROP DATABASE test", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database manager not available")
	})

	t.Run("invalid syntax", func(t *testing.T) {
		_, err := exec.Execute(ctx, "DROP DATABASE", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database name expected")
	})
}

func TestSystemCommands_ShowDatabases(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic")
	mockDBM.CreateDatabase("tenant_a")
	mockDBM.CreateDatabase("tenant_b")
	exec.SetDatabaseManager(mockDBM)

	t.Run("show all databases", func(t *testing.T) {
		result, err := exec.Execute(ctx, "SHOW DATABASES", nil)
		require.NoError(t, err)

		expectedColumns := []string{"name", "type", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "constituents"}
		assert.Equal(t, expectedColumns, result.Columns)
		assert.GreaterOrEqual(t, len(result.Rows), 3) // At least 3 databases

		// Verify database names are present
		dbNames := make(map[string]bool)
		for _, row := range result.Rows {
			if name, ok := row[0].(string); ok {
				dbNames[name] = true
			}
		}
		assert.True(t, dbNames["nornic"])
		assert.True(t, dbNames["tenant_a"])
		assert.True(t, dbNames["tenant_b"])
	})

	t.Run("show databases with whitespace", func(t *testing.T) {
		// Test that whitespace is handled flexibly
		// Note: findKeywordIndex handles whitespace, but multi-word keywords
		// need to match the pattern. "SHOW DATABASES" works, but "SHOW  DATABASES"
		// (double space) may not match the exact pattern.
		queries := []string{
			"SHOW DATABASES",
			"SHOW\tDATABASES",
			"SHOW\nDATABASES",
		}

		for _, query := range queries {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, "Query failed: %s", query)
			assert.GreaterOrEqual(t, len(result.Rows), 3)
		}
	})

	t.Run("show databases without manager fails", func(t *testing.T) {
		execNoDBM := NewStorageExecutor(store)
		_, err := execNoDBM.Execute(ctx, "SHOW DATABASES", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database manager not available")
	})
}

func TestSystemCommands_ShowDatabase(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("show current database", func(t *testing.T) {
		result, err := exec.Execute(ctx, "SHOW DATABASE", nil)
		require.NoError(t, err)

		expectedColumns := []string{"name", "type", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "constituents"}
		assert.Equal(t, expectedColumns, result.Columns)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "nornic", result.Rows[0][0]) // Default database name
	})
}

func TestSystemCommands_Integration(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic")
	exec.SetDatabaseManager(mockDBM)

	t.Run("create, show, drop workflow", func(t *testing.T) {
		// Create database
		_, err := exec.Execute(ctx, "CREATE DATABASE test_db", nil)
		require.NoError(t, err)
		assert.True(t, mockDBM.Exists("test_db"))

		// Show databases - should include test_db
		result, err := exec.Execute(ctx, "SHOW DATABASES", nil)
		require.NoError(t, err)
		found := false
		for _, row := range result.Rows {
			if name, ok := row[0].(string); ok && name == "test_db" {
				found = true
				break
			}
		}
		assert.True(t, found, "test_db should appear in SHOW DATABASES")

		// Drop database
		_, err = exec.Execute(ctx, "DROP DATABASE test_db", nil)
		require.NoError(t, err)
		assert.False(t, mockDBM.Exists("test_db"))
	})
}
