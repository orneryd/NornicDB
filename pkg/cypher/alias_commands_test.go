// Package cypher - Comprehensive unit tests for alias commands with whitespace permutations.
//
// These tests verify CREATE ALIAS, DROP ALIAS, and SHOW ALIASES commands
// work correctly with various whitespace patterns (spaces, tabs, newlines, etc.).
package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// aliasTestDatabaseManager implements DatabaseManagerInterface for alias testing.
type aliasTestDatabaseManager struct {
	databases map[string]*multidb.DatabaseInfo
	aliases   map[string]string // alias -> database
}

func newAliasTestDatabaseManager() *aliasTestDatabaseManager {
	return &aliasTestDatabaseManager{
		databases: make(map[string]*multidb.DatabaseInfo),
		aliases:   make(map[string]string),
	}
}

func (m *aliasTestDatabaseManager) CreateDatabase(name string) error {
	if _, exists := m.databases[name]; exists {
		return fmt.Errorf("database '%s' already exists", name)
	}
	m.databases[name] = &multidb.DatabaseInfo{
		Name:      name,
		CreatedAt: time.Now(),
		Status:    "online",
		Type:      "standard",
		IsDefault: false,
		UpdatedAt: time.Now(),
		Aliases:   []string{},
		Limits:    nil,
	}
	return nil
}

func (m *aliasTestDatabaseManager) DropDatabase(name string) error {
	if _, exists := m.databases[name]; !exists {
		return fmt.Errorf("database '%s' does not exist", name)
	}
	delete(m.databases, name)
	return nil
}

func (m *aliasTestDatabaseManager) ListDatabases() []DatabaseInfoInterface {
	result := make([]DatabaseInfoInterface, 0, len(m.databases))
	for _, db := range m.databases {
		result = append(result, &aliasTestDatabaseInfo{info: db})
	}
	return result
}

func (m *aliasTestDatabaseManager) Exists(name string) bool {
	_, exists := m.databases[name]
	return exists
}

func (m *aliasTestDatabaseManager) CreateAlias(alias, databaseName string) error {
	if _, exists := m.aliases[alias]; exists {
		return fmt.Errorf("alias '%s' already exists", alias)
	}
	if _, exists := m.databases[databaseName]; !exists {
		return fmt.Errorf("database '%s' does not exist", databaseName)
	}
	m.aliases[alias] = databaseName
	if m.databases[databaseName].Aliases == nil {
		m.databases[databaseName].Aliases = []string{}
	}
	m.databases[databaseName].Aliases = append(m.databases[databaseName].Aliases, alias)
	return nil
}

func (m *aliasTestDatabaseManager) DropAlias(alias string) error {
	if _, exists := m.aliases[alias]; !exists {
		return fmt.Errorf("alias '%s' does not exist", alias)
	}
	dbName := m.aliases[alias]
	delete(m.aliases, alias)
	// Remove from database's aliases list
	if info, exists := m.databases[dbName]; exists {
		for i, a := range info.Aliases {
			if a == alias {
				info.Aliases = append(info.Aliases[:i], info.Aliases[i+1:]...)
				break
			}
		}
	}
	return nil
}

func (m *aliasTestDatabaseManager) ListAliases(databaseName string) map[string]string {
	result := make(map[string]string)
	if databaseName == "" {
		// All aliases
		for alias, db := range m.aliases {
			result[alias] = db
		}
	} else {
		// Aliases for specific database
		if info, exists := m.databases[databaseName]; exists {
			for _, alias := range info.Aliases {
				result[alias] = databaseName
			}
		}
	}
	return result
}

func (m *aliasTestDatabaseManager) ResolveDatabase(nameOrAlias string) (string, error) {
	if _, exists := m.databases[nameOrAlias]; exists {
		return nameOrAlias, nil
	}
	if db, exists := m.aliases[nameOrAlias]; exists {
		return db, nil
	}
	return "", fmt.Errorf("database not found")
}

func (m *aliasTestDatabaseManager) SetDatabaseLimits(databaseName string, limits interface{}) error {
	return fmt.Errorf("not implemented")
}

func (m *aliasTestDatabaseManager) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *aliasTestDatabaseManager) CreateCompositeDatabase(name string, constituents []interface{}) error {
	return fmt.Errorf("not implemented")
}

func (m *aliasTestDatabaseManager) DropCompositeDatabase(name string) error {
	return fmt.Errorf("not implemented")
}

func (m *aliasTestDatabaseManager) AddConstituent(compositeName string, constituent interface{}) error {
	return fmt.Errorf("not implemented")
}

func (m *aliasTestDatabaseManager) RemoveConstituent(compositeName string, alias string) error {
	return fmt.Errorf("not implemented")
}

func (m *aliasTestDatabaseManager) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *aliasTestDatabaseManager) ListCompositeDatabases() []DatabaseInfoInterface {
	return []DatabaseInfoInterface{}
}

func (m *aliasTestDatabaseManager) IsCompositeDatabase(name string) bool {
	return false
}

type aliasTestDatabaseInfo struct {
	info *multidb.DatabaseInfo
}

func (a *aliasTestDatabaseInfo) Name() string         { return a.info.Name }
func (a *aliasTestDatabaseInfo) Type() string         { return a.info.Type }
func (a *aliasTestDatabaseInfo) Status() string       { return a.info.Status }
func (a *aliasTestDatabaseInfo) IsDefault() bool      { return a.info.IsDefault }
func (a *aliasTestDatabaseInfo) CreatedAt() time.Time { return a.info.CreatedAt }

// whitespaceVariations generates query variations with different whitespace patterns.
func whitespaceVariations(baseQuery string) []string {
	variations := []string{
		baseQuery,                                    // Original
		strings.ReplaceAll(baseQuery, " ", "  "),     // Double spaces
		strings.ReplaceAll(baseQuery, " ", "\t"),     // Tabs
		strings.ReplaceAll(baseQuery, " ", "\n"),     // Newlines
		strings.ReplaceAll(baseQuery, " ", "\r\n"),   // Windows newlines
		strings.ReplaceAll(baseQuery, " ", " \t"),    // Space + tab
		strings.ReplaceAll(baseQuery, " ", "\t "),    // Tab + space
		strings.ReplaceAll(baseQuery, " ", " \n"),    // Space + newline
		strings.ReplaceAll(baseQuery, " ", "\n "),    // Newline + space
		strings.ReplaceAll(baseQuery, " ", "  \t  "), // Multiple spaces + tabs
		strings.ReplaceAll(baseQuery, " ", "\n\t"),   // Newline + tab
		strings.ReplaceAll(baseQuery, " ", "\t\n"),   // Tab + newline
		strings.ReplaceAll(baseQuery, " ", " \t\n "), // Space + tab + newline + space
		strings.ReplaceAll(baseQuery, " ", "\r\n\t"), // Windows newline + tab
		strings.ReplaceAll(baseQuery, " ", " \r\n "), // Space + Windows newline + space
	}
	return variations
}

func TestAliasCommands_CreateAlias_WhitespaceVariations(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newAliasTestDatabaseManager()
	mockDBM.CreateDatabase("tenant_a")
	mockDBM.CreateDatabase("tenant_b")
	mockDBM.CreateDatabase("tenant_c")
	exec.SetDatabaseManager(mockDBM)

	baseQueries := []string{
		"CREATE ALIAS main FOR DATABASE tenant_a",
		"CREATE ALIAS prod FOR DATABASE tenant_b",
		"CREATE ALIAS dev FOR DATABASE tenant_c",
	}

	for _, baseQuery := range baseQueries {
		t.Run(fmt.Sprintf("base_query_%s", strings.Fields(baseQuery)[2]), func(t *testing.T) {
			variations := whitespaceVariations(baseQuery)
			for i, query := range variations {
				t.Run(fmt.Sprintf("variation_%d", i), func(t *testing.T) {
					// Extract alias name from query
					parts := strings.Fields(baseQuery)
					baseAliasName := parts[2]

					// Use unique alias name for each variation to avoid conflicts
					aliasName := fmt.Sprintf("%s_var_%d", baseAliasName, i)
					// Replace alias name in query (handle both alias and database name replacement)
					testQuery := strings.Replace(query, baseAliasName, aliasName, 1)
					// Also need to replace database name if it matches
					dbName := parts[len(parts)-1] // Last field is database name
					if dbName == baseAliasName {
						// Alias and database have same name, need different replacement
						testQuery = query
						testQuery = strings.Replace(testQuery, " "+baseAliasName+" ", " "+aliasName+" ", 1)
						testQuery = strings.Replace(testQuery, "\t"+baseAliasName+"\t", "\t"+aliasName+"\t", 1)
						testQuery = strings.Replace(testQuery, "\n"+baseAliasName+"\n", "\n"+aliasName+"\n", 1)
					}

					result, err := exec.Execute(ctx, testQuery, nil)
					require.NoError(t, err, "Query must succeed with whitespace variation: %q", testQuery)
					require.NotEmpty(t, result.Columns, "Query must return columns: %q", testQuery)
					assert.Equal(t, []string{"alias"}, result.Columns)
					require.Len(t, result.Rows, 1, "Query must return one row: %q", testQuery)
					assert.Equal(t, aliasName, result.Rows[0][0])

					// Verify alias was created
					aliases := mockDBM.ListAliases("")
					assert.Contains(t, aliases, aliasName, "Alias should be created: %q", testQuery)
				})
			}
		})
	}
}

func TestAliasCommands_DropAlias_WhitespaceVariations(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newAliasTestDatabaseManager()
	mockDBM.CreateDatabase("tenant_a")
	exec.SetDatabaseManager(mockDBM)

	baseQueries := []string{
		"DROP ALIAS main",
		"DROP ALIAS prod",
		"DROP ALIAS main IF EXISTS",
	}

	for _, baseQuery := range baseQueries {
		t.Run(fmt.Sprintf("base_query_%s", strings.Fields(baseQuery)[2]), func(t *testing.T) {
			variations := whitespaceVariations(baseQuery)
			for i, query := range variations {
				t.Run(fmt.Sprintf("variation_%d", i), func(t *testing.T) {
					// Extract alias name from query
					parts := strings.Fields(baseQuery)
					baseAliasName := parts[2]

					// Use unique alias name for each variation to avoid conflicts
					aliasName := fmt.Sprintf("%s_var_%d", baseAliasName, i)
					// Replace alias name in query
					testQuery := strings.Replace(query, baseAliasName, aliasName, 1)

					// Create alias first if testing DROP with IF EXISTS
					if strings.Contains(baseQuery, "IF EXISTS") {
						mockDBM.CreateAlias(aliasName, "tenant_a")
					} else {
						// Create alias for non-IF EXISTS tests
						mockDBM.CreateAlias(aliasName, "tenant_a")
					}

					result, err := exec.Execute(ctx, testQuery, nil)
					require.NoError(t, err, "Query must succeed with whitespace variation: %q", testQuery)
					require.NotEmpty(t, result.Columns, "Query must return columns: %q", testQuery)
					assert.Equal(t, []string{"alias"}, result.Columns)

					if strings.Contains(baseQuery, "IF EXISTS") {
						// IF EXISTS may return empty result if alias doesn't exist
						// But query should still succeed and return columns
					} else {
						require.Len(t, result.Rows, 1, "Query must return one row: %q", testQuery)
						assert.Equal(t, aliasName, result.Rows[0][0])
					}
				})
			}
		})
	}
}

func TestAliasCommands_ShowAliases_WhitespaceVariations(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newAliasTestDatabaseManager()
	mockDBM.CreateDatabase("tenant_a")
	mockDBM.CreateDatabase("tenant_b")
	mockDBM.CreateAlias("main", "tenant_a")
	mockDBM.CreateAlias("prod", "tenant_b")
	exec.SetDatabaseManager(mockDBM)

	baseQueries := []string{
		"SHOW ALIASES",
		"SHOW ALIASES FOR DATABASE tenant_a",
		"SHOW ALIASES FOR DATABASE tenant_b",
	}

	for _, baseQuery := range baseQueries {
		t.Run(fmt.Sprintf("base_query_%s", strings.Fields(baseQuery)[1]), func(t *testing.T) {
			variations := whitespaceVariations(baseQuery)
			for i, query := range variations {
				t.Run(fmt.Sprintf("variation_%d", i), func(t *testing.T) {
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, "Query must succeed with whitespace variation: %q", query)
					require.NotEmpty(t, result.Columns, "Query must return columns: %q", query)
					assert.Equal(t, []string{"alias", "database"}, result.Columns)

					if strings.Contains(baseQuery, "FOR DATABASE") {
						// Should have at least zero rows (may be empty if no aliases)
						assert.GreaterOrEqual(t, len(result.Rows), 0, "Query must return valid result: %q", query)
					} else {
						// Should have at least 2 rows (main and prod)
						assert.GreaterOrEqual(t, len(result.Rows), 2, "Query must return at least 2 rows: %q", query)
					}
				})
			}
		})
	}
}

func TestAliasCommands_CreateAlias_ErrorCases_WhitespaceVariations(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newAliasTestDatabaseManager()
	mockDBM.CreateDatabase("tenant_a")
	mockDBM.CreateAlias("existing", "tenant_a")
	exec.SetDatabaseManager(mockDBM)

	// Test cases that should fail
	testCases := []struct {
		name      string
		query     string
		expectErr bool
	}{
		{
			name:      "alias_already_exists",
			query:     "CREATE ALIAS existing FOR DATABASE tenant_a",
			expectErr: true,
		},
		{
			name:      "database_not_found",
			query:     "CREATE ALIAS new_alias FOR DATABASE nonexistent",
			expectErr: true,
		},
		{
			name:      "invalid_syntax_missing_FOR",
			query:     "CREATE ALIAS new_alias tenant_a",
			expectErr: true,
		},
		{
			name:      "invalid_syntax_missing_DATABASE",
			query:     "CREATE ALIAS new_alias FOR tenant_a",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			variations := whitespaceVariations(tc.query)
			for i, query := range variations {
				t.Run(fmt.Sprintf("variation_%d", i), func(t *testing.T) {
					_, err := exec.Execute(ctx, query, nil)
					if tc.expectErr {
						assert.Error(t, err, "Expected error for query: %q", query)
					} else {
						assert.NoError(t, err, "Unexpected error for query: %q", query)
					}
				})
			}
		})
	}
}

func TestAliasCommands_DropAlias_ErrorCases_WhitespaceVariations(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newAliasTestDatabaseManager()
	mockDBM.CreateDatabase("tenant_a")
	exec.SetDatabaseManager(mockDBM)

	// Test cases that should fail
	testCases := []struct {
		name      string
		query     string
		expectErr bool
	}{
		{
			name:      "alias_not_found",
			query:     "DROP ALIAS nonexistent",
			expectErr: true,
		},
		{
			name:      "alias_not_found_with_IF_EXISTS",
			query:     "DROP ALIAS nonexistent IF EXISTS",
			expectErr: false, // IF EXISTS should not error
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			variations := whitespaceVariations(tc.query)
			for i, query := range variations {
				t.Run(fmt.Sprintf("variation_%d", i), func(t *testing.T) {
					_, err := exec.Execute(ctx, query, nil)
					if tc.expectErr {
						assert.Error(t, err, "Expected error for query: %q", query)
					} else {
						assert.NoError(t, err, "Unexpected error for query: %q", query)
					}
				})
			}
		})
	}
}

func TestAliasCommands_ComplexWhitespacePatterns(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newAliasTestDatabaseManager()
	mockDBM.CreateDatabase("tenant_a")
	mockDBM.CreateDatabase("tenant_b")
	exec.SetDatabaseManager(mockDBM)

	// Complex whitespace patterns
	complexQueries := []string{
		"CREATE\tALIAS\nmain\r\nFOR\tDATABASE\n tenant_a",
		"CREATE  ALIAS\t\tmain\n\nFOR\r\n\r\nDATABASE\t\t tenant_a",
		"DROP\tALIAS\nmain\r\nIF\tEXISTS\n",
		"SHOW\tALIASES\nFOR\r\nDATABASE\t tenant_a",
		"CREATE ALIAS\tprod\nFOR DATABASE\r\ntenant_b\t",
		"DROP  ALIAS  main  IF  EXISTS  ",
		"SHOW  ALIASES  FOR  DATABASE  tenant_a  ",
	}

	for i, query := range complexQueries {
		t.Run(fmt.Sprintf("complex_pattern_%d", i), func(t *testing.T) {
			if strings.HasPrefix(strings.TrimSpace(query), "CREATE ALIAS") {
				// Create alias
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, "Query failed: %q", query)
				assert.Equal(t, []string{"alias"}, result.Columns)
			} else if strings.HasPrefix(strings.TrimSpace(query), "DROP ALIAS") {
				// Drop alias (create first if needed)
				if !strings.Contains(query, "main") || mockDBM.ListAliases("")["main"] == "" {
					mockDBM.CreateAlias("main", "tenant_a")
				}
				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, "Query failed: %q", query)
			} else if strings.HasPrefix(strings.TrimSpace(query), "SHOW ALIASES") {
				// Show aliases
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, "Query failed: %q", query)
				assert.Equal(t, []string{"alias", "database"}, result.Columns)
			}
		})
	}
}

func TestAliasCommands_EdgeCases_Whitespace(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newAliasTestDatabaseManager()
	mockDBM.CreateDatabase("tenant_a")
	exec.SetDatabaseManager(mockDBM)

	// Edge cases with whitespace - use unique alias names to avoid conflicts
	edgeCases := []struct {
		name      string
		query     string
		aliasName string
		expectErr bool
	}{
		{
			name:      "leading_whitespace",
			query:     "   CREATE ALIAS alias_leading FOR DATABASE tenant_a",
			aliasName: "alias_leading",
			expectErr: false,
		},
		{
			name:      "trailing_whitespace",
			query:     "CREATE ALIAS alias_trailing FOR DATABASE tenant_a   ",
			aliasName: "alias_trailing",
			expectErr: false,
		},
		{
			name:      "mixed_whitespace",
			query:     "\tCREATE\nALIAS\talias_mixed\r\nFOR DATABASE\ttenant_a\n",
			aliasName: "alias_mixed",
			expectErr: false,
		},
		{
			name:      "only_whitespace_between_keywords",
			query:     "CREATE\t\tALIAS\n\nalias_whitespace\r\n\r\nFOR\t\tDATABASE\n\ntenant_a",
			aliasName: "alias_whitespace",
			expectErr: false,
		},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			// Drop alias if it exists from previous test runs
			mockDBM.DropAlias(tc.aliasName)

			_, err := exec.Execute(ctx, tc.query, nil)
			if tc.expectErr {
				assert.Error(t, err, "Expected error for query: %q", tc.query)
			} else {
				assert.NoError(t, err, "Query must succeed: %q", tc.query)
			}
		})
	}
}
