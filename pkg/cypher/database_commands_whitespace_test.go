// Package cypher - Comprehensive whitespace permutation tests for database commands.
//
// These tests verify CREATE DATABASE, DROP DATABASE, SHOW DATABASES, and SHOW DATABASE
// commands work correctly with extensive whitespace variations (spaces, tabs, newlines, etc.).
package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// whitespacePermutations generates comprehensive whitespace variations for testing.
func whitespacePermutations(baseQuery string) []string {
	// Generate all possible whitespace combinations
	variations := []string{
		baseQuery, // Original (no change)
		// Single space variations
		strings.ReplaceAll(baseQuery, " ", "  "),  // Double spaces
		strings.ReplaceAll(baseQuery, " ", "   "), // Triple spaces
		// Tab variations
		strings.ReplaceAll(baseQuery, " ", "\t"),   // Tabs
		strings.ReplaceAll(baseQuery, " ", "\t\t"), // Double tabs
		// Newline variations
		strings.ReplaceAll(baseQuery, " ", "\n"),   // Newlines
		strings.ReplaceAll(baseQuery, " ", "\r\n"), // Windows newlines
		// Mixed whitespace
		strings.ReplaceAll(baseQuery, " ", " \t"),   // Space + tab
		strings.ReplaceAll(baseQuery, " ", "\t "),   // Tab + space
		strings.ReplaceAll(baseQuery, " ", " \n"),   // Space + newline
		strings.ReplaceAll(baseQuery, " ", "\n "),   // Newline + space
		strings.ReplaceAll(baseQuery, " ", "\t\n"),  // Tab + newline
		strings.ReplaceAll(baseQuery, " ", "\n\t"),  // Newline + tab
		strings.ReplaceAll(baseQuery, " ", " \r\n"), // Space + Windows newline
		strings.ReplaceAll(baseQuery, " ", "\r\n "), // Windows newline + space
		// Multiple whitespace combinations
		strings.ReplaceAll(baseQuery, " ", "  \t  "), // Multiple spaces + tabs
		strings.ReplaceAll(baseQuery, " ", "\t  \t"), // Tabs + spaces + tabs
		strings.ReplaceAll(baseQuery, " ", " \n\t "), // Space + newline + tab + space
		strings.ReplaceAll(baseQuery, " ", "\r\n\t"), // Windows newline + tab
		strings.ReplaceAll(baseQuery, " ", " \r\n "), // Space + Windows newline + space
		strings.ReplaceAll(baseQuery, " ", "\n\t\n"), // Newline + tab + newline
		strings.ReplaceAll(baseQuery, " ", "\t\n\t"), // Tab + newline + tab
		// Leading/trailing whitespace
		"   " + baseQuery + "   ",     // Leading and trailing spaces
		"\t\t" + baseQuery + "\t\t",   // Leading and trailing tabs
		"\n\n" + baseQuery + "\n\n",   // Leading and trailing newlines
		" \t\n" + baseQuery + "\n\t ", // Mixed leading/trailing
	}
	return variations
}

func TestDatabaseCommands_CreateDatabase_WhitespacePermutations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic") // Default database
	exec.SetDatabaseManager(mockDBM)

	baseQueries := []string{
		"CREATE DATABASE tenant_a",
		"CREATE DATABASE tenant_b IF NOT EXISTS",
		"CREATE DATABASE tenant_c",
	}

	for _, baseQuery := range baseQueries {
		t.Run(fmt.Sprintf("base_%s", strings.Fields(baseQuery)[2]), func(t *testing.T) {
			variations := whitespacePermutations(baseQuery)
			for i, query := range variations {
				t.Run(fmt.Sprintf("permutation_%d", i), func(t *testing.T) {
					// Extract expected database name
					parts := strings.Fields(baseQuery)
					expectedDB := parts[2]

					// Use unique database name for each permutation to avoid conflicts
					uniqueDB := fmt.Sprintf("%s_perm_%d", expectedDB, i)
					// Replace database name in query
					testQuery := strings.Replace(query, expectedDB, uniqueDB, 1)

					result, err := exec.Execute(ctx, testQuery, nil)
					require.NoError(t, err, "Query must succeed with whitespace permutation: %q", testQuery)
					require.NotEmpty(t, result.Columns, "Query must return columns: %q", testQuery)
					assert.Equal(t, []string{"name"}, result.Columns)

					// IF NOT EXISTS on existing database returns success but may have empty rows
					if strings.Contains(baseQuery, "IF NOT EXISTS") && mockDBM.Exists(uniqueDB) {
						// Already exists - query succeeds but may return empty or existing row
					} else {
						require.Len(t, result.Rows, 1, "Query must return one row: %q", testQuery)
						assert.Equal(t, uniqueDB, result.Rows[0][0])
						assert.True(t, mockDBM.Exists(uniqueDB), "Database should exist: %q", testQuery)
					}
				})
			}
		})
	}
}

func TestDatabaseCommands_DropDatabase_WhitespacePermutations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic")
	exec.SetDatabaseManager(mockDBM)

	baseQueries := []string{
		"DROP DATABASE tenant_a",
		"DROP DATABASE tenant_b IF EXISTS",
	}

	// Create databases for testing
	mockDBM.CreateDatabase("tenant_a")
	mockDBM.CreateDatabase("tenant_b")

	for _, baseQuery := range baseQueries {
		t.Run(fmt.Sprintf("base_%s", strings.Fields(baseQuery)[2]), func(t *testing.T) {
			variations := whitespacePermutations(baseQuery)
			for i, query := range variations {
				t.Run(fmt.Sprintf("permutation_%d", i), func(t *testing.T) {
					// Extract expected database name
					parts := strings.Fields(baseQuery)
					expectedDB := parts[2]

					// Recreate database if needed (for multiple test runs)
					if !mockDBM.Exists(expectedDB) && !strings.Contains(baseQuery, "IF EXISTS") {
						mockDBM.CreateDatabase(expectedDB)
					}

					result, err := exec.Execute(ctx, query, nil)

					if strings.Contains(baseQuery, "IF EXISTS") {
						// IF EXISTS should always succeed
						require.NoError(t, err, "Query must succeed with IF EXISTS: %q", query)
					} else {
						require.NoError(t, err, "Query must succeed with whitespace permutation: %q", query)
						require.NotEmpty(t, result.Columns, "Query must return columns: %q", query)
						assert.Equal(t, []string{"name"}, result.Columns)
						if len(result.Rows) > 0 {
							assert.Equal(t, expectedDB, result.Rows[0][0])
						}
						assert.False(t, mockDBM.Exists(expectedDB), "Database should be dropped: %q", query)
					}
				})
			}
		})
	}
}

func TestDatabaseCommands_ShowDatabases_WhitespacePermutations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic")
	mockDBM.CreateDatabase("tenant_a")
	mockDBM.CreateDatabase("tenant_b")
	exec.SetDatabaseManager(mockDBM)

	baseQuery := "SHOW DATABASES"
	variations := whitespacePermutations(baseQuery)

	for i, query := range variations {
		t.Run(fmt.Sprintf("permutation_%d", i), func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, "Query must succeed with whitespace permutation: %q", query)
			require.NotEmpty(t, result.Columns, "Query must return columns: %q", query)

			expectedColumns := []string{"name", "type", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "constituents"}
			assert.Equal(t, expectedColumns, result.Columns)
			assert.GreaterOrEqual(t, len(result.Rows), 3, "Query must return at least 3 databases: %q", query)
		})
	}
}

func TestDatabaseCommands_ShowDatabase_WhitespacePermutations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	baseQuery := "SHOW DATABASE"
	variations := whitespacePermutations(baseQuery)

	for i, query := range variations {
		t.Run(fmt.Sprintf("permutation_%d", i), func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, "Query must succeed with whitespace permutation: %q", query)
			require.NotEmpty(t, result.Columns, "Query must return columns: %q", query)

			expectedColumns := []string{"name", "type", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "constituents"}
			assert.Equal(t, expectedColumns, result.Columns)
			assert.Len(t, result.Rows, 1, "Query must return one row: %q", query)
		})
	}
}

func TestDatabaseCommands_ComplexWhitespacePatterns(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic")
	exec.SetDatabaseManager(mockDBM)

	// Complex whitespace patterns that should all work
	complexQueries := []string{
		"CREATE\tDATABASE\n tenant_a\r\n",
		"CREATE  DATABASE\t\ttenant_b\n\n",
		"DROP\tDATABASE\n tenant_a\r\n",
		"SHOW\tDATABASES\n",
		"SHOW  DATABASE  ",
		"CREATE DATABASE\tprod\nIF NOT EXISTS\r\n",
		"DROP  DATABASE  dev  IF  EXISTS  ",
		"\tCREATE\nDATABASE\ttenant_c\r\n",
		"  SHOW  DATABASES  ",
		"\n\nSHOW\n\nDATABASE\n\n",
	}

	for i, query := range complexQueries {
		t.Run(fmt.Sprintf("complex_pattern_%d", i), func(t *testing.T) {
			if strings.Contains(query, "CREATE DATABASE") {
				// Extract database name
				parts := strings.Fields(query)
				dbName := parts[2]

				// Skip if already exists (for repeated runs)
				if mockDBM.Exists(dbName) && !strings.Contains(query, "IF NOT EXISTS") {
					return
				}

				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, "Complex query must succeed: %q", query)
				if len(result.Rows) > 0 {
					assert.Equal(t, dbName, result.Rows[0][0])
				}
			} else if strings.Contains(query, "DROP DATABASE") {
				// Extract database name
				parts := strings.Fields(query)
				dbName := parts[2]

				// Create database first if it doesn't exist
				if !mockDBM.Exists(dbName) {
					mockDBM.CreateDatabase(dbName)
				}

				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, "Complex query must succeed: %q", query)
			} else if strings.Contains(query, "SHOW DATABASES") {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, "Complex query must succeed: %q", query)
				assert.GreaterOrEqual(t, len(result.Rows), 1)
			} else if strings.Contains(query, "SHOW DATABASE") {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, "Complex query must succeed: %q", query)
				assert.Len(t, result.Rows, 1)
			}
		})
	}
}

func TestDatabaseCommands_EdgeCases_Whitespace(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	mockDBM.CreateDatabase("nornic")
	exec.SetDatabaseManager(mockDBM)

	// Edge cases with extreme whitespace
	edgeCases := []struct {
		name      string
		query     string
		expectErr bool
	}{
		{
			name:      "only_tabs",
			query:     "CREATE\t\tDATABASE\t\ttenant_edge1",
			expectErr: false,
		},
		{
			name:      "only_newlines",
			query:     "CREATE\n\nDATABASE\n\ntenant_edge2",
			expectErr: false,
		},
		{
			name:      "mixed_extreme",
			query:     "\tCREATE\n\tDATABASE\r\n\ttenant_edge3\t",
			expectErr: false,
		},
		{
			name:      "windows_newlines",
			query:     "CREATE\r\nDATABASE\r\ntenant_edge4",
			expectErr: false,
		},
		{
			name:      "trailing_whitespace_only",
			query:     "CREATE DATABASE tenant_edge5   ",
			expectErr: false,
		},
		{
			name:      "leading_whitespace_only",
			query:     "   CREATE DATABASE tenant_edge6",
			expectErr: false,
		},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, tc.query, nil)
			if tc.expectErr {
				assert.Error(t, err, "Expected error for query: %q", tc.query)
			} else {
				assert.NoError(t, err, "Query must succeed: %q", tc.query)
			}
		})
	}
}
