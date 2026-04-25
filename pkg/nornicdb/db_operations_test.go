package nornicdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type backupErrorEngine struct {
	storage.Engine
	allNodesErr error
	allEdgesErr error
}

func (e *backupErrorEngine) AllNodes() ([]*storage.Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.Engine.AllNodes()
}

func (e *backupErrorEngine) AllEdges() ([]*storage.Edge, error) {
	if e.allEdgesErr != nil {
		return nil, e.allEdgesErr
	}
	return e.Engine.AllEdges()
}

func TestDB_GetIndexes(t *testing.T) {
	t.Run("returns empty for new database", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		indexes, err := db.GetIndexes(context.Background())
		require.NoError(t, err)
		assert.NotNil(t, indexes)
		assert.Len(t, indexes, 0)
	})

	t.Run("returns indexes after creation", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		// Create an index
		err = db.CreateIndex(context.Background(), "User", "email", "property")
		require.NoError(t, err)

		indexes, err := db.GetIndexes(context.Background())
		require.NoError(t, err)
		assert.Len(t, indexes, 1)
		assert.Equal(t, "User", indexes[0].Label)
		assert.Equal(t, "email", indexes[0].Property)
	})
}

func TestDB_CreateIndex(t *testing.T) {
	testCases := []struct {
		name      string
		indexType string
		wantErr   bool
	}{
		{"property index", "property", false},
		{"btree index", "btree", false},
		{"fulltext index", "fulltext", false},
		{"vector index", "vector", false},
		{"range index", "range", false},
		{"invalid type", "invalid", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open("", nil)
			require.NoError(t, err)
			defer db.Close()

			err = db.CreateIndex(context.Background(), "TestLabel", "testProperty", tc.indexType)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDB_Backup(t *testing.T) {
	t.Run("backup in-memory database as JSON", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		// Add test data
		_, err = db.ExecuteCypher(context.Background(), "CREATE (n:TestNode {name: 'test', value: 123})", nil)
		require.NoError(t, err)

		// Create backup
		backupPath := filepath.Join(t.TempDir(), "backup.json")
		err = db.Backup(context.Background(), backupPath)
		require.NoError(t, err)

		// Verify backup exists and has content
		data, err := os.ReadFile(backupPath)
		require.NoError(t, err)
		assert.Contains(t, string(data), "TestNode")
		assert.Contains(t, string(data), "test")
	})

	t.Run("backup persistent database", func(t *testing.T) {
		dbDir := t.TempDir()
		db, err := Open(dbDir, nil)
		require.NoError(t, err)

		// Add test data
		_, err = db.ExecuteCypher(context.Background(), "CREATE (n:TestNode {name: 'test'})", nil)
		require.NoError(t, err)

		// Create backup
		backupPath := filepath.Join(t.TempDir(), "backup.bin")
		err = db.Backup(context.Background(), backupPath)
		require.NoError(t, err)

		db.Close()

		// Verify backup exists
		info, err := os.Stat(backupPath)
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0))
	})

	t.Run("backup closed database returns ErrClosed", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		err = db.Backup(context.Background(), filepath.Join(t.TempDir(), "closed.json"))
		require.ErrorIs(t, err, ErrClosed)
	})

	t.Run("backup write failure returns wrapped error", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		// Writing backup to a directory path should fail in os.WriteFile.
		err = db.Backup(context.Background(), t.TempDir())
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to write backup")
	})

	t.Run("backup returns wrapped node read error", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		db := &DB{
			storage: &backupErrorEngine{
				Engine:      base,
				allNodesErr: errors.New("boom nodes"),
			},
			config: DefaultConfig(),
		}

		err := db.Backup(context.Background(), filepath.Join(t.TempDir(), "should-not-write.json"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get nodes")
	})

	t.Run("backup returns wrapped edge read error", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		db := &DB{
			storage: &backupErrorEngine{
				Engine:      base,
				allEdgesErr: errors.New("boom edges"),
			},
			config: DefaultConfig(),
		}

		err := db.Backup(context.Background(), filepath.Join(t.TempDir(), "should-not-write.json"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get edges")
	})
}

func TestDB_ExportUserData_CSV(t *testing.T) {
	t.Run("exports user data as CSV", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		// Create test nodes with owner_id
		ctx := context.Background()
		_, err = db.ExecuteCypher(ctx, `
			CREATE (u1:User {owner_id: 'user123', name: 'Alice', email: 'alice@example.com', age: 30})
			CREATE (u2:User {owner_id: 'user123', name: 'Bob', email: 'bob@example.com'})
			CREATE (u3:User {owner_id: 'other', name: 'Charlie'})
		`, nil)
		require.NoError(t, err)

		// Export as CSV
		data, err := db.ExportUserData(ctx, "user123", "csv")
		require.NoError(t, err)

		csv := string(data)

		// Verify CSV structure
		lines := strings.Split(strings.TrimSpace(csv), "\n")
		assert.GreaterOrEqual(t, len(lines), 2) // Header + at least 1 data row

		// Verify header
		header := lines[0]
		assert.Contains(t, header, "id")
		assert.Contains(t, header, "labels")
		assert.Contains(t, header, "created_at")

		// Verify data rows contain user data
		csvContent := string(data)
		assert.Contains(t, csvContent, "Alice")
		assert.Contains(t, csvContent, "Bob")
		assert.NotContains(t, csvContent, "Charlie") // Different owner
	})

	t.Run("handles special CSV characters", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		ctx := context.Background()
		_, err = db.ExecuteCypher(ctx, `
			CREATE (u:User {owner_id: 'user123', name: 'Test, User', description: 'Has "quotes" and, commas'})
		`, nil)
		require.NoError(t, err)

		data, err := db.ExportUserData(ctx, "user123", "csv")
		require.NoError(t, err)

		csv := string(data)
		// Verify proper CSV escaping (quotes should be doubled and wrapped in quotes)
		assert.Contains(t, csv, "\"Test, User\"")
		assert.Contains(t, csv, "\"Has \"\"quotes\"\" and, commas\"")
	})

	t.Run("exports empty result for non-existent user", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		data, err := db.ExportUserData(context.Background(), "nonexistent", "csv")
		require.NoError(t, err)

		csv := string(data)
		lines := strings.Split(strings.TrimSpace(csv), "\n")
		assert.Len(t, lines, 1) // Only header
	})
}

func TestDB_ExportUserData_JSON(t *testing.T) {
	t.Run("exports user data as JSON", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		ctx := context.Background()
		_, err = db.ExecuteCypher(ctx, `
			CREATE (u:User {owner_id: 'user456', name: 'Test User'})
		`, nil)
		require.NoError(t, err)

		data, err := db.ExportUserData(ctx, "user456", "json")
		require.NoError(t, err)

		jsonStr := string(data)
		assert.Contains(t, jsonStr, "user456")
		assert.Contains(t, jsonStr, "Test User")
		assert.Contains(t, jsonStr, "data")
		assert.Contains(t, jsonStr, "exported_at")
	})
}

func TestDB_GetDecayInfo(t *testing.T) {
	t.Run("returns disabled for default config", func(t *testing.T) {
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		info := db.GetDecayInfo()
		require.NotNil(t, info)
		assert.False(t, info.Enabled)
	})

	t.Run("returns config when decay enabled", func(t *testing.T) {
		config := DefaultConfig()
		config.Memory.DecayEnabled = true
		config.Memory.VisibilityThreshold = 0.1

		db, err := Open("", config)
		require.NoError(t, err)
		defer db.Close()

		info := db.GetDecayInfo()
		require.NotNil(t, info)
		assert.True(t, info.Enabled)
		assert.Equal(t, 0.1, info.VisibilityThreshold)
		assert.Greater(t, info.FlushInterval, time.Duration(0))
	})
}

func TestEscapeCSV(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with,comma", "\"with,comma\""},
		{"with\"quote", "\"with\"\"quote\""},
		{"with\nnewline", "\"with\nnewline\""},
		{"with,comma and \"quotes\"", "\"with,comma and \"\"quotes\"\"\""},
		{"", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := escapeCSV(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestDB_GetEdgesForNode(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	src, err := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "a"})
	require.NoError(t, err)
	dst, err := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "b"})
	require.NoError(t, err)

	_, err = db.CreateEdge(ctx, src.ID, dst.ID, "KNOWS", map[string]interface{}{"since": 2024})
	require.NoError(t, err)

	edges, err := db.GetEdgesForNode(ctx, src.ID)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, src.ID, edges[0].Source)
	require.Equal(t, dst.ID, edges[0].Target)

	_, err = db.GetEdgesForNode(ctx, "")
	require.ErrorIs(t, err, ErrInvalidID)
}

func TestTypedCypherDecodeHelpers(t *testing.T) {
	type personRow struct {
		Name  string `cypher:"name"`
		Age   int    `cypher:"age"`
		Score float64
		Alive bool
	}

	var row personRow
	err := decodeRow([]string{"name", "age", "score", "alive"}, []interface{}{"alice", float64(42), int64(9), true}, &row)
	require.NoError(t, err)
	require.Equal(t, "alice", row.Name)
	require.Equal(t, 42, row.Age)
	require.Equal(t, 9.0, row.Score)
	require.True(t, row.Alive)

	// Map decode path with nested properties.
	row = personRow{}
	err = decodeRow([]string{"n"}, []interface{}{
		map[string]interface{}{
			"properties": map[string]interface{}{"name": "bob", "age": int64(33)},
		},
	}, &row)
	require.NoError(t, err)
	require.Equal(t, "bob", row.Name)
	require.Equal(t, 33, row.Age)

	// Invalid destination.
	err = decodeRow([]string{"name"}, []interface{}{"x"}, row)
	require.Error(t, err)

	// assignValue conversion branches.
	var asInt int
	require.NoError(t, assignValue(reflect.ValueOf(&asInt).Elem(), float64(12)))
	require.Equal(t, 12, asInt)
	var asFloat float64
	require.NoError(t, assignValue(reflect.ValueOf(&asFloat).Elem(), int64(7)))
	require.Equal(t, 7.0, asFloat)
	var asString string
	require.NoError(t, assignValue(reflect.ValueOf(&asString).Elem(), struct{ A int }{A: 123}))
	require.Equal(t, "{123}", asString)
	var asBool bool
	require.NoError(t, assignValue(reflect.ValueOf(&asBool).Elem(), true))
	require.True(t, asBool)
	err = assignValue(reflect.ValueOf(&asBool).Elem(), "not-bool")
	require.Error(t, err)

	// nil value is a no-op.
	asInt = 99
	require.NoError(t, assignValue(reflect.ValueOf(&asInt).Elem(), nil))
	require.Equal(t, 99, asInt)

	// Direct assignable branch.
	type customMap map[string]int
	var cmap customMap
	srcMap := customMap{"a": 1}
	require.NoError(t, assignValue(reflect.ValueOf(&cmap).Elem(), srcMap))
	require.Equal(t, srcMap, cmap)

	// ConvertibleTo branch (named type -> primitive target).
	type myInt int64
	var asI64 int64
	require.NoError(t, assignValue(reflect.ValueOf(&asI64).Elem(), myInt(123)))
	require.Equal(t, int64(123), asI64)

	// Additional numeric kind conversion branch.
	var asInt32 int32
	require.NoError(t, assignValue(reflect.ValueOf(&asInt32).Elem(), int64(44)))
	require.Equal(t, int32(44), asInt32)
}

func TestExecuteCypherTypedAndFirst(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	defer db.Close()

	type row struct {
		Name string `cypher:"name"`
		Age  int    `cypher:"age"`
	}

	_, err = db.ExecuteCypher(context.Background(), "CREATE (:Person {name:'alice', age:42})", nil)
	require.NoError(t, err)

	typed, err := ExecuteCypherTyped[row](db, context.Background(), "MATCH (n:Person) RETURN n.name as name, n.age as age", nil)
	require.NoError(t, err)
	require.NotEmpty(t, typed.Rows)

	first, ok := typed.First()
	require.True(t, ok)
	require.Equal(t, "alice", first.Name)
	require.Equal(t, 42, first.Age)

	empty := &TypedCypherResult[row]{Rows: nil}
	_, ok = empty.First()
	require.False(t, ok)
}

func TestExecuteCypherTyped_DecodeErrorPath(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	defer db.Close()

	type badRow struct {
		Alive bool `cypher:"alive"`
	}

	// String cannot be assigned to bool in assignValue -> ExecuteCypherTyped should return decode error.
	_, err = ExecuteCypherTyped[badRow](db, context.Background(), "RETURN 'not-bool' AS alive", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode row")
}

func TestDecodeRow_MapWithoutPropertiesAndFieldFallbacks(t *testing.T) {
	type mixed struct {
		Name       string `cypher:"name"`
		LegacyCode int
	}

	var out mixed
	err := decodeRow([]string{"n"}, []interface{}{
		map[string]interface{}{
			"name":       "carol",
			"LegacyCode": int64(7), // field-name fallback branch
		},
	}, &out)
	require.NoError(t, err)
	require.Equal(t, "carol", out.Name)
	require.Equal(t, 7, out.LegacyCode)
}

func TestDB_AdminSmallHelpers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.AsyncWritesEnabled = true
	db, err := Open("", cfg)
	require.NoError(t, err)
	defer db.Close()

	require.True(t, db.IsAsyncWritesEnabled())

	// Cached count paths when no search service exists yet.
	require.Equal(t, 0, db.EmbeddingCountCached())
	require.Equal(t, 1024, db.VectorIndexDimensionsCached())

	db.SetAllDatabasesProvider(func() []DatabaseAndStorage { return nil })
	require.NotNil(t, db.allDatabasesProvider)

	stats := db.GetSearchStats()
	// Service should initialize and return stats (non-nil path).
	require.NotNil(t, stats)

	require.NoError(t, db.Close())
	require.Nil(t, db.GetSearchStats(), "closed DB should return nil stats")
}

func TestToStringValue(t *testing.T) {
	require.Equal(t, "", toStringValue(nil))
	require.Equal(t, "abc", toStringValue("abc"))
	require.Equal(t, "42", toStringValue(42))
}

func TestDB_GetSearchStats_WithClusterStatsBranch(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	stats := db.GetSearchStats()
	require.NotNil(t, stats)
	require.True(t, stats.ClusteringEnabled)
	require.GreaterOrEqual(t, stats.NumClusters, 0)
	require.GreaterOrEqual(t, stats.ClusterIterations, 0)
}
