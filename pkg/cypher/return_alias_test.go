package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReturnAliases_WithAggregation(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	exec.Execute(ctx, `CREATE (p1:Person {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (p2:Person {name: 'Bob'})`, nil)
	exec.Execute(ctx, `CREATE (a1:Area {name: 'Engineering'})`, nil)
	exec.Execute(ctx, `CREATE (a2:Area {name: 'Sales'})`, nil)
	exec.Execute(ctx, `CREATE (a3:Area {name: 'Marketing'})`, nil)
	exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (a:Area {name: 'Engineering'}) CREATE (p)-[:WORKS_IN]->(a)`, nil)
	exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (a:Area {name: 'Sales'}) CREATE (p)-[:WORKS_IN]->(a)`, nil)
	exec.Execute(ctx, `MATCH (p:Person {name: 'Bob'}), (a:Area {name: 'Marketing'}) CREATE (p)-[:WORKS_IN]->(a)`, nil)

	t.Run("WITH count and collect with aliases", func(t *testing.T) {
		query := `
			MATCH (p:Person)-[:WORKS_IN]->(a:Area)
			WITH p, count(a) as area_count, collect(a.name) as areas
			RETURN p.name as name, area_count, areas
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		// Check column names respect aliases
		assert.Equal(t, []string{"name", "area_count", "areas"}, result.Columns)

		// Check we have results
		assert.Len(t, result.Rows, 2)
	})

	t.Run("WITH count alias only", func(t *testing.T) {
		query := `
			MATCH (p:Person)-[:WORKS_IN]->(a:Area)
			WITH p, count(a) as area_count
			RETURN p.name as name, area_count
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		assert.Equal(t, []string{"name", "area_count"}, result.Columns)
		assert.Len(t, result.Rows, 2)
	})

	t.Run("simple RETURN with alias", func(t *testing.T) {
		query := `MATCH (p:Person) RETURN p.name as personName`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		assert.Equal(t, []string{"personName"}, result.Columns)
		assert.Len(t, result.Rows, 2)
	})

	t.Run("multiple aliases in RETURN", func(t *testing.T) {
		query := `MATCH (p:Person) RETURN p.name as name, 'constant' as type`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		assert.Equal(t, []string{"name", "type"}, result.Columns)
	})
}

func TestReturnAliases_OptimizedExecutors(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data for optimized executor tests
	exec.Execute(ctx, `CREATE (p1:Person {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (p2:Person {name: 'Bob'})`, nil)
	exec.Execute(ctx, `CREATE (p3:Person {name: 'Charlie'})`, nil)
	exec.Execute(ctx, `MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:FOLLOWS]->(b)`, nil)
	exec.Execute(ctx, `MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Alice'}) CREATE (a)-[:FOLLOWS]->(b)`, nil)
	exec.Execute(ctx, `MATCH (a:Person {name: 'Charlie'}), (b:Person {name: 'Alice'}) CREATE (a)-[:FOLLOWS]->(b)`, nil)

	t.Run("incoming count with alias", func(t *testing.T) {
		query := `MATCH (p:Person)<-[:FOLLOWS]-(follower:Person) RETURN p.name as person, count(follower) as followers`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		assert.Equal(t, []string{"person", "followers"}, result.Columns)
	})

	t.Run("outgoing count with alias", func(t *testing.T) {
		query := `MATCH (p:Person)-[:FOLLOWS]->(following:Person) RETURN p.name as person, count(following) as following_count`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		assert.Equal(t, []string{"person", "following_count"}, result.Columns)
	})

	t.Run("mutual relationship with aliases", func(t *testing.T) {
		query := `MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN a.name as person1, b.name as person2`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		assert.Equal(t, []string{"person1", "person2"}, result.Columns)
	})
}

func TestReturnAliases_WithWhereFilter(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	exec.Execute(ctx, `CREATE (p1:Person {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (p2:Person {name: 'Bob'})`, nil)
	exec.Execute(ctx, `CREATE (a1:Area {name: 'Engineering'})`, nil)
	exec.Execute(ctx, `CREATE (a2:Area {name: 'Sales'})`, nil)
	exec.Execute(ctx, `CREATE (a3:Area {name: 'Marketing'})`, nil)
	exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (a:Area {name: 'Engineering'}) CREATE (p)-[:WORKS_IN]->(a)`, nil)
	exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (a:Area {name: 'Sales'}) CREATE (p)-[:WORKS_IN]->(a)`, nil)
	exec.Execute(ctx, `MATCH (p:Person {name: 'Bob'}), (a:Area {name: 'Marketing'}) CREATE (p)-[:WORKS_IN]->(a)`, nil)

	t.Run("WITH WHERE filter on aggregated value", func(t *testing.T) {
		query := `
			MATCH (p:Person)-[:WORKS_IN]->(a:Area)
			WITH p, count(a) as area_count, collect(a.name) as areas
			WHERE area_count > 1
			RETURN p.name as name, area_count, areas
			LIMIT 5
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		// Check column names
		assert.Equal(t, []string{"name", "area_count", "areas"}, result.Columns)

		// Should only return Alice (2 areas), not Bob (1 area)
		assert.Len(t, result.Rows, 1)
		if len(result.Rows) > 0 {
			assert.Equal(t, "Alice", result.Rows[0][0])
			assert.EqualValues(t, 2, result.Rows[0][1])
		}
	})

	t.Run("WITH WHERE filter area_count >= 1", func(t *testing.T) {
		query := `
			MATCH (p:Person)-[:WORKS_IN]->(a:Area)
			WITH p, count(a) as area_count
			WHERE area_count >= 1
			RETURN p.name as name, area_count
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		assert.Equal(t, []string{"name", "area_count"}, result.Columns)
		assert.Len(t, result.Rows, 2) // Both Alice and Bob
	})
}

func TestExtractReturnItemsFromQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name            string
		query           string
		expectedColumns []string
	}{
		{
			name:            "simple property with alias",
			query:           "MATCH (n) RETURN n.name as name",
			expectedColumns: []string{"name"},
		},
		{
			name:            "multiple items with aliases",
			query:           "MATCH (n) RETURN n.name as name, n.age as age",
			expectedColumns: []string{"name", "age"},
		},
		{
			name:            "count with alias",
			query:           "MATCH (n)-[:REL]->(m) RETURN n.name, count(m) as total",
			expectedColumns: []string{"n.name", "total"},
		},
		{
			name:            "mixed aliases and no aliases",
			query:           "MATCH (n) RETURN n.id, n.name as name, n.age",
			expectedColumns: []string{"n.id", "name", "n.age"},
		},
		{
			name:            "with ORDER BY and LIMIT",
			query:           "MATCH (n) RETURN n.name as name ORDER BY name LIMIT 10",
			expectedColumns: []string{"name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := exec.extractReturnItemsFromQuery(tt.query)
			columns := exec.buildColumnsFromReturnItems(items)
			assert.Equal(t, tt.expectedColumns, columns)
		})
	}
}
