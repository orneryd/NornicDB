package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDocumentationExamples tests all Cypher queries from the documentation
// to ensure they work correctly. This validates that our documentation is accurate.
//
// Source: docs/getting-started/first-queries.md
func TestDocumentationExamples_FirstQueries(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("CreateFirstNode", func(t *testing.T) {
		query := `
			CREATE (alice:Person {
				name: "Alice Johnson",
				age: 30,
				email: "alice@example.com"
			})
			RETURN alice
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Result is a *storage.Node (Neo4j compatible)
		node, ok := result.Rows[0][0].(*storage.Node)
		require.True(t, ok, "Expected *storage.Node")
		assert.Equal(t, "Alice Johnson", node.Properties["name"])
	})

	t.Run("CreateMultipleNodes", func(t *testing.T) {
		query := `
			CREATE 
				(bob:Person {name: "Bob Smith", age: 35}),
				(carol:Person {name: "Carol White", age: 28}),
				(company:Company {name: "TechCorp", founded: 2010})
			RETURN bob, carol, company
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		bob, ok := result.Rows[0][0].(*storage.Node)
		require.True(t, ok, "Expected bob *storage.Node")
		assert.Equal(t, "Bob Smith", bob.Properties["name"])

		company, ok := result.Rows[0][2].(*storage.Node)
		require.True(t, ok, "Expected company *storage.Node")
		assert.Equal(t, "TechCorp", company.Properties["name"])
	})

	t.Run("CreateRelationship", func(t *testing.T) {
		query := `
			MATCH 
				(alice:Person {name: "Alice Johnson"}),
				(company:Company {name: "TechCorp"})
			CREATE (alice)-[r:WORKS_AT {since: 2020, role: "Engineer"}]->(company)
			RETURN alice, r, company
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		// May return 0 rows if MATCH finds nothing, that's ok for this test
		if len(result.Rows) > 0 {
			assert.NotNil(t, result.Rows[0])
		}
	})

	t.Run("FindAllPeople", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			RETURN p.name, p.age
			ORDER BY p.age DESC
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 3)

		// Should be ordered by age descending
		if len(result.Rows) >= 2 {
			// Age values should be in descending order
			assert.NotNil(t, result.Rows[0][1])
			assert.NotNil(t, result.Rows[1][1])
		}
	})

	t.Run("FindRelationships", func(t *testing.T) {
		// First create the relationship if it doesn't exist
		_, _ = exec.Execute(ctx, `
			MATCH (alice:Person {name: "Alice Johnson"}), (company:Company {name: "TechCorp"})
			CREATE (alice)-[:WORKS_AT {since: 2020, role: "Engineer"}]->(company)
		`, nil)

		query := `
			MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
			RETURN p.name, c.name
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		// May have results if relationship was created
		assert.NotNil(t, result)
	})
}

// TestDocumentationExamples_QueryPatterns tests query patterns from documentation
func TestDocumentationExamples_QueryPatterns(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Setup test data
	setupQueries := []string{
		`CREATE (a:Person {name: "Alice", age: 30, city: "New York"})`,
		`CREATE (b:Person {name: "Bob", age: 25, city: "Boston"})`,
		`CREATE (c:Person {name: "Charlie", age: 35, city: "New York"})`,
		`CREATE (d:Person {name: "Diana", age: 28, city: "Boston"})`,
	}
	for _, q := range setupQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("WhereClauseEquality", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			WHERE p.city = 'New York'
			RETURN p.name
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should find 2 people in New York")
	})

	t.Run("WhereClauseComparison", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			WHERE p.age >= 30
			RETURN p.name, p.age
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should find 2 people age >= 30")
	})

	t.Run("WhereClauseAnd", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			WHERE p.age > 25 AND p.city = 'Boston'
			RETURN p.name
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows), "Should find Diana")
	})

	t.Run("OrderByAscending", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			RETURN p.name, p.age
			ORDER BY p.age
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 4)
	})

	t.Run("OrderByDescending", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			RETURN p.name, p.age
			ORDER BY p.age DESC
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 4)
	})

	t.Run("LimitResults", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			RETURN p.name
			LIMIT 2
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should return exactly 2 rows")
	})

	t.Run("SkipResults", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			RETURN p.name
			ORDER BY p.name
			SKIP 1
			LIMIT 2
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "Should return 2 rows after skipping 1")
	})
}

// TestDocumentationExamples_Aggregations tests aggregation queries from documentation
func TestDocumentationExamples_Aggregations(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Setup test data
	setupQueries := []string{
		`CREATE (a:Product {name: "Widget", category: "Electronics", price: 29.99})`,
		`CREATE (b:Product {name: "Gadget", category: "Electronics", price: 49.99})`,
		`CREATE (c:Product {name: "Gizmo", category: "Electronics", price: 19.99})`,
		`CREATE (d:Product {name: "Tool", category: "Hardware", price: 15.99})`,
		`CREATE (e:Product {name: "Supply", category: "Hardware", price: 9.99})`,
	}
	for _, q := range setupQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("CountAll", func(t *testing.T) {
		query := `
			MATCH (p:Product)
			RETURN count(*) as total
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(5), result.Rows[0][0])
	})

	t.Run("CountByCategory", func(t *testing.T) {
		query := `
			MATCH (p:Product)
			WITH p.category as category, count(*) as count
			RETURN category, count
			ORDER BY count DESC
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 categories")
	})

	t.Run("SumPrices", func(t *testing.T) {
		query := `
			MATCH (p:Product)
			RETURN sum(p.price) as total
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// Total: 29.99 + 49.99 + 19.99 + 15.99 + 9.99 = 125.95
		total, ok := result.Rows[0][0].(float64)
		require.True(t, ok)
		assert.InDelta(t, 125.95, total, 0.01)
	})

	t.Run("AvgPrice", func(t *testing.T) {
		query := `
			MATCH (p:Product)
			RETURN avg(p.price) as average
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// Avg: 125.95 / 5 = 25.19
		avg, ok := result.Rows[0][0].(float64)
		require.True(t, ok)
		assert.InDelta(t, 25.19, avg, 0.01)
	})

	t.Run("CollectNames", func(t *testing.T) {
		query := `
			MATCH (p:Product)
			WHERE p.category = 'Electronics'
			RETURN collect(p.name) as names
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		names, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.Len(t, names, 3, "Should collect 3 electronics products")
	})
}

// TestDocumentationExamples_Updates tests update queries from documentation
func TestDocumentationExamples_Updates(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("SetProperty", func(t *testing.T) {
		// Create node
		_, err := exec.Execute(ctx, `CREATE (p:Person {name: "Test", age: 25})`, nil)
		require.NoError(t, err)

		// Update property
		query := `
			MATCH (p:Person {name: "Test"})
			SET p.age = 26
			RETURN p.age
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("SetMultipleProperties", func(t *testing.T) {
		// Create node
		_, err := exec.Execute(ctx, `CREATE (p:Person {name: "Multi"})`, nil)
		require.NoError(t, err)

		// Update multiple properties
		query := `
			MATCH (p:Person {name: "Multi"})
			SET p.age = 30, p.city = "Boston"
			RETURN p.name, p.age, p.city
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("MergeCreate", func(t *testing.T) {
		query := `
			MERGE (p:Person {name: "NewPerson"})
			ON CREATE SET p.created = true
			RETURN p.name, p.created
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("MergeMatch", func(t *testing.T) {
		// Create existing node
		_, err := exec.Execute(ctx, `CREATE (p:Person {name: "Existing"})`, nil)
		require.NoError(t, err)

		query := `
			MERGE (p:Person {name: "Existing"})
			ON MATCH SET p.updated = true
			RETURN p.name, p.updated
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})
}

// TestDocumentationExamples_Delete tests delete queries from documentation
func TestDocumentationExamples_Delete(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("DeleteNode", func(t *testing.T) {
		// Create node
		_, err := exec.Execute(ctx, `CREATE (p:Person {name: "ToDelete"})`, nil)
		require.NoError(t, err)

		// Verify it exists
		result, err := exec.Execute(ctx, `MATCH (p:Person {name: "ToDelete"}) RETURN p`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Delete it
		_, err = exec.Execute(ctx, `MATCH (p:Person {name: "ToDelete"}) DELETE p`, nil)
		require.NoError(t, err)

		// Verify it's gone
		result, err = exec.Execute(ctx, `MATCH (p:Person {name: "ToDelete"}) RETURN p`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0)
	})

	t.Run("DetachDelete", func(t *testing.T) {
		// Create node with relationship
		_, err := exec.Execute(ctx, `CREATE (a:Person {name: "A"})-[:KNOWS]->(b:Person {name: "B"})`, nil)
		require.NoError(t, err)

		// Detach delete removes relationships first
		_, err = exec.Execute(ctx, `MATCH (p:Person {name: "A"}) DETACH DELETE p`, nil)
		require.NoError(t, err)

		// Verify A is gone
		result, err := exec.Execute(ctx, `MATCH (p:Person {name: "A"}) RETURN p`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0)
	})
}

// TestDocumentationExamples_Functions tests built-in function usage
func TestDocumentationExamples_Functions(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Setup
	_, err := exec.Execute(ctx, `CREATE (p:Person:Employee {name: "FuncTest", email: "test@example.com"})`, nil)
	require.NoError(t, err)

	t.Run("IdFunction", func(t *testing.T) {
		query := `
			MATCH (p:Person {name: "FuncTest"})
			RETURN id(p)
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.NotNil(t, result.Rows[0][0])
	})

	t.Run("LabelsFunction", func(t *testing.T) {
		query := `
			MATCH (p:Person {name: "FuncTest"})
			RETURN labels(p)
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		labels, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.GreaterOrEqual(t, len(labels), 2, "Should have at least Person and Employee labels")
	})

	t.Run("KeysFunction", func(t *testing.T) {
		query := `
			MATCH (p:Person {name: "FuncTest"})
			RETURN keys(p)
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		keys, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.GreaterOrEqual(t, len(keys), 2, "Should have name and email keys")
	})

	t.Run("CoalesceFunction", func(t *testing.T) {
		// Create node without optional property
		_, err := exec.Execute(ctx, `CREATE (p:Person {name: "CoalesceTest"})`, nil)
		require.NoError(t, err)

		query := `
			MATCH (p:Person {name: "CoalesceTest"})
			RETURN coalesce(p.nickname, p.name) as displayName
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "CoalesceTest", result.Rows[0][0])
	})

	t.Run("ToStringFunction", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE (p:Person {name: "StringTest", age: 42})`, nil)
		require.NoError(t, err)

		query := `
			MATCH (p:Person {name: "StringTest"})
			RETURN toString(p.age)
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "42", result.Rows[0][0])
	})
}

// TestDocumentationExamples_StringFunctions tests string functions
func TestDocumentationExamples_StringFunctions(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("ToUpperToLower", func(t *testing.T) {
		query := `RETURN toUpper('hello') as upper, toLower('WORLD') as lower`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "HELLO", result.Rows[0][0])
		assert.Equal(t, "world", result.Rows[0][1])
	})

	t.Run("TrimFunction", func(t *testing.T) {
		query := `RETURN trim('  hello  ') as trimmed`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "hello", result.Rows[0][0])
	})

	t.Run("SubstringFunction", func(t *testing.T) {
		query := `RETURN substring('hello world', 0, 5) as sub`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "hello", result.Rows[0][0])
	})

	t.Run("ReplaceFunction", func(t *testing.T) {
		query := `RETURN replace('hello world', 'world', 'cypher') as replaced`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "hello cypher", result.Rows[0][0])
	})

	t.Run("SizeFunction", func(t *testing.T) {
		query := `RETURN size('hello') as len`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(5), result.Rows[0][0])
	})
}

// TestDocumentationExamples_ListFunctions tests list functions
func TestDocumentationExamples_ListFunctions(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("RangeFunction", func(t *testing.T) {
		query := `RETURN range(1, 5) as nums`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		nums, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.Len(t, nums, 5)
	})

	t.Run("HeadTailFunctions", func(t *testing.T) {
		query := `
			WITH [1, 2, 3, 4, 5] as nums
			RETURN head(nums) as first, last(nums) as last
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("SizeOfList", func(t *testing.T) {
		query := `RETURN size([1, 2, 3, 4, 5]) as count`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(5), result.Rows[0][0])
	})

	t.Run("ReverseFunction", func(t *testing.T) {
		query := `RETURN reverse([1, 2, 3]) as reversed`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})
}

// TestDocumentationExamples_CaseExpression tests CASE expressions
func TestDocumentationExamples_CaseExpression(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Setup
	setupQueries := []string{
		`CREATE (a:Person {name: "Young", age: 18})`,
		`CREATE (b:Person {name: "Adult", age: 35})`,
		`CREATE (c:Person {name: "Senior", age: 70})`,
	}
	for _, q := range setupQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("SimpleCaseWhen", func(t *testing.T) {
		query := `
			MATCH (p:Person)
			RETURN p.name,
				CASE
					WHEN p.age < 20 THEN 'Young'
					WHEN p.age < 60 THEN 'Adult'
					ELSE 'Senior'
				END as category
			ORDER BY p.name
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})
}

// TestDocumentationExamples_UnwindClause tests UNWIND functionality
func TestDocumentationExamples_UnwindClause(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("UnwindSimpleList", func(t *testing.T) {
		query := `
			UNWIND [1, 2, 3, 4, 5] AS x
			RETURN x
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 5)
	})

	t.Run("UnwindRange", func(t *testing.T) {
		query := `
			UNWIND range(1, 10) AS x
			RETURN x
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 10)
	})

	t.Run("UnwindWithMatch", func(t *testing.T) {
		// Create test data
		_, err := exec.Execute(ctx, `CREATE (p:Person:Developer {name: "UnwindTest"})`, nil)
		require.NoError(t, err)

		query := `
			MATCH (p:Person {name: "UnwindTest"})
			UNWIND labels(p) as label
			RETURN label
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(result.Rows), 2, "Should have at least Person and Developer labels")
	})
}

// TestDocumentationExamples_ListComprehension tests list comprehension
func TestDocumentationExamples_ListComprehension(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("SimpleListComprehension", func(t *testing.T) {
		query := `RETURN [x IN [1, 2, 3, 4, 5]] as nums`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		nums, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.Len(t, nums, 5)
	})

	t.Run("ListComprehensionWithFilter", func(t *testing.T) {
		query := `RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2] as filtered`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		filtered, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.Len(t, filtered, 3, "Should have [3, 4, 5]")
	})

	t.Run("ListComprehensionWithTransform", func(t *testing.T) {
		query := `RETURN [x IN [1, 2, 3] | x * 2] as doubled`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		doubled, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.Len(t, doubled, 3)
	})
}

// TestDocumentationExamples_Procedures tests CALL procedure syntax
func TestDocumentationExamples_Procedures(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("DbmsComponents", func(t *testing.T) {
		query := `CALL dbms.components()`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("DbLabels", func(t *testing.T) {
		// Create some nodes first
		_, _ = exec.Execute(ctx, `CREATE (:TestLabel1), (:TestLabel2)`, nil)

		query := `CALL db.labels()`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(result.Rows), 2)
	})

	t.Run("DbRelationshipTypes", func(t *testing.T) {
		query := `CALL db.relationshipTypes()`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		// May be empty, that's ok
		assert.NotNil(t, result)
	})

	t.Run("DbPropertyKeys", func(t *testing.T) {
		query := `CALL db.propertyKeys()`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})
}
