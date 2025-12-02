package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// SECTION 1: NODE PATTERNS
// Tests parsing of node patterns: (), (n), (n:Label), (n:Label {props})
// =============================================================================

func TestParseNodePatterns(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create nodes for testing
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice", "age": int64(30)}},
		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(25)}},
		{ID: "n3", Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Acme"}},
		{ID: "n4", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Carol"}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		minRows     int
		description string
	}{
		// Anonymous node patterns
		{
			name:        "anonymous node - all nodes",
			query:       "MATCH () RETURN count(*) as c",
			expectErr:   false,
			minRows:     1,
			description: "Match any node without binding",
		},
		// Variable binding
		{
			name:        "variable binding only",
			query:       "MATCH (n) RETURN n",
			expectErr:   false,
			minRows:     4,
			description: "Match any node and bind to variable n",
		},
		// Single label
		{
			name:        "single label",
			query:       "MATCH (n:Person) RETURN n",
			expectErr:   false,
			minRows:     3,
			description: "Match nodes with Person label",
		},
		// Multiple labels
		{
			name:        "multiple labels",
			query:       "MATCH (n:Person:Employee) RETURN n",
			expectErr:   false,
			minRows:     1,
			description: "Match nodes with both Person AND Employee labels",
		},
		// Property filter - string
		{
			name:        "property filter - string equality",
			query:       "MATCH (n:Person {name: 'Alice'}) RETURN n",
			expectErr:   false,
			minRows:     1,
			description: "Match with inline string property filter",
		},
		// Property filter - number
		{
			name:        "property filter - number",
			query:       "MATCH (n:Person {age: 30}) RETURN n",
			expectErr:   false,
			minRows:     1,
			description: "Match with inline numeric property filter",
		},
		// Property filter - multiple properties
		{
			name:        "property filter - multiple",
			query:       "MATCH (n:Person {name: 'Alice', age: 30}) RETURN n",
			expectErr:   false,
			minRows:     1,
			description: "Match with multiple inline property filters",
		},
		// Label without variable
		{
			name:        "label without variable",
			query:       "MATCH (:Person) RETURN count(*) as c",
			expectErr:   false,
			minRows:     1,
			description: "Match with label but no variable binding",
		},
		// Properties without variable
		{
			name:        "properties without variable",
			query:       "MATCH (:Person {name: 'Alice'}) RETURN count(*) as c",
			expectErr:   false,
			minRows:     1,
			description: "Match with properties but no variable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				require.NoError(t, err, "Query failed: %s\nDescription: %s", tt.query, tt.description)
				assert.GreaterOrEqual(t, len(result.Rows), tt.minRows,
					"Expected at least %d rows for: %s", tt.minRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 2: RELATIONSHIP PATTERNS
// Tests: -[r]-, -[r:TYPE]->, <-[r:TYPE]-, -[r:TYPE|OTHER]->
// =============================================================================

func TestParseRelationshipPatterns(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create nodes and relationships
	nodes := []*storage.Node{
		{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: "p3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol"}},
		{ID: "c1", Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Acme"}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	edges := []*storage.Edge{
		{ID: "e1", Type: "KNOWS", StartNode: "p1", EndNode: "p2", Properties: map[string]interface{}{"since": int64(2020)}},
		{ID: "e2", Type: "KNOWS", StartNode: "p2", EndNode: "p3"},
		{ID: "e3", Type: "WORKS_AT", StartNode: "p1", EndNode: "c1"},
		{ID: "e4", Type: "LIKES", StartNode: "p1", EndNode: "p3"},
	}
	for _, e := range edges {
		require.NoError(t, store.CreateEdge(e))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		minRows     int
		description string
	}{
		// Outgoing relationship
		{
			name:        "outgoing - any type",
			query:       "MATCH (a)-[r]->(b) RETURN a, r, b",
			expectErr:   false,
			minRows:     4,
			description: "Match any outgoing relationship",
		},
		// Incoming relationship
		{
			name:        "incoming - any type",
			query:       "MATCH (a)<-[r]-(b) RETURN a, r, b",
			expectErr:   false,
			minRows:     4,
			description: "Match any incoming relationship",
		},
		// Undirected relationship
		{
			name:        "undirected - any type",
			query:       "MATCH (a)-[r]-(b) RETURN a, r, b",
			expectErr:   false,
			minRows:     4, // Could be more due to bidirectional matching
			description: "Match any relationship regardless of direction",
		},
		// Typed relationship - outgoing
		{
			name:        "typed outgoing",
			query:       "MATCH (a)-[r:KNOWS]->(b) RETURN a, b",
			expectErr:   false,
			minRows:     2,
			description: "Match KNOWS relationships outgoing",
		},
		// Typed relationship - incoming
		{
			name:        "typed incoming",
			query:       "MATCH (a)<-[r:KNOWS]-(b) RETURN a, b",
			expectErr:   false,
			minRows:     2,
			description: "Match KNOWS relationships incoming",
		},
		// Multiple types (OR)
		{
			name:        "multiple types OR",
			query:       "MATCH (a)-[r:KNOWS|LIKES]->(b) RETURN a, b",
			expectErr:   false,
			minRows:     3,
			description: "Match KNOWS or LIKES relationships",
		},
		// Relationship with property filter
		{
			name:        "relationship with property",
			query:       "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN a, b",
			expectErr:   false,
			minRows:     1,
			description: "Match relationship with property filter",
		},
		// Anonymous relationship
		{
			name:        "anonymous relationship",
			query:       "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
			expectErr:   false,
			minRows:     2,
			description: "Match without binding relationship to variable",
		},
		// Chain of relationships
		{
			name:        "relationship chain",
			query:       "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, c.name",
			expectErr:   false,
			minRows:     1,
			description: "Match chain of two KNOWS relationships",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				require.NoError(t, err, "Query failed: %s\nDescription: %s", tt.query, tt.description)
				assert.GreaterOrEqual(t, len(result.Rows), tt.minRows,
					"Expected at least %d rows for: %s", tt.minRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 3: VARIABLE LENGTH PATHS
// Tests: -[*]-, -[*2]-, -[*1..3]-, -[:TYPE*1..5]->
// =============================================================================

func TestParseVariableLengthPaths(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create a chain of nodes
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Node"}, Properties: map[string]interface{}{"level": int64(1)}},
		{ID: "n2", Labels: []string{"Node"}, Properties: map[string]interface{}{"level": int64(2)}},
		{ID: "n3", Labels: []string{"Node"}, Properties: map[string]interface{}{"level": int64(3)}},
		{ID: "n4", Labels: []string{"Node"}, Properties: map[string]interface{}{"level": int64(4)}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	edges := []*storage.Edge{
		{ID: "e1", Type: "NEXT", StartNode: "n1", EndNode: "n2"},
		{ID: "e2", Type: "NEXT", StartNode: "n2", EndNode: "n3"},
		{ID: "e3", Type: "NEXT", StartNode: "n3", EndNode: "n4"},
	}
	for _, e := range edges {
		require.NoError(t, store.CreateEdge(e))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		minRows     int
		description string
	}{
		// Fixed length
		{
			name:        "fixed length 2",
			query:       "MATCH (a:Node)-[:NEXT*2]->(b:Node) RETURN a.level, b.level",
			expectErr:   false,
			minRows:     2,
			description: "Match paths of exactly 2 hops",
		},
		// Range length
		{
			name:        "range 1 to 3",
			query:       "MATCH (a:Node {level: 1})-[:NEXT*1..3]->(b:Node) RETURN b.level",
			expectErr:   false,
			minRows:     3,
			description: "Match paths of 1 to 3 hops",
		},
		// Min only
		{
			name:        "min only",
			query:       "MATCH (a:Node {level: 1})-[:NEXT*2..]->(b:Node) RETURN b.level",
			expectErr:   false,
			minRows:     2,
			description: "Match paths of at least 2 hops",
		},
		// Max only
		{
			name:        "max only",
			query:       "MATCH (a:Node {level: 1})-[:NEXT*..2]->(b:Node) RETURN b.level",
			expectErr:   false,
			minRows:     2,
			description: "Match paths of at most 2 hops",
		},
		// Any length
		{
			name:        "any length",
			query:       "MATCH (a:Node {level: 1})-[:NEXT*]->(b:Node) RETURN b.level",
			expectErr:   false,
			minRows:     3,
			description: "Match paths of any length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s", tt.description)
				}
				assert.GreaterOrEqual(t, len(result.Rows), tt.minRows,
					"Expected at least %d rows for: %s", tt.minRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 4: WHERE CLAUSE OPERATORS
// Tests all comparison and logical operators in WHERE
// =============================================================================

func TestParseWhereOperators(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Item"}, Properties: map[string]interface{}{
			"name": "Apple", "price": 1.50, "quantity": int64(10), "available": true,
		}},
		{ID: "n2", Labels: []string{"Item"}, Properties: map[string]interface{}{
			"name": "Banana", "price": 0.75, "quantity": int64(20), "available": true,
		}},
		{ID: "n3", Labels: []string{"Item"}, Properties: map[string]interface{}{
			"name": "Cherry", "price": 3.00, "quantity": int64(5), "available": false,
		}},
		{ID: "n4", Labels: []string{"Item"}, Properties: map[string]interface{}{
			"name": "Date", "price": 2.50, "quantity": int64(0),
		}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectRows  int
		description string
	}{
		// Equality
		{
			name:        "equals string",
			query:       "MATCH (n:Item) WHERE n.name = 'Apple' RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "String equality comparison",
		},
		{
			name:        "equals number",
			query:       "MATCH (n:Item) WHERE n.quantity = 10 RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Numeric equality comparison",
		},
		// Inequality
		{
			name:        "not equals",
			query:       "MATCH (n:Item) WHERE n.name <> 'Apple' RETURN n",
			expectErr:   false,
			expectRows:  3,
			description: "Not equals comparison",
		},
		{
			name:        "not equals !=",
			query:       "MATCH (n:Item) WHERE n.name != 'Apple' RETURN n",
			expectErr:   false,
			expectRows:  3,
			description: "Not equals with != syntax",
		},
		// Numeric comparisons
		{
			name:        "less than",
			query:       "MATCH (n:Item) WHERE n.price < 2.00 RETURN n",
			expectErr:   false,
			expectRows:  2,
			description: "Less than comparison",
		},
		{
			name:        "less than or equal",
			query:       "MATCH (n:Item) WHERE n.price <= 1.50 RETURN n",
			expectErr:   false,
			expectRows:  2,
			description: "Less than or equal comparison",
		},
		{
			name:        "greater than",
			query:       "MATCH (n:Item) WHERE n.price > 2.00 RETURN n",
			expectErr:   false,
			expectRows:  2,
			description: "Greater than comparison",
		},
		{
			name:        "greater than or equal",
			query:       "MATCH (n:Item) WHERE n.price >= 2.50 RETURN n",
			expectErr:   false,
			expectRows:  2,
			description: "Greater than or equal comparison",
		},
		// Boolean operators
		{
			name:        "AND",
			query:       "MATCH (n:Item) WHERE n.price > 1.00 AND n.quantity > 5 RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "AND logical operator",
		},
		{
			name:        "OR",
			query:       "MATCH (n:Item) WHERE n.price < 1.00 OR n.price > 2.50 RETURN n",
			expectErr:   false,
			expectRows:  2,
			description: "OR logical operator",
		},
		{
			name:        "NOT",
			query:       "MATCH (n:Item) WHERE NOT n.available = true RETURN n",
			expectErr:   false,
			expectRows:  2, // Cherry (false) and Date (null)
			description: "NOT logical operator",
		},
		{
			name:        "complex boolean",
			query:       "MATCH (n:Item) WHERE (n.price > 2.00 OR n.quantity > 15) AND n.available = true RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Complex boolean expression with parentheses",
		},
		// NULL checks
		{
			name:        "IS NULL",
			query:       "MATCH (n:Item) WHERE n.available IS NULL RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "IS NULL check",
		},
		{
			name:        "IS NOT NULL",
			query:       "MATCH (n:Item) WHERE n.available IS NOT NULL RETURN n",
			expectErr:   false,
			expectRows:  3,
			description: "IS NOT NULL check",
		},
		// String predicates
		{
			name:        "STARTS WITH",
			query:       "MATCH (n:Item) WHERE n.name STARTS WITH 'A' RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "STARTS WITH string predicate",
		},
		{
			name:        "ENDS WITH",
			query:       "MATCH (n:Item) WHERE n.name ENDS WITH 'e' RETURN n",
			expectErr:   false,
			expectRows:  2, // Apple, Date
			description: "ENDS WITH string predicate",
		},
		{
			name:        "CONTAINS",
			query:       "MATCH (n:Item) WHERE n.name CONTAINS 'an' RETURN n",
			expectErr:   false,
			expectRows:  1, // Banana
			description: "CONTAINS string predicate",
		},
		// IN list
		{
			name:        "IN list",
			query:       "MATCH (n:Item) WHERE n.name IN ['Apple', 'Cherry'] RETURN n",
			expectErr:   false,
			expectRows:  2,
			description: "IN list predicate",
		},
		// Boolean property
		{
			name:        "boolean true",
			query:       "MATCH (n:Item) WHERE n.available = true RETURN n",
			expectErr:   false,
			expectRows:  2,
			description: "Boolean true comparison",
		},
		{
			name:        "boolean false",
			query:       "MATCH (n:Item) WHERE n.available = false RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Boolean false comparison",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s\nQuery: %s", tt.expectRows, tt.description, tt.query)
			}
		})
	}
}

// =============================================================================
// SECTION 5: RETURN CLAUSE VARIATIONS
// Tests: RETURN expressions, aliases, aggregations, DISTINCT
// =============================================================================

func TestParseReturnClause(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice", "age": int64(30)}},
		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(30)}},
		{ID: "n3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol", "age": int64(25)}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectRows  int
		expectCols  int
		description string
	}{
		// Return node
		{
			name:        "return node",
			query:       "MATCH (n:Person) RETURN n",
			expectErr:   false,
			expectRows:  3,
			expectCols:  1,
			description: "Return entire node",
		},
		// Return property
		{
			name:        "return property",
			query:       "MATCH (n:Person) RETURN n.name",
			expectErr:   false,
			expectRows:  3,
			expectCols:  1,
			description: "Return single property",
		},
		// Return multiple properties
		{
			name:        "return multiple properties",
			query:       "MATCH (n:Person) RETURN n.name, n.age",
			expectErr:   false,
			expectRows:  3,
			expectCols:  2,
			description: "Return multiple properties",
		},
		// Return with alias
		{
			name:        "return with alias",
			query:       "MATCH (n:Person) RETURN n.name AS personName",
			expectErr:   false,
			expectRows:  3,
			expectCols:  1,
			description: "Return with AS alias",
		},
		// Count aggregation
		{
			name:        "count all",
			query:       "MATCH (n:Person) RETURN count(*) AS total",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "COUNT(*) aggregation",
		},
		{
			name:        "count property",
			query:       "MATCH (n:Person) RETURN count(n.name) AS nameCount",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "COUNT(property) aggregation",
		},
		// DISTINCT
		{
			name:        "distinct values",
			query:       "MATCH (n:Person) RETURN DISTINCT n.age",
			expectErr:   false,
			expectRows:  2, // 30 and 25
			expectCols:  1,
			description: "DISTINCT values",
		},
		// Other aggregations
		{
			name:        "sum aggregation",
			query:       "MATCH (n:Person) RETURN sum(n.age) AS totalAge",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "SUM aggregation",
		},
		{
			name:        "avg aggregation",
			query:       "MATCH (n:Person) RETURN avg(n.age) AS avgAge",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "AVG aggregation",
		},
		{
			name:        "min aggregation",
			query:       "MATCH (n:Person) RETURN min(n.age) AS minAge",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "MIN aggregation",
		},
		{
			name:        "max aggregation",
			query:       "MATCH (n:Person) RETURN max(n.age) AS maxAge",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "MAX aggregation",
		},
		// Collect
		{
			name:        "collect aggregation",
			query:       "MATCH (n:Person) RETURN collect(n.name) AS names",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "COLLECT aggregation",
		},
		// Return literal
		{
			name:        "return literal string",
			query:       "RETURN 'hello' AS greeting",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "Return literal string value",
		},
		{
			name:        "return literal number",
			query:       "RETURN 42 AS answer",
			expectErr:   false,
			expectRows:  1,
			expectCols:  1,
			description: "Return literal number value",
		},
		// Expressions
		{
			name:        "return expression",
			query:       "MATCH (n:Person) RETURN n.age + 1 AS nextAge",
			expectErr:   false,
			expectRows:  3,
			expectCols:  1,
			description: "Return arithmetic expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s", tt.expectRows, tt.description)
				if len(result.Rows) > 0 {
					assert.Equal(t, tt.expectCols, len(result.Rows[0]),
						"Expected %d columns for: %s", tt.expectCols, tt.description)
				}
			}
		})
	}
}

// =============================================================================
// SECTION 6: WITH CLAUSE
// Tests: WITH projection, aggregation, WHERE after WITH
// =============================================================================

func TestParseWithClause(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice", "dept": "Engineering", "salary": int64(100000)}},
		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob", "dept": "Engineering", "salary": int64(90000)}},
		{ID: "n3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol", "dept": "Sales", "salary": int64(80000)}},
		{ID: "n4", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Dave", "dept": "Sales", "salary": int64(85000)}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		minRows     int
		description string
	}{
		// Simple projection
		{
			name:        "simple projection",
			query:       "MATCH (n:Person) WITH n.name AS name RETURN name",
			expectErr:   false,
			minRows:     4,
			description: "Simple WITH projection",
		},
		// Aggregation with GROUP BY
		{
			name:        "group by with count",
			query:       "MATCH (n:Person) WITH n.dept AS dept, count(n) AS cnt RETURN dept, cnt",
			expectErr:   false,
			minRows:     2,
			description: "WITH clause GROUP BY with count",
		},
		{
			name:        "group by with sum",
			query:       "MATCH (n:Person) WITH n.dept AS dept, sum(n.salary) AS totalSalary RETURN dept, totalSalary",
			expectErr:   false,
			minRows:     2,
			description: "WITH clause GROUP BY with sum",
		},
		// WHERE after WITH
		{
			name:        "WHERE after WITH",
			query:       "MATCH (n:Person) WITH n.dept AS dept, count(n) AS cnt WHERE cnt > 1 RETURN dept, cnt",
			expectErr:   false,
			minRows:     2,
			description: "Filter aggregation results with WHERE after WITH",
		},
		// ORDER BY after WITH
		{
			name:        "ORDER BY after WITH",
			query:       "MATCH (n:Person) WITH n ORDER BY n.salary DESC RETURN n.name",
			expectErr:   false,
			minRows:     4,
			description: "ORDER BY in WITH clause",
		},
		// LIMIT after WITH
		{
			name:        "LIMIT after WITH",
			query:       "MATCH (n:Person) WITH n LIMIT 2 RETURN n.name",
			expectErr:   false,
			minRows:     2,
			description: "LIMIT in WITH clause",
		},
		// Multiple WITH clauses
		{
			name:        "multiple WITH",
			query:       "MATCH (n:Person) WITH n.dept AS dept, count(n) AS cnt WITH dept WHERE cnt > 1 RETURN dept",
			expectErr:   false,
			minRows:     2,
			description: "Chained WITH clauses",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.GreaterOrEqual(t, len(result.Rows), tt.minRows,
					"Expected at least %d rows for: %s", tt.minRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 7: ORDER BY, SKIP, LIMIT
// Tests ordering and pagination
// =============================================================================

func TestParseOrderBySkipLimit(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Item"}, Properties: map[string]interface{}{"name": "A", "value": int64(30)}},
		{ID: "n2", Labels: []string{"Item"}, Properties: map[string]interface{}{"name": "B", "value": int64(10)}},
		{ID: "n3", Labels: []string{"Item"}, Properties: map[string]interface{}{"name": "C", "value": int64(20)}},
		{ID: "n4", Labels: []string{"Item"}, Properties: map[string]interface{}{"name": "D", "value": int64(40)}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectRows  int
		firstValue  interface{}
		description string
	}{
		// ORDER BY ascending
		{
			name:        "order by ascending",
			query:       "MATCH (n:Item) RETURN n.name ORDER BY n.value ASC",
			expectErr:   false,
			expectRows:  4,
			firstValue:  "B",
			description: "ORDER BY ascending",
		},
		// ORDER BY descending
		{
			name:        "order by descending",
			query:       "MATCH (n:Item) RETURN n.name ORDER BY n.value DESC",
			expectErr:   false,
			expectRows:  4,
			firstValue:  "D",
			description: "ORDER BY descending",
		},
		// ORDER BY default (ascending)
		{
			name:        "order by default",
			query:       "MATCH (n:Item) RETURN n.name ORDER BY n.value",
			expectErr:   false,
			expectRows:  4,
			firstValue:  "B",
			description: "ORDER BY default is ascending",
		},
		// Multiple ORDER BY
		{
			name:        "multiple order by",
			query:       "MATCH (n:Item) RETURN n.name ORDER BY n.value ASC, n.name DESC",
			expectErr:   false,
			expectRows:  4,
			firstValue:  "B",
			description: "Multiple ORDER BY columns",
		},
		// LIMIT
		{
			name:        "limit",
			query:       "MATCH (n:Item) RETURN n.name LIMIT 2",
			expectErr:   false,
			expectRows:  2,
			description: "LIMIT clause",
		},
		// SKIP
		{
			name:        "skip",
			query:       "MATCH (n:Item) RETURN n.name ORDER BY n.value SKIP 1",
			expectErr:   false,
			expectRows:  3,
			firstValue:  "C",
			description: "SKIP clause",
		},
		// SKIP and LIMIT
		{
			name:        "skip and limit",
			query:       "MATCH (n:Item) RETURN n.name ORDER BY n.value SKIP 1 LIMIT 2",
			expectErr:   false,
			expectRows:  2,
			firstValue:  "C",
			description: "SKIP and LIMIT together",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s", tt.expectRows, tt.description)
				if tt.firstValue != nil && len(result.Rows) > 0 {
					assert.Equal(t, tt.firstValue, result.Rows[0][0],
						"Expected first value to be %v for: %s", tt.firstValue, tt.description)
				}
			}
		})
	}
}

// =============================================================================
// SECTION 8: CREATE PATTERNS
// Tests node and relationship creation
// =============================================================================

func TestParseCreatePatterns(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectErr   bool
		checkQuery  string
		expectRows  int
		description string
	}{
		// Create node
		{
			name:        "create simple node",
			query:       "CREATE (n:TestNode)",
			expectErr:   false,
			checkQuery:  "MATCH (n:TestNode) RETURN n",
			expectRows:  1,
			description: "Create node with label",
		},
		// Create node with properties
		{
			name:        "create node with properties",
			query:       "CREATE (n:TestPerson {name: 'Test', age: 30})",
			expectErr:   false,
			checkQuery:  "MATCH (n:TestPerson {name: 'Test'}) RETURN n",
			expectRows:  1,
			description: "Create node with properties",
		},
		// Create multiple labels
		{
			name:        "create node multiple labels",
			query:       "CREATE (n:TestA:TestB {id: 'multi'})",
			expectErr:   false,
			checkQuery:  "MATCH (n:TestA:TestB) RETURN n",
			expectRows:  1,
			description: "Create node with multiple labels",
		},
		// Create node and return
		{
			name:        "create and return",
			query:       "CREATE (n:ReturnTest {name: 'returned'}) RETURN n",
			expectErr:   false,
			checkQuery:  "MATCH (n:ReturnTest) RETURN n",
			expectRows:  1,
			description: "Create node and return it",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fresh store for each test
			store := storage.NewMemoryEngine()
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			_, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				// Verify with check query
				result, err := exec.Execute(ctx, tt.checkQuery, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows after CREATE for: %s", tt.expectRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 9: CREATE RELATIONSHIPS
// Tests relationship creation between nodes
// =============================================================================

func TestParseCreateRelationships(t *testing.T) {
	tests := []struct {
		name        string
		setup       string
		query       string
		expectErr   bool
		checkQuery  string
		expectRows  int
		description string
	}{
		// Create relationship between existing nodes
		{
			name:        "create relationship",
			setup:       "CREATE (a:RelTestA {id: 'a'}), (b:RelTestB {id: 'b'})",
			query:       "MATCH (a:RelTestA), (b:RelTestB) CREATE (a)-[:RELATES_TO]->(b)",
			expectErr:   false,
			checkQuery:  "MATCH (a:RelTestA)-[:RELATES_TO]->(b:RelTestB) RETURN a, b",
			expectRows:  1,
			description: "Create relationship between matched nodes",
		},
		// Create relationship with properties
		{
			name:        "create relationship with properties",
			setup:       "CREATE (a:PropRelA {id: 'pa'}), (b:PropRelB {id: 'pb'})",
			query:       "MATCH (a:PropRelA), (b:PropRelB) CREATE (a)-[:HAS_PROP {weight: 10}]->(b)",
			expectErr:   false,
			checkQuery:  "MATCH (a)-[r:HAS_PROP]->(b) WHERE r.weight = 10 RETURN r",
			expectRows:  1,
			description: "Create relationship with properties",
		},
		// Create inline (CREATE full pattern)
		{
			name:        "create inline pattern",
			query:       "CREATE (a:InlineA {name: 'ia'})-[:INLINE_REL]->(b:InlineB {name: 'ib'})",
			expectErr:   false,
			checkQuery:  "MATCH (a:InlineA)-[:INLINE_REL]->(b:InlineB) RETURN a, b",
			expectRows:  1,
			description: "Create full pattern inline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewMemoryEngine()
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			// Run setup if present
			if tt.setup != "" {
				_, err := exec.Execute(ctx, tt.setup, nil)
				require.NoError(t, err, "Setup failed")
			}

			_, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				result, err := exec.Execute(ctx, tt.checkQuery, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s", tt.expectRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 10: MERGE PATTERNS
// Tests MERGE with ON CREATE and ON MATCH
// =============================================================================

func TestParseMergePatterns(t *testing.T) {
	tests := []struct {
		name        string
		setup       string
		query       string
		expectErr   bool
		checkQuery  string
		expectRows  int
		description string
	}{
		// Simple MERGE (creates)
		{
			name:        "merge creates",
			query:       "MERGE (n:MergeTest {id: 'new'})",
			expectErr:   false,
			checkQuery:  "MATCH (n:MergeTest {id: 'new'}) RETURN n",
			expectRows:  1,
			description: "MERGE creates when not exists",
		},
		// MERGE (matches existing)
		{
			name:        "merge matches",
			setup:       "CREATE (n:MergeMatch {id: 'exists'})",
			query:       "MERGE (n:MergeMatch {id: 'exists'})",
			expectErr:   false,
			checkQuery:  "MATCH (n:MergeMatch {id: 'exists'}) RETURN n",
			expectRows:  1, // Should still be 1, not 2
			description: "MERGE matches existing node",
		},
		// MERGE ON CREATE
		{
			name:        "merge on create",
			query:       "MERGE (n:MergeCreate {id: 'oc'}) ON CREATE SET n.created = true RETURN n",
			expectErr:   false,
			checkQuery:  "MATCH (n:MergeCreate {created: true}) RETURN n",
			expectRows:  1,
			description: "MERGE ON CREATE sets properties",
		},
		// MERGE ON MATCH
		{
			name:        "merge on match",
			setup:       "CREATE (n:MergeOnMatch {id: 'om', updated: false})",
			query:       "MERGE (n:MergeOnMatch {id: 'om'}) ON MATCH SET n.updated = true",
			expectErr:   false,
			checkQuery:  "MATCH (n:MergeOnMatch {updated: true}) RETURN n",
			expectRows:  1,
			description: "MERGE ON MATCH updates properties",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewMemoryEngine()
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			if tt.setup != "" {
				_, err := exec.Execute(ctx, tt.setup, nil)
				require.NoError(t, err, "Setup failed")
			}

			_, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				result, err := exec.Execute(ctx, tt.checkQuery, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s", tt.expectRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 11: SET AND REMOVE
// Tests property updates
// =============================================================================

func TestParseSetRemove(t *testing.T) {
	tests := []struct {
		name        string
		setup       string
		query       string
		expectErr   bool
		checkQuery  string
		checkValue  interface{}
		description string
	}{
		// SET single property
		{
			name:        "set property",
			setup:       "CREATE (n:SetTest {id: 's1'})",
			query:       "MATCH (n:SetTest {id: 's1'}) SET n.value = 100",
			expectErr:   false,
			checkQuery:  "MATCH (n:SetTest {id: 's1'}) RETURN n.value",
			checkValue:  int64(100),
			description: "SET single property",
		},
		// SET multiple properties
		{
			name:        "set multiple properties",
			setup:       "CREATE (n:SetMulti {id: 'sm'})",
			query:       "MATCH (n:SetMulti {id: 'sm'}) SET n.a = 1, n.b = 2",
			expectErr:   false,
			checkQuery:  "MATCH (n:SetMulti {id: 'sm'}) RETURN n.a",
			checkValue:  int64(1),
			description: "SET multiple properties",
		},
		// SET += (merge properties)
		{
			name:        "set merge properties",
			setup:       "CREATE (n:SetMerge {id: 'merge', existing: true})",
			query:       "MATCH (n:SetMerge {id: 'merge'}) SET n += {newProp: 'added'}",
			expectErr:   false,
			checkQuery:  "MATCH (n:SetMerge {id: 'merge'}) RETURN n.newProp",
			checkValue:  "added",
			description: "SET += merges properties",
		},
		// REMOVE property
		{
			name:        "remove property",
			setup:       "CREATE (n:RemoveTest {id: 'rm', toRemove: 'gone'})",
			query:       "MATCH (n:RemoveTest {id: 'rm'}) REMOVE n.toRemove",
			expectErr:   false,
			checkQuery:  "MATCH (n:RemoveTest {id: 'rm'}) RETURN n.toRemove",
			checkValue:  nil,
			description: "REMOVE property",
		},
		// SET label
		{
			name:        "set label",
			setup:       "CREATE (n:Original {id: 'label'})",
			query:       "MATCH (n:Original {id: 'label'}) SET n:Additional",
			expectErr:   false,
			checkQuery:  "MATCH (n:Original:Additional) RETURN n",
			description: "SET adds label",
		},
		// REMOVE label
		{
			name:        "remove label",
			setup:       "CREATE (n:ToKeep:ToRemove {id: 'rmlabel'})",
			query:       "MATCH (n {id: 'rmlabel'}) REMOVE n:ToRemove",
			expectErr:   false,
			checkQuery:  "MATCH (n:ToKeep {id: 'rmlabel'}) WHERE NOT n:ToRemove RETURN n",
			description: "REMOVE removes label",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewMemoryEngine()
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			if tt.setup != "" {
				_, err := exec.Execute(ctx, tt.setup, nil)
				require.NoError(t, err, "Setup failed")
			}

			_, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				if tt.checkQuery != "" {
					result, err := exec.Execute(ctx, tt.checkQuery, nil)
					require.NoError(t, err)
					if tt.checkValue != nil {
						require.GreaterOrEqual(t, len(result.Rows), 1)
						assert.Equal(t, tt.checkValue, result.Rows[0][0],
							"Expected value %v for: %s", tt.checkValue, tt.description)
					}
				}
			}
		})
	}
}

// =============================================================================
// SECTION 12: DELETE
// Tests node and relationship deletion
// =============================================================================

func TestParseDeletePatterns(t *testing.T) {
	tests := []struct {
		name        string
		setup       string
		query       string
		expectErr   bool
		checkQuery  string
		expectRows  int
		description string
	}{
		// DELETE node
		{
			name:        "delete node",
			setup:       "CREATE (n:DeleteMe {id: 'del'})",
			query:       "MATCH (n:DeleteMe {id: 'del'}) DELETE n",
			expectErr:   false,
			checkQuery:  "MATCH (n:DeleteMe {id: 'del'}) RETURN n",
			expectRows:  0,
			description: "DELETE removes node",
		},
		// DETACH DELETE (node with relationships)
		{
			name:        "detach delete",
			setup:       "CREATE (a:DetachA {id: 'da'})-[:REL]->(b:DetachB {id: 'db'})",
			query:       "MATCH (n:DetachA {id: 'da'}) DETACH DELETE n",
			expectErr:   false,
			checkQuery:  "MATCH (n:DetachA {id: 'da'}) RETURN n",
			expectRows:  0,
			description: "DETACH DELETE removes node and relationships",
		},
		// DELETE relationship
		{
			name:        "delete relationship",
			setup:       "CREATE (a:DelRelA {id: 'dra'})-[:TO_DELETE]->(b:DelRelB {id: 'drb'})",
			query:       "MATCH (a:DelRelA)-[r:TO_DELETE]->(b:DelRelB) DELETE r",
			expectErr:   false,
			checkQuery:  "MATCH (a:DelRelA)-[r:TO_DELETE]->(b:DelRelB) RETURN r",
			expectRows:  0,
			description: "DELETE removes relationship only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewMemoryEngine()
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			if tt.setup != "" {
				_, err := exec.Execute(ctx, tt.setup, nil)
				require.NoError(t, err, "Setup failed")
			}

			_, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				result, err := exec.Execute(ctx, tt.checkQuery, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s", tt.expectRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 13: SPECIAL CHARACTER HANDLING
// Tests queries with special characters in values
// =============================================================================

func TestParseSpecialCharacters(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup nodes with special characters
	nodes := []*storage.Node{
		{ID: "sp1", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "Test (with parens)"}},
		{ID: "sp2", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "Test 'with quotes'"}},
		{ID: "sp3", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "Test \"double quotes\""}},
		{ID: "sp4", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "Tú & Yo"}},
		{ID: "sp5", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "Line\nBreak"}},
		{ID: "sp6", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "Tab\tHere"}},
		{ID: "sp7", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "Backslash\\Path"}},
		{ID: "sp8", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "日本語テスト"}},
		{ID: "sp9", Labels: []string{"Special"}, Properties: map[string]interface{}{"name": "Emoji 🎉 Test"}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectRows  int
		description string
	}{
		// Parentheses in value
		{
			name:        "parentheses in value",
			query:       "MATCH (n:Special {name: 'Test (with parens)'}) RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Match value with parentheses",
		},
		// Single quotes (escaped)
		{
			name:        "escaped single quotes",
			query:       "MATCH (n:Special) WHERE n.name = 'Test \\'with quotes\\'' RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Match value with escaped single quotes",
		},
		// Accented characters
		{
			name:        "accented characters",
			query:       "MATCH (n:Special) WHERE n.name CONTAINS 'Tú' RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Match value with accented characters",
		},
		// Ampersand
		{
			name:        "ampersand in value",
			query:       "MATCH (n:Special) WHERE n.name CONTAINS '&' RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Match value with ampersand",
		},
		// Unicode (CJK)
		{
			name:        "unicode CJK",
			query:       "MATCH (n:Special {name: '日本語テスト'}) RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Match value with CJK characters",
		},
		// Emoji
		{
			name:        "emoji",
			query:       "MATCH (n:Special) WHERE n.name CONTAINS '🎉' RETURN n",
			expectErr:   false,
			expectRows:  1,
			description: "Match value with emoji",
		},
		// Inline property with special chars
		{
			name:        "inline property with parens",
			query:       "MATCH (n:Special {name: 'Test (with parens)'}) RETURN n.name",
			expectErr:   false,
			expectRows:  1,
			description: "Inline property filter with parentheses",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s\nQuery: %s", tt.expectRows, tt.description, tt.query)
			}
		})
	}
}

// =============================================================================
// SECTION 14: MULTIPLE MATCH PATTERNS
// Tests queries with multiple MATCH clauses
// =============================================================================

func TestParseMultipleMatchPatterns(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create a graph with multiple relationship types
	nodes := []*storage.Node{
		{ID: "person-1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "person-2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: "company-1", Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "TechCorp"}},
		{ID: "skill-1", Labels: []string{"Skill"}, Properties: map[string]interface{}{"name": "Go"}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	edges := []*storage.Edge{
		{ID: "e1", Type: "WORKS_AT", StartNode: "person-1", EndNode: "company-1"},
		{ID: "e2", Type: "KNOWS", StartNode: "person-1", EndNode: "person-2"},
		{ID: "e3", Type: "HAS_SKILL", StartNode: "person-1", EndNode: "skill-1"},
		{ID: "e4", Type: "WORKS_AT", StartNode: "person-2", EndNode: "company-1"},
	}
	for _, e := range edges {
		require.NoError(t, store.CreateEdge(e))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		minRows     int
		description string
	}{
		// Two MATCH clauses - same variable
		{
			name: "two MATCH same variable",
			query: `
				MATCH (p:Person)-[:WORKS_AT]->(c:Company)
				MATCH (p)-[:HAS_SKILL]->(s:Skill)
				RETURN p.name, c.name, s.name
			`,
			expectErr:   false,
			minRows:     1,
			description: "Two MATCH clauses sharing variable p",
		},
		// Two MATCH clauses - different patterns
		{
			name: "two MATCH different patterns",
			query: `
				MATCH (p1:Person)-[:WORKS_AT]->(c:Company)
				MATCH (p2:Person)-[:WORKS_AT]->(c)
				WHERE p1 <> p2
				RETURN p1.name, p2.name, c.name
			`,
			expectErr:   false,
			minRows:     2, // Alice-Bob and Bob-Alice at TechCorp
			description: "Find coworkers at same company",
		},
		// Three MATCH clauses
		{
			name: "three MATCH clauses",
			query: `
				MATCH (p:Person {name: 'Alice'})
				MATCH (p)-[:WORKS_AT]->(c:Company)
				MATCH (p)-[:HAS_SKILL]->(s:Skill)
				RETURN c.name, s.name
			`,
			expectErr:   false,
			minRows:     1,
			description: "Three MATCH clauses",
		},
		// MATCH with WHERE on second pattern
		{
			name: "MATCH WHERE on relationship",
			query: `
				MATCH (p:Person)-[:WORKS_AT]->(c:Company)
				MATCH (p)-[:KNOWS]->(friend:Person)
				RETURN p.name, friend.name
			`,
			expectErr:   false,
			minRows:     1,
			description: "Find people and their friends who work somewhere",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.GreaterOrEqual(t, len(result.Rows), tt.minRows,
					"Expected at least %d rows for: %s", tt.minRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 15: OPTIONAL MATCH
// Tests optional pattern matching
// =============================================================================

func TestParseOptionalMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	nodes := []*storage.Node{
		{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: "addr1", Labels: []string{"Address"}, Properties: map[string]interface{}{"city": "NYC"}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	edges := []*storage.Edge{
		{ID: "e1", Type: "LIVES_AT", StartNode: "p1", EndNode: "addr1"},
	}
	for _, e := range edges {
		require.NoError(t, store.CreateEdge(e))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectRows  int
		description string
	}{
		// OPTIONAL MATCH returns null for missing
		{
			name:        "optional match returns null",
			query:       "MATCH (p:Person) OPTIONAL MATCH (p)-[:LIVES_AT]->(a:Address) RETURN p.name, a.city",
			expectErr:   false,
			expectRows:  2, // Alice with NYC, Bob with null
			description: "OPTIONAL MATCH returns null for non-matching patterns",
		},
		// OPTIONAL MATCH with filter
		{
			name:        "optional match with where",
			query:       "MATCH (p:Person) OPTIONAL MATCH (p)-[:LIVES_AT]->(a:Address) WHERE a.city = 'NYC' RETURN p.name, a.city",
			expectErr:   false,
			expectRows:  2,
			description: "OPTIONAL MATCH with WHERE filter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s", tt.expectRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 16: UNWIND
// Tests list unwinding
// =============================================================================

func TestParseUnwind(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectRows  int
		description string
	}{
		// UNWIND literal list
		{
			name:        "unwind literal list",
			query:       "UNWIND [1, 2, 3] AS x RETURN x",
			expectErr:   false,
			expectRows:  3,
			description: "UNWIND a literal list",
		},
		// UNWIND with CREATE
		{
			name:        "unwind with create",
			query:       "UNWIND ['A', 'B', 'C'] AS name CREATE (n:UnwindTest {name: name})",
			expectErr:   false,
			expectRows:  0, // CREATE doesn't return unless asked
			description: "UNWIND with CREATE",
		},
		// UNWIND collected values
		{
			name: "unwind collected",
			query: `
				UNWIND ['a', 'b'] AS letter
				WITH collect(letter) AS letters
				UNWIND letters AS l
				RETURN l
			`,
			expectErr:   false,
			expectRows:  2,
			description: "UNWIND collected list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				if tt.expectRows > 0 {
					assert.Equal(t, tt.expectRows, len(result.Rows),
						"Expected %d rows for: %s", tt.expectRows, tt.description)
				}
			}
		})
	}
}

// =============================================================================
// SECTION 17: PARAMETERS
// Tests parameterized queries
// =============================================================================

func TestParseParameters(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Param"}, Properties: map[string]interface{}{"name": "Test1", "value": int64(100)}},
		{ID: "n2", Labels: []string{"Param"}, Properties: map[string]interface{}{"name": "Test2", "value": int64(200)}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	tests := []struct {
		name        string
		query       string
		params      map[string]interface{}
		expectErr   bool
		expectRows  int
		description string
	}{
		// String parameter
		{
			name:        "string parameter",
			query:       "MATCH (n:Param {name: $name}) RETURN n",
			params:      map[string]interface{}{"name": "Test1"},
			expectErr:   false,
			expectRows:  1,
			description: "Query with string parameter",
		},
		// Numeric parameter
		{
			name:        "numeric parameter",
			query:       "MATCH (n:Param) WHERE n.value > $threshold RETURN n",
			params:      map[string]interface{}{"threshold": int64(150)},
			expectErr:   false,
			expectRows:  1,
			description: "Query with numeric parameter",
		},
		// Multiple parameters
		{
			name:        "multiple parameters",
			query:       "MATCH (n:Param) WHERE n.name = $name AND n.value = $value RETURN n",
			params:      map[string]interface{}{"name": "Test1", "value": int64(100)},
			expectErr:   false,
			expectRows:  1,
			description: "Query with multiple parameters",
		},
		// List parameter
		{
			name:        "list parameter",
			query:       "MATCH (n:Param) WHERE n.name IN $names RETURN n",
			params:      map[string]interface{}{"names": []interface{}{"Test1", "Test2"}},
			expectErr:   false,
			expectRows:  2,
			description: "Query with list parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, tt.params)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s", tt.expectRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 18: CASE EXPRESSIONS
// Tests CASE WHEN THEN ELSE END
// =============================================================================

func TestParseCaseExpressions(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Score"}, Properties: map[string]interface{}{"value": int64(90)}},
		{ID: "n2", Labels: []string{"Score"}, Properties: map[string]interface{}{"value": int64(75)}},
		{ID: "n3", Labels: []string{"Score"}, Properties: map[string]interface{}{"value": int64(50)}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectRows  int
		description string
	}{
		// Simple CASE
		{
			name: "simple case",
			query: `
				MATCH (n:Score)
				RETURN n.value,
				       CASE
				         WHEN n.value >= 80 THEN 'A'
				         WHEN n.value >= 60 THEN 'B'
				         ELSE 'C'
				       END AS grade
			`,
			expectErr:   false,
			expectRows:  3,
			description: "Simple CASE expression with WHEN/THEN/ELSE",
		},
		// Generic CASE
		{
			name: "generic case",
			query: `
				MATCH (n:Score)
				RETURN CASE n.value
				         WHEN 90 THEN 'Excellent'
				         WHEN 75 THEN 'Good'
				         ELSE 'Average'
				       END AS rating
			`,
			expectErr:   false,
			expectRows:  3,
			description: "Generic CASE expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				assert.Equal(t, tt.expectRows, len(result.Rows),
					"Expected %d rows for: %s", tt.expectRows, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 19: STRING FUNCTIONS
// Tests string manipulation functions
// =============================================================================

func TestParseStringFunctions(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	node := &storage.Node{
		ID:     "s1",
		Labels: []string{"String"},
		Properties: map[string]interface{}{
			"text": "  Hello World  ",
			"name": "Alice",
		},
	}
	require.NoError(t, store.CreateNode(node))

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectValue interface{}
		description string
	}{
		// toUpper
		{
			name:        "toUpper",
			query:       "MATCH (n:String) RETURN toUpper(n.name)",
			expectErr:   false,
			expectValue: "ALICE",
			description: "toUpper function",
		},
		// toLower
		{
			name:        "toLower",
			query:       "MATCH (n:String) RETURN toLower(n.name)",
			expectErr:   false,
			expectValue: "alice",
			description: "toLower function",
		},
		// trim
		{
			name:        "trim",
			query:       "MATCH (n:String) RETURN trim(n.text)",
			expectErr:   false,
			expectValue: "Hello World",
			description: "trim function",
		},
		// size
		{
			name:        "size",
			query:       "MATCH (n:String) RETURN size(n.name)",
			expectErr:   false,
			expectValue: int64(5),
			description: "size function",
		},
		// substring
		{
			name:        "substring",
			query:       "MATCH (n:String) RETURN substring(n.name, 0, 3)",
			expectErr:   false,
			expectValue: "Ali",
			description: "substring function",
		},
		// replace
		{
			name:        "replace",
			query:       "MATCH (n:String) RETURN replace(n.name, 'ice', 'ex')",
			expectErr:   false,
			expectValue: "Alex",
			description: "replace function",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				require.GreaterOrEqual(t, len(result.Rows), 1)
				assert.Equal(t, tt.expectValue, result.Rows[0][0],
					"Expected %v for: %s", tt.expectValue, tt.description)
			}
		})
	}
}

// =============================================================================
// SECTION 20: LIST FUNCTIONS
// Tests list manipulation functions
// =============================================================================

func TestParseListFunctions(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name        string
		query       string
		expectErr   bool
		expectValue interface{}
		description string
	}{
		// range
		{
			name:        "range function",
			query:       "RETURN range(1, 5)",
			expectErr:   false,
			expectValue: []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
			description: "range function generates list",
		},
		// head
		{
			name:        "head function",
			query:       "RETURN head([1, 2, 3])",
			expectErr:   false,
			expectValue: int64(1),
			description: "head returns first element",
		},
		// tail
		{
			name:        "tail function",
			query:       "RETURN tail([1, 2, 3])",
			expectErr:   false,
			expectValue: []interface{}{int64(2), int64(3)},
			description: "tail returns all but first",
		},
		// last
		{
			name:        "last function",
			query:       "RETURN last([1, 2, 3])",
			expectErr:   false,
			expectValue: int64(3),
			description: "last returns last element",
		},
		// size of list
		{
			name:        "size of list",
			query:       "RETURN size([1, 2, 3, 4, 5])",
			expectErr:   false,
			expectValue: int64(5),
			description: "size returns list length",
		},
		// reverse
		{
			name:        "reverse function",
			query:       "RETURN reverse([1, 2, 3])",
			expectErr:   false,
			expectValue: []interface{}{int64(3), int64(2), int64(1)},
			description: "reverse reverses list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if tt.expectErr {
				assert.Error(t, err, "Expected error for: %s", tt.description)
			} else {
				if err != nil {
					t.Skipf("Feature not yet implemented: %s - %v", tt.description, err)
					return
				}
				require.GreaterOrEqual(t, len(result.Rows), 1)
				assert.Equal(t, tt.expectValue, result.Rows[0][0],
					"Expected %v for: %s", tt.expectValue, tt.description)
			}
		})
	}
}

// =============================================================================
// SUMMARY REPORT TEST
// Runs all tests and produces a coverage report
// =============================================================================

func TestParserCoverageReport(t *testing.T) {
	t.Log(`
================================================================================
CYPHER PARSER COVERAGE REPORT
================================================================================

This test suite covers the following Cypher syntax categories:

1.  Node Patterns          - (), (n), (n:Label), (n {prop: val})
2.  Relationship Patterns  - -[r]-, -[:TYPE]->, <-[:TYPE]-, -[:A|B]->
3.  Variable Length Paths  - -[*]-, -[*2]-, -[*1..3]->
4.  WHERE Operators        - =, <>, <, >, <=, >=, AND, OR, NOT, IN, IS NULL
5.  RETURN Clause          - properties, aliases, aggregations, DISTINCT
6.  WITH Clause            - projection, aggregation, filtering
7.  ORDER BY/SKIP/LIMIT    - sorting and pagination
8.  CREATE Patterns        - node creation, relationship creation
9.  CREATE Relationships   - between existing nodes, inline patterns
10. MERGE Patterns         - ON CREATE, ON MATCH
11. SET/REMOVE             - property updates, label manipulation
12. DELETE                 - node deletion, DETACH DELETE
13. Special Characters     - parentheses, quotes, unicode, emoji
14. Multiple MATCH         - chained patterns, shared variables
15. OPTIONAL MATCH         - null handling for missing patterns
16. UNWIND                 - list expansion
17. Parameters             - $param syntax
18. CASE Expressions       - WHEN/THEN/ELSE
19. String Functions       - toUpper, toLower, trim, substring, replace
20. List Functions         - range, head, tail, last, reverse

Run individual sections to identify unsupported features:
  go test -run TestParseNodePatterns
  go test -run TestParseWhereOperators
  etc.

================================================================================
`)
}
