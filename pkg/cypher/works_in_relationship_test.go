package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorksInRelationships tests the WORKS_IN relationship creation queries
// used in the data import process. These queries link Person nodes to Area nodes
// through intermediate POC, WarRoom, and ApplicationContact nodes.
//
// The patterns tested are:
// 1. MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)
// 2. MATCH (p:Person)<-[:HAS_VP|HAS_PRIMARY_LEAD|HAS_SECONDARY_LEAD]-(w:WarRoom)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)
// 3. MATCH (p:Person)<-[:HAS_PRODUCT_LEAD|HAS_PRODUCT_CONTACT|HAS_ENGINEERING_LEAD|HAS_ENGINEERING_CONTACT]-(app:ApplicationContact)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)

func setupWorksInTestData(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()

	// Create Areas
	areas := []string{
		`CREATE (a:Area {name: 'Engineering', code: 'ENG'})`,
		`CREATE (a:Area {name: 'Product', code: 'PROD'})`,
		`CREATE (a:Area {name: 'Operations', code: 'OPS'})`,
	}
	for _, q := range areas {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to create Area: %s", q)
	}

	// Create Person nodes
	persons := []string{
		`CREATE (p:Person {name: 'Alice Smith', email: 'alice@example.com'})`,
		`CREATE (p:Person {name: 'Bob Jones', email: 'bob@example.com'})`,
		`CREATE (p:Person {name: 'Carol White', email: 'carol@example.com'})`,
		`CREATE (p:Person {name: 'David Brown', email: 'david@example.com'})`,
		`CREATE (p:Person {name: 'Eve Davis', email: 'eve@example.com'})`,
		`CREATE (p:Person {name: 'Frank Miller', email: 'frank@example.com'})`,
		`CREATE (p:Person {name: 'Grace Wilson', email: 'grace@example.com'})`,
		`CREATE (p:Person {name: 'Henry Taylor', email: 'henry@example.com'})`,
		`CREATE (p:Person {name: 'Ivy Anderson', email: 'ivy@example.com'})`,
		`CREATE (p:Person {name: 'Jack Thomas', email: 'jack@example.com'})`,
	}
	for _, q := range persons {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to create Person: %s", q)
	}

	// Create POC nodes and link to Areas
	pocSetup := []string{
		`CREATE (poc:POC {name: 'POC-Engineering-1', type: 'primary'})`,
		`CREATE (poc:POC {name: 'POC-Product-1', type: 'secondary'})`,
		`CREATE (poc:POC {name: 'POC-Operations-1', type: 'primary'})`,
	}
	for _, q := range pocSetup {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to create POC: %s", q)
	}

	// Link POCs to Areas with BELONGS_TO
	pocAreaLinks := []string{
		`MATCH (poc:POC {name: 'POC-Engineering-1'}), (a:Area {code: 'ENG'}) CREATE (poc)-[:BELONGS_TO]->(a)`,
		`MATCH (poc:POC {name: 'POC-Product-1'}), (a:Area {code: 'PROD'}) CREATE (poc)-[:BELONGS_TO]->(a)`,
		`MATCH (poc:POC {name: 'POC-Operations-1'}), (a:Area {code: 'OPS'}) CREATE (poc)-[:BELONGS_TO]->(a)`,
	}
	for _, q := range pocAreaLinks {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to link POC to Area: %s", q)
	}

	// Link Persons to POCs with HAS_CONTACT, MANAGED_BY, HAS_LEADER relationships
	pocPersonLinks := []string{
		`MATCH (p:Person {name: 'Alice Smith'}), (poc:POC {name: 'POC-Engineering-1'}) CREATE (poc)-[:HAS_CONTACT]->(p)`,
		`MATCH (p:Person {name: 'Bob Jones'}), (poc:POC {name: 'POC-Engineering-1'}) CREATE (poc)-[:MANAGED_BY]->(p)`,
		`MATCH (p:Person {name: 'Carol White'}), (poc:POC {name: 'POC-Product-1'}) CREATE (poc)-[:HAS_LEADER]->(p)`,
	}
	for _, q := range pocPersonLinks {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to link POC to Person: %s", q)
	}

	// Create WarRoom nodes and link to Areas
	warRoomSetup := []string{
		`CREATE (w:WarRoom {name: 'WarRoom-Engineering', status: 'active'})`,
		`CREATE (w:WarRoom {name: 'WarRoom-Product', status: 'active'})`,
	}
	for _, q := range warRoomSetup {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to create WarRoom: %s", q)
	}

	// Link WarRooms to Areas with BELONGS_TO
	warRoomAreaLinks := []string{
		`MATCH (w:WarRoom {name: 'WarRoom-Engineering'}), (a:Area {code: 'ENG'}) CREATE (w)-[:BELONGS_TO]->(a)`,
		`MATCH (w:WarRoom {name: 'WarRoom-Product'}), (a:Area {code: 'PROD'}) CREATE (w)-[:BELONGS_TO]->(a)`,
	}
	for _, q := range warRoomAreaLinks {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to link WarRoom to Area: %s", q)
	}

	// Link Persons to WarRooms with HAS_VP, HAS_PRIMARY_LEAD, HAS_SECONDARY_LEAD relationships
	warRoomPersonLinks := []string{
		`MATCH (p:Person {name: 'David Brown'}), (w:WarRoom {name: 'WarRoom-Engineering'}) CREATE (w)-[:HAS_VP]->(p)`,
		`MATCH (p:Person {name: 'Eve Davis'}), (w:WarRoom {name: 'WarRoom-Engineering'}) CREATE (w)-[:HAS_PRIMARY_LEAD]->(p)`,
		`MATCH (p:Person {name: 'Frank Miller'}), (w:WarRoom {name: 'WarRoom-Product'}) CREATE (w)-[:HAS_SECONDARY_LEAD]->(p)`,
	}
	for _, q := range warRoomPersonLinks {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to link WarRoom to Person: %s", q)
	}

	// Create ApplicationContact nodes and link to Areas
	appContactSetup := []string{
		`CREATE (app:ApplicationContact {name: 'App-Engineering-Backend', application: 'Backend API'})`,
		`CREATE (app:ApplicationContact {name: 'App-Product-Frontend', application: 'Web Portal'})`,
		`CREATE (app:ApplicationContact {name: 'App-Operations-Infra', application: 'Infrastructure'})`,
	}
	for _, q := range appContactSetup {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to create ApplicationContact: %s", q)
	}

	// Link ApplicationContacts to Areas with BELONGS_TO
	appContactAreaLinks := []string{
		`MATCH (app:ApplicationContact {name: 'App-Engineering-Backend'}), (a:Area {code: 'ENG'}) CREATE (app)-[:BELONGS_TO]->(a)`,
		`MATCH (app:ApplicationContact {name: 'App-Product-Frontend'}), (a:Area {code: 'PROD'}) CREATE (app)-[:BELONGS_TO]->(a)`,
		`MATCH (app:ApplicationContact {name: 'App-Operations-Infra'}), (a:Area {code: 'OPS'}) CREATE (app)-[:BELONGS_TO]->(a)`,
	}
	for _, q := range appContactAreaLinks {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to link ApplicationContact to Area: %s", q)
	}

	// Link Persons to ApplicationContacts with various relationship types
	appContactPersonLinks := []string{
		`MATCH (p:Person {name: 'Grace Wilson'}), (app:ApplicationContact {name: 'App-Engineering-Backend'}) CREATE (app)-[:HAS_PRODUCT_LEAD]->(p)`,
		`MATCH (p:Person {name: 'Henry Taylor'}), (app:ApplicationContact {name: 'App-Product-Frontend'}) CREATE (app)-[:HAS_PRODUCT_CONTACT]->(p)`,
		`MATCH (p:Person {name: 'Ivy Anderson'}), (app:ApplicationContact {name: 'App-Operations-Infra'}) CREATE (app)-[:HAS_ENGINEERING_LEAD]->(p)`,
		`MATCH (p:Person {name: 'Jack Thomas'}), (app:ApplicationContact {name: 'App-Operations-Infra'}) CREATE (app)-[:HAS_ENGINEERING_CONTACT]->(p)`,
	}
	for _, q := range appContactPersonLinks {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to link ApplicationContact to Person: %s", q)
	}
}

// TestWorksInFromPOC tests the first WORKS_IN query:
// MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area)
// MERGE (p)-[:WORKS_IN]->(a)
func TestWorksInFromPOC(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	setupWorksInTestData(t, exec, ctx)

	// Verify initial state - no WORKS_IN relationships should exist
	initialCheck, err := exec.Execute(ctx, `MATCH (p:Person)-[r:WORKS_IN]->(a:Area) RETURN count(r) as count`, nil)
	require.NoError(t, err)
	require.Len(t, initialCheck.Rows, 1)
	initialCount, ok := initialCheck.Rows[0][0].(int64)
	require.True(t, ok)
	assert.Equal(t, int64(0), initialCount, "No WORKS_IN relationships should exist initially")

	// Execute the first WORKS_IN query (POC-based)
	_, err = exec.Execute(ctx, `
		MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area)
		MERGE (p)-[:WORKS_IN]->(a)
	`, nil)
	require.NoError(t, err, "POC WORKS_IN query should execute without error")

	// Verify WORKS_IN relationships were created for POC-linked persons
	t.Run("Alice should WORKS_IN Engineering via HAS_CONTACT", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Alice Smith'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Alice should have 1 WORKS_IN relationship")
		assert.Equal(t, "Engineering", result.Rows[0][0])
	})

	t.Run("Bob should WORKS_IN Engineering via MANAGED_BY", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Bob Jones'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Bob should have 1 WORKS_IN relationship")
		assert.Equal(t, "Engineering", result.Rows[0][0])
	})

	t.Run("Carol should WORKS_IN Product via HAS_LEADER", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Carol White'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Carol should have 1 WORKS_IN relationship")
		assert.Equal(t, "Product", result.Rows[0][0])
	})

	// Verify total count of WORKS_IN from POC
	countResult, err := exec.Execute(ctx, `
		MATCH (p:Person)-[r:WORKS_IN]->(a:Area)
		RETURN count(r) as count
	`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	count, ok := countResult.Rows[0][0].(int64)
	require.True(t, ok)
	assert.Equal(t, int64(3), count, "Should have 3 WORKS_IN relationships from POC query")
}

// TestWorksInFromWarRoom tests the second WORKS_IN query:
// MATCH (p:Person)<-[:HAS_VP|HAS_PRIMARY_LEAD|HAS_SECONDARY_LEAD]-(w:WarRoom)-[:BELONGS_TO]->(a:Area)
// MERGE (p)-[:WORKS_IN]->(a)
func TestWorksInFromWarRoom(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	setupWorksInTestData(t, exec, ctx)

	// Execute the second WORKS_IN query (WarRoom-based)
	_, err := exec.Execute(ctx, `
		MATCH (p:Person)<-[:HAS_VP|HAS_PRIMARY_LEAD|HAS_SECONDARY_LEAD]-(w:WarRoom)-[:BELONGS_TO]->(a:Area)
		MERGE (p)-[:WORKS_IN]->(a)
	`, nil)
	require.NoError(t, err, "WarRoom WORKS_IN query should execute without error")

	// Verify WORKS_IN relationships were created for WarRoom-linked persons
	t.Run("David should WORKS_IN Engineering via HAS_VP", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'David Brown'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "David should have 1 WORKS_IN relationship")
		assert.Equal(t, "Engineering", result.Rows[0][0])
	})

	t.Run("Eve should WORKS_IN Engineering via HAS_PRIMARY_LEAD", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Eve Davis'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Eve should have 1 WORKS_IN relationship")
		assert.Equal(t, "Engineering", result.Rows[0][0])
	})

	t.Run("Frank should WORKS_IN Product via HAS_SECONDARY_LEAD", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Frank Miller'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Frank should have 1 WORKS_IN relationship")
		assert.Equal(t, "Product", result.Rows[0][0])
	})

	// Verify total count of WORKS_IN from WarRoom
	countResult, err := exec.Execute(ctx, `
		MATCH (p:Person)-[r:WORKS_IN]->(a:Area)
		RETURN count(r) as count
	`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	count, ok := countResult.Rows[0][0].(int64)
	require.True(t, ok)
	assert.Equal(t, int64(3), count, "Should have 3 WORKS_IN relationships from WarRoom query")
}

// TestWorksInFromApplicationContact tests the third WORKS_IN query:
// MATCH (p:Person)<-[:HAS_PRODUCT_LEAD|HAS_PRODUCT_CONTACT|HAS_ENGINEERING_LEAD|HAS_ENGINEERING_CONTACT]-(app:ApplicationContact)-[:BELONGS_TO]->(a:Area)
// MERGE (p)-[:WORKS_IN]->(a)
func TestWorksInFromApplicationContact(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	setupWorksInTestData(t, exec, ctx)

	// Execute the third WORKS_IN query (ApplicationContact-based)
	_, err := exec.Execute(ctx, `
		MATCH (p:Person)<-[:HAS_PRODUCT_LEAD|HAS_PRODUCT_CONTACT|HAS_ENGINEERING_LEAD|HAS_ENGINEERING_CONTACT]-(app:ApplicationContact)-[:BELONGS_TO]->(a:Area)
		MERGE (p)-[:WORKS_IN]->(a)
	`, nil)
	require.NoError(t, err, "ApplicationContact WORKS_IN query should execute without error")

	// Verify WORKS_IN relationships were created for ApplicationContact-linked persons
	t.Run("Grace should WORKS_IN Engineering via HAS_PRODUCT_LEAD", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Grace Wilson'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Grace should have 1 WORKS_IN relationship")
		assert.Equal(t, "Engineering", result.Rows[0][0])
	})

	t.Run("Henry should WORKS_IN Product via HAS_PRODUCT_CONTACT", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Henry Taylor'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Henry should have 1 WORKS_IN relationship")
		assert.Equal(t, "Product", result.Rows[0][0])
	})

	t.Run("Ivy should WORKS_IN Operations via HAS_ENGINEERING_LEAD", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Ivy Anderson'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Ivy should have 1 WORKS_IN relationship")
		assert.Equal(t, "Operations", result.Rows[0][0])
	})

	t.Run("Jack should WORKS_IN Operations via HAS_ENGINEERING_CONTACT", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person {name: 'Jack Thomas'})-[:WORKS_IN]->(a:Area)
			RETURN a.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Jack should have 1 WORKS_IN relationship")
		assert.Equal(t, "Operations", result.Rows[0][0])
	})

	// Verify total count of WORKS_IN from ApplicationContact
	countResult, err := exec.Execute(ctx, `
		MATCH (p:Person)-[r:WORKS_IN]->(a:Area)
		RETURN count(r) as count
	`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	count, ok := countResult.Rows[0][0].(int64)
	require.True(t, ok)
	assert.Equal(t, int64(4), count, "Should have 4 WORKS_IN relationships from ApplicationContact query")
}

// TestWorksInAllQueriesCombined tests running all three WORKS_IN queries in sequence
// to verify they work correctly together and MERGE doesn't create duplicates
func TestWorksInAllQueriesCombined(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	setupWorksInTestData(t, exec, ctx)

	// Execute all three WORKS_IN queries in sequence (as in the original import)
	queries := []struct {
		name  string
		query string
	}{
		{
			name: "POC-based WORKS_IN",
			query: `
				MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area)
				MERGE (p)-[:WORKS_IN]->(a)
			`,
		},
		{
			name: "WarRoom-based WORKS_IN",
			query: `
				MATCH (p:Person)<-[:HAS_VP|HAS_PRIMARY_LEAD|HAS_SECONDARY_LEAD]-(w:WarRoom)-[:BELONGS_TO]->(a:Area)
				MERGE (p)-[:WORKS_IN]->(a)
			`,
		},
		{
			name: "ApplicationContact-based WORKS_IN",
			query: `
				MATCH (p:Person)<-[:HAS_PRODUCT_LEAD|HAS_PRODUCT_CONTACT|HAS_ENGINEERING_LEAD|HAS_ENGINEERING_CONTACT]-(app:ApplicationContact)-[:BELONGS_TO]->(a:Area)
				MERGE (p)-[:WORKS_IN]->(a)
			`,
		},
	}

	for _, q := range queries {
		t.Run(q.name+" executes without error", func(t *testing.T) {
			_, err := exec.Execute(ctx, q.query, nil)
			require.NoError(t, err, "%s should execute without error", q.name)
		})
	}

	// Verify total WORKS_IN relationships (should be 10 unique)
	t.Run("total WORKS_IN count is correct", func(t *testing.T) {
		countResult, err := exec.Execute(ctx, `
			MATCH (p:Person)-[r:WORKS_IN]->(a:Area)
			RETURN count(r) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, countResult.Rows, 1)
		count, ok := countResult.Rows[0][0].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(10), count, "Should have 10 total WORKS_IN relationships")
	})

	// Verify each person has exactly one WORKS_IN relationship
	t.Run("each person has one WORKS_IN relationship", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)-[r:WORKS_IN]->(a:Area)
			RETURN p.name, count(r) as relCount
			ORDER BY p.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 10, "Should have 10 persons with WORKS_IN relationships")

		for _, row := range result.Rows {
			name := row[0].(string)
			count, ok := row[1].(int64)
			require.True(t, ok)
			assert.Equal(t, int64(1), count, "Person %s should have exactly 1 WORKS_IN relationship", name)
		}
	})

	// Verify relationships by Area
	t.Run("Engineering has 5 persons", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)-[:WORKS_IN]->(a:Area {code: 'ENG'})
			RETURN count(p) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		count, ok := result.Rows[0][0].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(5), count, "Engineering should have 5 persons (Alice, Bob via POC; David, Eve via WarRoom; Grace via AppContact)")
	})

	t.Run("Product has 3 persons", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)-[:WORKS_IN]->(a:Area {code: 'PROD'})
			RETURN count(p) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		count, ok := result.Rows[0][0].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(3), count, "Product should have 3 persons (Carol via POC; Frank via WarRoom; Henry via AppContact)")
	})

	t.Run("Operations has 2 persons", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)-[:WORKS_IN]->(a:Area {code: 'OPS'})
			RETURN count(p) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		count, ok := result.Rows[0][0].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(2), count, "Operations should have 2 persons")
	})
}

// TestWorksInMergeIdempotency tests that running the same MERGE query twice
// does not create duplicate relationships
func TestWorksInMergeIdempotency(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	setupWorksInTestData(t, exec, ctx)

	// Execute POC WORKS_IN query twice
	for i := 0; i < 2; i++ {
		_, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area)
			MERGE (p)-[:WORKS_IN]->(a)
		`, nil)
		require.NoError(t, err, "POC WORKS_IN query iteration %d should execute without error", i+1)
	}

	// Verify no duplicate relationships were created
	result, err := exec.Execute(ctx, `
		MATCH (p:Person {name: 'Alice Smith'})-[r:WORKS_IN]->(a:Area)
		RETURN count(r) as count
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	count, ok := result.Rows[0][0].(int64)
	require.True(t, ok)
	assert.Equal(t, int64(1), count, "Alice should still have exactly 1 WORKS_IN relationship after running MERGE twice")
}

// TestWorksInEmptyMatch tests the queries when no matching patterns exist
func TestWorksInEmptyMatch(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes but no relationships
	_, err := exec.Execute(ctx, `CREATE (p:Person {name: 'Orphan Person'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (a:Area {name: 'Empty Area', code: 'EMPTY'})`, nil)
	require.NoError(t, err)

	// Execute all three queries - should not fail even with no matches
	queries := []string{
		`MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)`,
		`MATCH (p:Person)<-[:HAS_VP|HAS_PRIMARY_LEAD|HAS_SECONDARY_LEAD]-(w:WarRoom)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)`,
		`MATCH (p:Person)<-[:HAS_PRODUCT_LEAD|HAS_PRODUCT_CONTACT|HAS_ENGINEERING_LEAD|HAS_ENGINEERING_CONTACT]-(app:ApplicationContact)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)`,
	}

	for i, q := range queries {
		t.Run("empty match query "+string(rune('1'+i)), func(t *testing.T) {
			_, err := exec.Execute(ctx, q, nil)
			require.NoError(t, err, "Query should execute without error even with no matches")
		})
	}

	// Verify no WORKS_IN relationships were created
	result, err := exec.Execute(ctx, `MATCH ()-[r:WORKS_IN]->() RETURN count(r) as count`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	count, ok := result.Rows[0][0].(int64)
	require.True(t, ok)
	assert.Equal(t, int64(0), count, "No WORKS_IN relationships should exist when patterns don't match")
}

// TestWorksInMultiplePathsToSameArea tests that a Person connected to the same Area
// through multiple intermediate nodes only gets one WORKS_IN relationship
func TestWorksInMultiplePathsToSameArea(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create Area
	_, err := exec.Execute(ctx, `CREATE (a:Area {name: 'Shared Area', code: 'SHARED'})`, nil)
	require.NoError(t, err)

	// Create Person
	_, err = exec.Execute(ctx, `CREATE (p:Person {name: 'Multi-Path Person', email: 'multi@example.com'})`, nil)
	require.NoError(t, err)

	// Create multiple intermediate nodes all pointing to the same Area
	_, err = exec.Execute(ctx, `CREATE (poc:POC {name: 'POC-Shared'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (w:WarRoom {name: 'WarRoom-Shared'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (app:ApplicationContact {name: 'App-Shared'})`, nil)
	require.NoError(t, err)

	// Link all intermediates to the same Area
	_, err = exec.Execute(ctx, `MATCH (poc:POC {name: 'POC-Shared'}), (a:Area {code: 'SHARED'}) CREATE (poc)-[:BELONGS_TO]->(a)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (w:WarRoom {name: 'WarRoom-Shared'}), (a:Area {code: 'SHARED'}) CREATE (w)-[:BELONGS_TO]->(a)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (app:ApplicationContact {name: 'App-Shared'}), (a:Area {code: 'SHARED'}) CREATE (app)-[:BELONGS_TO]->(a)`, nil)
	require.NoError(t, err)

	// Link the same Person from all intermediates
	_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Multi-Path Person'}), (poc:POC {name: 'POC-Shared'}) CREATE (poc)-[:HAS_CONTACT]->(p)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Multi-Path Person'}), (w:WarRoom {name: 'WarRoom-Shared'}) CREATE (w)-[:HAS_VP]->(p)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Multi-Path Person'}), (app:ApplicationContact {name: 'App-Shared'}) CREATE (app)-[:HAS_PRODUCT_LEAD]->(p)`, nil)
	require.NoError(t, err)

	// Execute all three WORKS_IN queries
	queries := []string{
		`MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)`,
		`MATCH (p:Person)<-[:HAS_VP|HAS_PRIMARY_LEAD|HAS_SECONDARY_LEAD]-(w:WarRoom)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)`,
		`MATCH (p:Person)<-[:HAS_PRODUCT_LEAD|HAS_PRODUCT_CONTACT|HAS_ENGINEERING_LEAD|HAS_ENGINEERING_CONTACT]-(app:ApplicationContact)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)`,
	}

	for _, q := range queries {
		_, err = exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	// Verify only ONE WORKS_IN relationship exists (MERGE should not duplicate)
	result, err := exec.Execute(ctx, `
		MATCH (p:Person {name: 'Multi-Path Person'})-[r:WORKS_IN]->(a:Area)
		RETURN count(r) as count
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	count, ok := result.Rows[0][0].(int64)
	require.True(t, ok)
	assert.Equal(t, int64(1), count, "Person connected through multiple paths should have exactly 1 WORKS_IN relationship due to MERGE")
}

// TestWorksInRelationshipTypeAlternation tests that the pipe (|) operator
// correctly matches multiple relationship types
func TestWorksInRelationshipTypeAlternation(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data for each relationship type in the alternation
	_, err := exec.Execute(ctx, `CREATE (a:Area {name: 'Test Area', code: 'TEST'})`, nil)
	require.NoError(t, err)

	// Create POC and persons for each relationship type
	pocRelTypes := []string{"HAS_CONTACT", "MANAGED_BY", "HAS_LEADER"}
	for i, relType := range pocRelTypes {
		personName := "POC-Person-" + relType
		_, err = exec.Execute(ctx, `CREATE (p:Person {name: $name})`, map[string]interface{}{"name": personName})
		require.NoError(t, err)

		pocName := "POC-" + relType
		_, err = exec.Execute(ctx, `CREATE (poc:POC {name: $name})`, map[string]interface{}{"name": pocName})
		require.NoError(t, err)

		// Link POC to Area
		_, err = exec.Execute(ctx, `MATCH (poc:POC {name: $pocName}), (a:Area {code: 'TEST'}) CREATE (poc)-[:BELONGS_TO]->(a)`,
			map[string]interface{}{"pocName": pocName})
		require.NoError(t, err)

		// Link Person to POC with specific relationship type
		query := "MATCH (p:Person {name: $personName}), (poc:POC {name: $pocName}) CREATE (poc)-[:" + relType + "]->(p)"
		_, err = exec.Execute(ctx, query, map[string]interface{}{"personName": personName, "pocName": pocName})
		require.NoError(t, err, "Failed to create relationship type %s (index %d)", relType, i)
	}

	// Execute the POC WORKS_IN query
	_, err = exec.Execute(ctx, `
		MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area)
		MERGE (p)-[:WORKS_IN]->(a)
	`, nil)
	require.NoError(t, err)

	// Verify all three persons got WORKS_IN relationships
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)-[:WORKS_IN]->(a:Area {code: 'TEST'})
		RETURN p.name
		ORDER BY p.name
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 3, "All three persons should have WORKS_IN relationships")

	expectedNames := []string{"POC-Person-HAS_CONTACT", "POC-Person-HAS_LEADER", "POC-Person-MANAGED_BY"}
	for i, row := range result.Rows {
		assert.Equal(t, expectedNames[i], row[0], "Person %d should be %s", i, expectedNames[i])
	}
}
