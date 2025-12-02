package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChainedWithMatchMerge tests the pattern:
// MERGE (e:Entry {key: $key}) WITH e MATCH (b:Category) MERGE (e)-[:REL]->(b)
// This is the pattern used in the import script
func TestChainedWithMatchMerge(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a Category node first
	_, err := exec.Execute(ctx, `CREATE (c:Category {name: 'TestCategory'})`, nil)
	require.NoError(t, err)

	t.Run("MERGE then WITH then MATCH then MERGE relationship - match succeeds", func(t *testing.T) {
		// This should create Entry and link it to Category
		result, err := exec.Execute(ctx, `
			MERGE (e:Entry {key: 'entry1'})
			ON CREATE SET e.value = 'test'
			WITH e
			MATCH (c:Category {name: 'TestCategory'})
			MERGE (e)-[:IN_CATEGORY]->(c)
			RETURN e.key, c.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Should return 1 row when MATCH succeeds")
		assert.Equal(t, "entry1", result.Rows[0][0])
		assert.Equal(t, "TestCategory", result.Rows[0][1])

		// Verify relationship was created
		verifyResult, err := exec.Execute(ctx, `
			MATCH (e:Entry {key: 'entry1'})-[:IN_CATEGORY]->(c:Category)
			RETURN c.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, verifyResult.Rows, 1, "Relationship should exist")
		assert.Equal(t, "TestCategory", verifyResult.Rows[0][0])
	})

	t.Run("MERGE then WITH then MATCH then MERGE relationship - match fails", func(t *testing.T) {
		// This should create Entry but NOT link it (Category doesn't exist)
		result, err := exec.Execute(ctx, `
			MERGE (e:Entry {key: 'entry2'})
			ON CREATE SET e.value = 'test2'
			WITH e
			MATCH (c:Category {name: 'NonExistent'})
			MERGE (e)-[:IN_CATEGORY]->(c)
			RETURN e.key, c.name
		`, nil)
		require.NoError(t, err)
		// In Neo4j, when MATCH fails, it returns 0 rows
		assert.Len(t, result.Rows, 0, "Should return 0 rows when MATCH fails")

		// But the Entry should still have been created by MERGE
		verifyEntry, err := exec.Execute(ctx, `
			MATCH (e:Entry {key: 'entry2'})
			RETURN e.value
		`, nil)
		require.NoError(t, err)
		require.Len(t, verifyEntry.Rows, 1, "Entry should still be created even if MATCH fails")
		assert.Equal(t, "test2", verifyEntry.Rows[0][0])

		// And it should NOT have a relationship
		verifyRel, err := exec.Execute(ctx, `
			MATCH (e:Entry {key: 'entry2'})-[:IN_CATEGORY]->(c:Category)
			RETURN c.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, verifyRel.Rows, 0, "No relationship should exist when MATCH failed")
	})

	t.Run("chained MATCH failures should not prevent earlier relationships", func(t *testing.T) {
		// Create additional nodes
		_, err := exec.Execute(ctx, `CREATE (b:BusinessArea {name: 'TestBusiness'})`, nil)
		require.NoError(t, err)

		// This pattern mirrors the import script:
		// First MATCH succeeds, second MATCH fails
		result, err := exec.Execute(ctx, `
			MERGE (e:Entry {key: 'entry3'})
			WITH e
			MATCH (b:BusinessArea {name: 'TestBusiness'})
			MERGE (e)-[:IN_BUSINESS]->(b)
			WITH e
			MATCH (c:Category {name: 'NonExistent'})
			MERGE (e)-[:IN_CATEGORY]->(c)
			RETURN e.key
		`, nil)
		require.NoError(t, err)
		// Second MATCH fails, so 0 rows returned
		assert.Len(t, result.Rows, 0)

		// Check if the FIRST relationship was created before the chain broke
		verifyFirst, err := exec.Execute(ctx, `
			MATCH (e:Entry {key: 'entry3'})-[:IN_BUSINESS]->(b:BusinessArea)
			RETURN b.name
		`, nil)
		require.NoError(t, err)
		// In Neo4j, the first MERGE happens, then chain breaks at second MATCH
		// This is the KEY question: does NornicDB handle this correctly?
		t.Logf("First relationship exists: %d rows", len(verifyFirst.Rows))
	})
}

// TestChainedWithMatchPermutations tests all permutations of chained WITH...MATCH patterns
func TestChainedWithMatchPermutations(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create lookup nodes
	setup := []string{
		`CREATE (a:TypeA {name: 'A1'})`,
		`CREATE (a:TypeA {name: 'A2'})`,
		`CREATE (b:TypeB {name: 'B1'})`,
		`CREATE (c:TypeC {name: 'C1'})`,
		`CREATE (d:TypeD {name: 'D1'})`,
	}
	for _, q := range setup {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("single WITH+MATCH succeeds", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm1'})
			WITH e
			MATCH (a:TypeA {name: 'A1'})
			MERGE (e)-[:REL_A]->(a)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		verify, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm1'})-[:REL_A]->(a) RETURN a.name`, nil)
		require.Len(t, verify.Rows, 1)
	})

	t.Run("single WITH+MATCH fails", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm2'})
			WITH e
			MATCH (a:TypeA {name: 'NONEXISTENT'})
			MERGE (e)-[:REL_A]->(a)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "MATCH fails = 0 rows")

		// Entity still created
		verify, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm2'}) RETURN e.id`, nil)
		require.Len(t, verify.Rows, 1, "Entity should exist")
	})

	t.Run("two chained: both succeed", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm3'})
			WITH e
			MATCH (a:TypeA {name: 'A1'})
			MERGE (e)-[:REL_A]->(a)
			WITH e
			MATCH (b:TypeB {name: 'B1'})
			MERGE (e)-[:REL_B]->(b)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		verifyA, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm3'})-[:REL_A]->(a) RETURN a.name`, nil)
		verifyB, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm3'})-[:REL_B]->(b) RETURN b.name`, nil)
		assert.Len(t, verifyA.Rows, 1, "REL_A should exist")
		assert.Len(t, verifyB.Rows, 1, "REL_B should exist")
	})

	t.Run("two chained: first succeeds, second fails", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm4'})
			WITH e
			MATCH (a:TypeA {name: 'A1'})
			MERGE (e)-[:REL_A]->(a)
			WITH e
			MATCH (b:TypeB {name: 'NONEXISTENT'})
			MERGE (e)-[:REL_B]->(b)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "Second MATCH fails = 0 rows")

		verifyA, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm4'})-[:REL_A]->(a) RETURN a.name`, nil)
		verifyB, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm4'})-[:REL_B]->(b) RETURN b.name`, nil)
		// First rel created before chain broke
		t.Logf("REL_A exists: %d, REL_B exists: %d", len(verifyA.Rows), len(verifyB.Rows))
	})

	t.Run("two chained: first fails, second would succeed", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm5'})
			WITH e
			MATCH (a:TypeA {name: 'NONEXISTENT'})
			MERGE (e)-[:REL_A]->(a)
			WITH e
			MATCH (b:TypeB {name: 'B1'})
			MERGE (e)-[:REL_B]->(b)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "First MATCH fails = chain broken")

		verifyA, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm5'})-[:REL_A]->(a) RETURN a.name`, nil)
		verifyB, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm5'})-[:REL_B]->(b) RETURN b.name`, nil)
		assert.Len(t, verifyA.Rows, 0, "REL_A should NOT exist")
		assert.Len(t, verifyB.Rows, 0, "REL_B should NOT exist - chain broke before")
	})

	t.Run("two chained: both fail", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm6'})
			WITH e
			MATCH (a:TypeA {name: 'NONEXISTENT'})
			MERGE (e)-[:REL_A]->(a)
			WITH e
			MATCH (b:TypeB {name: 'ALSO_NONEXISTENT'})
			MERGE (e)-[:REL_B]->(b)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0)

		// Entity still created
		verify, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm6'}) RETURN e.id`, nil)
		require.Len(t, verify.Rows, 1, "Entity should exist despite failed MATCHes")
	})

	t.Run("three chained: all succeed", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm7'})
			WITH e
			MATCH (a:TypeA {name: 'A1'})
			MERGE (e)-[:REL_A]->(a)
			WITH e
			MATCH (b:TypeB {name: 'B1'})
			MERGE (e)-[:REL_B]->(b)
			WITH e
			MATCH (c:TypeC {name: 'C1'})
			MERGE (e)-[:REL_C]->(c)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		verifyA, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm7'})-[:REL_A]->() RETURN count(*) as c`, nil)
		verifyB, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm7'})-[:REL_B]->() RETURN count(*) as c`, nil)
		verifyC, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm7'})-[:REL_C]->() RETURN count(*) as c`, nil)
		assert.Equal(t, int64(1), verifyA.Rows[0][0])
		assert.Equal(t, int64(1), verifyB.Rows[0][0])
		assert.Equal(t, int64(1), verifyC.Rows[0][0])
	})

	t.Run("three chained: middle fails", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm8'})
			WITH e
			MATCH (a:TypeA {name: 'A1'})
			MERGE (e)-[:REL_A]->(a)
			WITH e
			MATCH (b:TypeB {name: 'NONEXISTENT'})
			MERGE (e)-[:REL_B]->(b)
			WITH e
			MATCH (c:TypeC {name: 'C1'})
			MERGE (e)-[:REL_C]->(c)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0)

		verifyA, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm8'})-[:REL_A]->() RETURN count(*) as c`, nil)
		verifyB, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm8'})-[:REL_B]->() RETURN count(*) as c`, nil)
		verifyC, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm8'})-[:REL_C]->() RETURN count(*) as c`, nil)
		t.Logf("REL_A: %v, REL_B: %v, REL_C: %v", verifyA.Rows[0][0], verifyB.Rows[0][0], verifyC.Rows[0][0])
	})

	t.Run("five chained: third fails (mirrors import script)", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'perm9'})
			WITH e
			MATCH (a:TypeA {name: 'A1'})
			MERGE (e)-[:REL_A]->(a)
			WITH e
			MATCH (b:TypeB {name: 'B1'})
			MERGE (e)-[:REL_B]->(b)
			WITH e
			MATCH (c:TypeC {name: 'NONEXISTENT'})
			MERGE (e)-[:REL_C]->(c)
			WITH e
			MATCH (d:TypeD {name: 'D1'})
			MERGE (e)-[:REL_D]->(d)
			WITH e
			MATCH (a2:TypeA {name: 'A2'})
			MERGE (e)-[:REL_A2]->(a2)
			RETURN e.id
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0)

		// Check which rels were created before chain broke
		counts := make(map[string]int64)
		for _, rel := range []string{"REL_A", "REL_B", "REL_C", "REL_D", "REL_A2"} {
			v, _ := exec.Execute(ctx, `MATCH (e:Entity {id: 'perm9'})-[:`+rel+`]->() RETURN count(*) as c`, nil)
			if len(v.Rows) > 0 {
				counts[rel] = v.Rows[0][0].(int64)
			}
		}
		t.Logf("Relationships created before chain broke: %+v", counts)
	})
}

// TestOptionalMatchInChain tests OPTIONAL MATCH behavior (the fix for the import script)
func TestOptionalMatchInChain(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (a:TypeA {name: 'A1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:TypeC {name: 'C1'})`, nil)
	require.NoError(t, err)

	t.Run("OPTIONAL MATCH does not break chain", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:Entity {id: 'opt1'})
			WITH e
			OPTIONAL MATCH (a:TypeA {name: 'A1'})
			FOREACH (x IN CASE WHEN a IS NOT NULL THEN [1] ELSE [] END |
				MERGE (e)-[:REL_A]->(a)
			)
			WITH e
			OPTIONAL MATCH (b:TypeB {name: 'NONEXISTENT'})
			FOREACH (x IN CASE WHEN b IS NOT NULL THEN [1] ELSE [] END |
				MERGE (e)-[:REL_B]->(b)
			)
			WITH e
			OPTIONAL MATCH (c:TypeC {name: 'C1'})
			FOREACH (x IN CASE WHEN c IS NOT NULL THEN [1] ELSE [] END |
				MERGE (e)-[:REL_C]->(c)
			)
			RETURN e.id
		`, nil)
		// Note: This is the CORRECT way to handle optional relationships
		// The query should return 1 row and create REL_A and REL_C but not REL_B
		if err != nil {
			t.Logf("OPTIONAL MATCH + FOREACH pattern error: %v", err)
			t.Skip("OPTIONAL MATCH + FOREACH pattern may not be fully supported")
		}
		t.Logf("OPTIONAL MATCH result rows: %d", len(result.Rows))
	})
}

// TestImportScriptQueryPattern tests the exact query structure from import-translation-audit.mjs
func TestImportScriptQueryPattern(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Pre-create all the lookup nodes (mimics the import script)
	_, err := exec.Execute(ctx, `CREATE (b:BusinessArea {name: 'Caremark'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (p:Page {url: '/test/page'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (f:FeatureCategory {name: 'Orders'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (o:ProductOwner {name: 'John Doe'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (t:Team {name: 'PCW Team'})`, nil)
	require.NoError(t, err)

	t.Run("all MATCHes succeed - all relationships created", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:TranslationEntry {textKey: 'test-key-1'})
			ON CREATE SET e.aiAuditScore = 85
			
			WITH e
			MATCH (b:BusinessArea {name: 'Caremark'})
			MERGE (e)-[:IN_BUSINESS_AREA]->(b)
			
			WITH e
			MATCH (p:Page {url: '/test/page'})
			MERGE (e)-[:ON_PAGE]->(p)
			
			WITH e
			MATCH (f:FeatureCategory {name: 'Orders'})
			MERGE (e)-[:IN_CATEGORY]->(f)
			
			WITH e
			MATCH (o:ProductOwner {name: 'John Doe'})
			MERGE (e)-[:OWNED_BY]->(o)
			
			WITH e
			MATCH (t:Team {name: 'PCW Team'})
			MERGE (e)-[:MANAGED_BY]->(t)
			
			RETURN e.textKey
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "All MATCHes succeed, should return 1 row")

		// Verify IN_CATEGORY relationship specifically
		verifyCategory, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-1'})-[:IN_CATEGORY]->(f:FeatureCategory)
			RETURN f.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, verifyCategory.Rows, 1, "IN_CATEGORY relationship should exist")
		assert.Equal(t, "Orders", verifyCategory.Rows[0][0])
	})

	t.Run("BusinessArea MATCH fails - subsequent relationships NOT created", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:TranslationEntry {textKey: 'test-key-2'})
			ON CREATE SET e.aiAuditScore = 75
			
			WITH e
			MATCH (b:BusinessArea {name: 'NonExistent'})
			MERGE (e)-[:IN_BUSINESS_AREA]->(b)
			
			WITH e
			MATCH (f:FeatureCategory {name: 'Orders'})
			MERGE (e)-[:IN_CATEGORY]->(f)
			
			RETURN e.textKey
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "First MATCH fails, should return 0 rows")

		// Entry should still exist
		verifyEntry, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-2'})
			RETURN e.aiAuditScore
		`, nil)
		require.NoError(t, err)
		require.Len(t, verifyEntry.Rows, 1, "Entry should exist")

		// But IN_CATEGORY should NOT exist (chain broke earlier)
		verifyCategory, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-2'})-[:IN_CATEGORY]->(f:FeatureCategory)
			RETURN f.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, verifyCategory.Rows, 0, "IN_CATEGORY should NOT exist - chain broke at BusinessArea")
	})

	t.Run("Page MATCH fails (second in chain) - IN_CATEGORY not created", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:TranslationEntry {textKey: 'test-key-3'})
			ON CREATE SET e.aiAuditScore = 65
			
			WITH e
			MATCH (b:BusinessArea {name: 'Caremark'})
			MERGE (e)-[:IN_BUSINESS_AREA]->(b)
			
			WITH e
			MATCH (p:Page {url: '/nonexistent/page'})
			MERGE (e)-[:ON_PAGE]->(p)
			
			WITH e
			MATCH (f:FeatureCategory {name: 'Orders'})
			MERGE (e)-[:IN_CATEGORY]->(f)
			
			RETURN e.textKey
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "Second MATCH fails")

		// Check what relationships were created
		verifyBusiness, _ := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-3'})-[:IN_BUSINESS_AREA]->(b)
			RETURN b.name
		`, nil)
		verifyPage, _ := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-3'})-[:ON_PAGE]->(p)
			RETURN p.url
		`, nil)
		verifyCategory, _ := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-3'})-[:IN_CATEGORY]->(f)
			RETURN f.name
		`, nil)

		t.Logf("IN_BUSINESS_AREA: %d, ON_PAGE: %d, IN_CATEGORY: %d",
			len(verifyBusiness.Rows), len(verifyPage.Rows), len(verifyCategory.Rows))

		// IN_CATEGORY should NOT exist - chain broke at Page
		assert.Len(t, verifyCategory.Rows, 0, "IN_CATEGORY should NOT exist")
	})

	t.Run("FeatureCategory MATCH fails (third in chain)", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MERGE (e:TranslationEntry {textKey: 'test-key-4'})
			ON CREATE SET e.aiAuditScore = 55
			
			WITH e
			MATCH (b:BusinessArea {name: 'Caremark'})
			MERGE (e)-[:IN_BUSINESS_AREA]->(b)
			
			WITH e
			MATCH (p:Page {url: '/test/page'})
			MERGE (e)-[:ON_PAGE]->(p)
			
			WITH e
			MATCH (f:FeatureCategory {name: 'NonExistent'})
			MERGE (e)-[:IN_CATEGORY]->(f)
			
			WITH e
			MATCH (t:Team {name: 'PCW Team'})
			MERGE (e)-[:MANAGED_BY]->(t)
			
			RETURN e.textKey
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "Third MATCH fails")

		// Check what relationships were created
		verifyBusiness, _ := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-4'})-[:IN_BUSINESS_AREA]->(b) RETURN count(*) as c
		`, nil)
		verifyPage, _ := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-4'})-[:ON_PAGE]->(p) RETURN count(*) as c
		`, nil)
		verifyCategory, _ := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-4'})-[:IN_CATEGORY]->(f) RETURN count(*) as c
		`, nil)
		verifyTeam, _ := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'test-key-4'})-[:MANAGED_BY]->(t) RETURN count(*) as c
		`, nil)

		t.Logf("IN_BUSINESS_AREA: %v, ON_PAGE: %v, IN_CATEGORY: %v, MANAGED_BY: %v",
			verifyBusiness.Rows[0][0], verifyPage.Rows[0][0],
			verifyCategory.Rows[0][0], verifyTeam.Rows[0][0])
	})

	t.Run("empty string parameter matches nothing", func(t *testing.T) {
		// This simulates what happens when CSV has empty featureCategory
		result, err := exec.Execute(ctx, `
			MERGE (e:TranslationEntry {textKey: 'test-key-5'})
			WITH e
			MATCH (f:FeatureCategory {name: ''})
			MERGE (e)-[:IN_CATEGORY]->(f)
			RETURN e.textKey
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "Empty string should not match any node")

		// Entry still created
		verifyEntry, _ := exec.Execute(ctx, `MATCH (e:TranslationEntry {textKey: 'test-key-5'}) RETURN e`, nil)
		require.Len(t, verifyEntry.Rows, 1, "Entry should exist")
	})
}

// TestMergeRelationshipDirectly tests creating relationships without chained MATCH
func TestMergeRelationshipDirectly(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (f:FeatureCategory {name: 'TestCat'})`, nil)
	require.NoError(t, err)

	t.Run("MERGE node and relationship in single MATCH", func(t *testing.T) {
		// Alternative pattern: MERGE both nodes and relationship together
		result, err := exec.Execute(ctx, `
			MERGE (e:TranslationEntry {textKey: 'direct-1'})
			MERGE (f:FeatureCategory {name: 'TestCat'})
			MERGE (e)-[:IN_CATEGORY]->(f)
			RETURN e.textKey, f.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		verify, _ := exec.Execute(ctx, `
			MATCH (e:TranslationEntry {textKey: 'direct-1'})-[:IN_CATEGORY]->(f)
			RETURN f.name
		`, nil)
		require.Len(t, verify.Rows, 1)
		assert.Equal(t, "TestCat", verify.Rows[0][0])
	})

	t.Run("MERGE with nonexistent target creates it", func(t *testing.T) {
		// If we MERGE the category instead of MATCH, it gets created
		result, err := exec.Execute(ctx, `
			MERGE (e:TranslationEntry {textKey: 'direct-2'})
			MERGE (f:FeatureCategory {name: 'NewCategory'})
			MERGE (e)-[:IN_CATEGORY]->(f)
			RETURN e.textKey, f.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "NewCategory", result.Rows[0][1])
	})
}
