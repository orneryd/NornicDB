package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRelationshipPropertyFilter tests inline property filters on relationship traversal
// Bug discovered: MATCH (a)-[:REL]->(b:Label {name: 'value'}) returns 0 when value contains special chars
func TestRelationshipPropertyFilter(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: TranslationEntry nodes with HAS_ISSUE relationships to IssueType nodes
	issueTypes := []struct {
		id   string
		name string
	}{
		{"issue-1", "Other Issue"},
		{"issue-2", "Informal Register (tú)"},
		{"issue-3", "Non-Standard Terminology"},
		{"issue-4", "Untranslated Term"},
	}

	for _, it := range issueTypes {
		node := &storage.Node{
			ID:         storage.NodeID(it.id),
			Labels:     []string{"IssueType"},
			Properties: map[string]interface{}{"name": it.name},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// Create translation entries
	entries := []struct {
		id        string
		issueRefs []string // which issue types this entry links to
	}{
		{"entry-1", []string{"issue-1"}},
		{"entry-2", []string{"issue-1", "issue-2"}},
		{"entry-3", []string{"issue-2"}},
		{"entry-4", []string{"issue-2", "issue-3"}},
		{"entry-5", []string{"issue-3"}},
		{"entry-6", []string{"issue-4"}},
	}

	for _, e := range entries {
		node := &storage.Node{
			ID:         storage.NodeID(e.id),
			Labels:     []string{"TranslationEntry"},
			Properties: map[string]interface{}{"textKey": e.id},
		}
		require.NoError(t, store.CreateNode(node))

		for i, issueRef := range e.issueRefs {
			edge := &storage.Edge{
				ID:         storage.EdgeID(e.id + "-HAS_ISSUE-" + issueRef),
				Type:       "HAS_ISSUE",
				StartNode:  storage.NodeID(e.id),
				EndNode:    storage.NodeID(issueRef),
				Properties: map[string]interface{}{"index": i},
			}
			require.NoError(t, store.CreateEdge(edge))
		}
	}

	t.Run("inline property filter - simple name", func(t *testing.T) {
		// MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType {name: 'Other Issue'})
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType {name: 'Other Issue'})
			RETURN count(e) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// entry-1 and entry-2 link to "Other Issue"
		assert.Equal(t, int64(2), result.Rows[0][0], "Should find 2 entries with 'Other Issue'")
	})

	t.Run("inline property filter - special chars (parentheses and accented)", func(t *testing.T) {
		// MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType {name: 'Informal Register (tú)'})
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType {name: 'Informal Register (tú)'})
			RETURN count(e) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// entry-2, entry-3, entry-4 link to "Informal Register (tú)"
		assert.Equal(t, int64(3), result.Rows[0][0], "Should find 3 entries with 'Informal Register (tú)'")
	})

	t.Run("inline property filter - hyphenated name", func(t *testing.T) {
		// MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType {name: 'Non-Standard Terminology'})
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType {name: 'Non-Standard Terminology'})
			RETURN count(e) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// entry-4 and entry-5 link to "Non-Standard Terminology"
		assert.Equal(t, int64(2), result.Rows[0][0], "Should find 2 entries with 'Non-Standard Terminology'")
	})

	t.Run("WHERE clause equality - simple name", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType)
			WHERE i.name = 'Other Issue'
			RETURN count(e) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(2), result.Rows[0][0], "WHERE clause should filter to 2 entries")
	})

	t.Run("WHERE clause equality - special chars", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType)
			WHERE i.name = 'Informal Register (tú)'
			RETURN count(e) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(3), result.Rows[0][0], "WHERE clause should filter to 3 entries with special chars")
	})

	t.Run("inline filter returns actual data not just count", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType {name: 'Untranslated Term'})
			RETURN e.textKey, i.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "entry-6", result.Rows[0][0])
		assert.Equal(t, "Untranslated Term", result.Rows[0][1])
	})

	t.Run("group by issue type with inline filter", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType)
			WITH i.name as issueName, count(e) as cnt
			RETURN issueName, cnt
			ORDER BY cnt DESC
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 4)
		// "Informal Register (tú)" should have 3 entries (highest count)
	})
}

// TestCategoryRelationshipTraversal tests IN_CATEGORY style relationship patterns
func TestCategoryRelationshipTraversal(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create categories
	categories := []string{"Orders", "Cart & Checkout", "Prescription List Page"}
	for i, name := range categories {
		node := &storage.Node{
			ID:         storage.NodeID("cat-" + string(rune('a'+i))),
			Labels:     []string{"FeatureCategory"},
			Properties: map[string]interface{}{"name": name},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// Create entries with category relationships
	entries := []struct {
		id       string
		category string
		score    int
	}{
		{"e1", "cat-a", 85}, // Orders
		{"e2", "cat-a", 90}, // Orders
		{"e3", "cat-b", 75}, // Cart & Checkout
		{"e4", "cat-c", 95}, // Prescription List Page
		{"e5", "cat-c", 80}, // Prescription List Page
	}

	for _, e := range entries {
		node := &storage.Node{
			ID:         storage.NodeID(e.id),
			Labels:     []string{"TranslationEntry"},
			Properties: map[string]interface{}{"aiAuditScore": e.score},
		}
		require.NoError(t, store.CreateNode(node))

		edge := &storage.Edge{
			ID:        storage.EdgeID(e.id + "-IN_CATEGORY"),
			Type:      "IN_CATEGORY",
			StartNode: storage.NodeID(e.id),
			EndNode:   storage.NodeID(e.category),
		}
		require.NoError(t, store.CreateEdge(edge))
	}

	t.Run("count IN_CATEGORY relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:IN_CATEGORY]->(f:FeatureCategory)
			RETURN count(e) as total
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(5), result.Rows[0][0])
	})

	t.Run("avg score by category", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:IN_CATEGORY]->(f:FeatureCategory)
			RETURN f.name as category, avg(e.aiAuditScore) as avgScore, count(e) as total
			ORDER BY avgScore ASC
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 3)
	})

	t.Run("filter by category name with special chars", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:IN_CATEGORY]->(f:FeatureCategory {name: 'Cart & Checkout'})
			RETURN count(e) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(1), result.Rows[0][0])
	})
}

// TestSpecialCharacterPropertyValues tests various special characters in property values
func TestSpecialCharacterPropertyValues(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	specialValues := []struct {
		id    string
		value string
	}{
		{"sp-1", "Simple Value"},
		{"sp-2", "Value (with parentheses)"},
		{"sp-3", "Value with tú accented"},
		{"sp-4", "Value & ampersand"},
		{"sp-5", "Value - hyphen"},
		{"sp-6", "Value: colon"},
		{"sp-7", "Value's apostrophe"},
		{"sp-8", "Value / slash"},
		{"sp-9", "Español (tú/usted)"},
	}

	for _, sv := range specialValues {
		node := &storage.Node{
			ID:         storage.NodeID(sv.id),
			Labels:     []string{"SpecialChar"},
			Properties: map[string]interface{}{"value": sv.value},
		}
		require.NoError(t, store.CreateNode(node))
	}

	for _, sv := range specialValues {
		t.Run("match "+sv.id, func(t *testing.T) {
			result, err := exec.Execute(ctx, `
				MATCH (n:SpecialChar {value: $val})
				RETURN n.value
			`, map[string]interface{}{"val": sv.value})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1, "Should find node with value: %s", sv.value)
			assert.Equal(t, sv.value, result.Rows[0][0])
		})
	}

	t.Run("inline filter with parentheses and accent", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:SpecialChar {value: 'Español (tú/usted)'})
			RETURN n.value
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "Español (tú/usted)", result.Rows[0][0])
	})

	t.Run("WHERE with parentheses and accent", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:SpecialChar)
			WHERE n.value = 'Español (tú/usted)'
			RETURN n.value
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "Español (tú/usted)", result.Rows[0][0])
	})

	t.Run("WHERE CONTAINS with special chars", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:SpecialChar)
			WHERE n.value CONTAINS 'tú'
			RETURN n.value
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2) // "Value with tú accented" and "Español (tú/usted)"
	})
}

// TestMultipleRelationshipPatterns tests complex patterns with multiple relationship types
func TestMultipleRelationshipPatterns(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create: Team -> Entry -> IssueType
	//                 |-> Category

	team := &storage.Node{
		ID:         "team-1",
		Labels:     []string{"Team"},
		Properties: map[string]interface{}{"name": "PCW - Orders"},
	}
	require.NoError(t, store.CreateNode(team))

	category := &storage.Node{
		ID:         "category-1",
		Labels:     []string{"FeatureCategory"},
		Properties: map[string]interface{}{"name": "Orders (Guest)"},
	}
	require.NoError(t, store.CreateNode(category))

	issueType := &storage.Node{
		ID:         "issue-type-1",
		Labels:     []string{"IssueType"},
		Properties: map[string]interface{}{"name": "Informal Register (tú)"},
	}
	require.NoError(t, store.CreateNode(issueType))

	entry := &storage.Node{
		ID:     "entry-1",
		Labels: []string{"TranslationEntry"},
		Properties: map[string]interface{}{
			"textKey":      "abc123",
			"aiAuditScore": 80,
		},
	}
	require.NoError(t, store.CreateNode(entry))

	// Create relationships
	edges := []*storage.Edge{
		{ID: "e1", Type: "MANAGED_BY", StartNode: "entry-1", EndNode: "team-1"},
		{ID: "e2", Type: "IN_CATEGORY", StartNode: "entry-1", EndNode: "category-1"},
		{ID: "e3", Type: "HAS_ISSUE", StartNode: "entry-1", EndNode: "issue-type-1"},
	}
	for _, e := range edges {
		require.NoError(t, store.CreateEdge(e))
	}

	t.Run("count issues by type per team", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:MANAGED_BY]->(t:Team)
			MATCH (e)-[:HAS_ISSUE]->(i:IssueType)
			WHERE e.aiAuditScore < 90
			RETURN t.name as team, i.name as issueType, count(e) as count
			ORDER BY team, count DESC
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "PCW - Orders", result.Rows[0][0])
		assert.Equal(t, "Informal Register (tú)", result.Rows[0][1])
		assert.Equal(t, int64(1), result.Rows[0][2])
	})

	t.Run("filter by issue type with special chars in multi-match", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:TranslationEntry)-[:HAS_ISSUE]->(i:IssueType {name: 'Informal Register (tú)'})
			MATCH (e)-[:IN_CATEGORY]->(c:FeatureCategory)
			RETURN e.textKey, c.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "abc123", result.Rows[0][0])
		assert.Equal(t, "Orders (Guest)", result.Rows[0][1])
	})
}
