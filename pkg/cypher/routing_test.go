package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// DELETE Query Routing Tests
// =============================================================================
// These tests ensure DELETE queries are routed to executeDelete, not executeMatch.
// Bug history: DELETE was incorrectly going to executeMatch because:
// 1. Old code checked for " DELETE " (with trailing space) but DELETE is often
//    followed by variable name (e.g., "DELETE n" not "DELETE ")
// 2. Relationship deletion wasn't being detected properly in executeDelete
// =============================================================================

func TestGh713QuotedProjectionRoutes(t *testing.T) {
	for _, route := range []string{"autocommit", "explicit transaction"} {
		for _, test := range []struct {
			query   string
			columns []string
			rows    [][]interface{}
		}{
			{"RETURN 1 AS `left,right`", []string{"left,right"}, [][]interface{}{{int64(1)}}},
			{"CALL { RETURN 1 AS `left,right` } RETURN `left,right`", []string{"left,right"}, [][]interface{}{{int64(1)}}},
			{"UNWIND [1, 2] AS value CALL { WITH value RETURN value AS `left,right` } RETURN `left,right` ORDER BY `left,right`", []string{"left,right"}, [][]interface{}{{int64(1)}, {int64(2)}}},
			{"CALL { RETURN 'escaped\\', comma' AS value } RETURN value", []string{"value"}, [][]interface{}{{"escaped', comma"}}},
			{"CALL { RETURN 1 AS value UNION ALL RETURN 1 AS value } RETURN DISTINCT value", []string{"value"}, [][]interface{}{{int64(1)}}},
			{"CALL { MATCH (n:Missing) RETURN n AS `left,right` } RETURN `left,right`", []string{"left,right"}, [][]interface{}{}},
		} {
			t.Run(route+"/"+test.query, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				if route == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(ctx, test.query, nil)
				require.NoError(t, err)
				require.Equal(t, test.columns, result.Columns)
				require.Equal(t, test.rows, result.Rows)
				if route == "explicit transaction" {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				stored, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS total", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
			})
		}
	}
}

func TestGh713CallTailColumnsUseSharedReturnPlan(t *testing.T) {
	for _, test := range []struct {
		clause  string
		columns []string
	}{
		{"RETURN", nil},
		{"RETURN *", []string{"*"}},
		{"CALL { RETURN 2 AS ignored }", nil},
		{"CALL { RETURN 2 AS ignored } RETURN ignored AS value ORDER BY value LIMIT 1", []string{"value"}},
		{"RETURN DISTINCT value", []string{"value"}},
		{"RETURN DISTINCT $value", []string{"$value"}},
		{"RETURN 1 AS `left,right`", []string{"left,right"}},
		{"RETURN 1 AS `a``b`", []string{"a`b"}},
		{"RETURN [1, 2] AS list, {value: 3} AS map", []string{"list", "map"}},
		{"RETURN count(*) AS total ORDER BY total SKIP 0 LIMIT 1", []string{"total"}},
	} {
		t.Run(test.clause, func(t *testing.T) {
			require.Equal(t, test.columns, expectedReturnColumnsFromTail(test.clause))
		})
	}
	columns := expectedReturnColumnsFromTail("RETURN 1 AS value")
	columns[0] = "changed"
	require.Equal(t, []string{"value"}, expectedReturnColumnsFromTail("RETURN 1 AS value"))
}

func TestGh713ProjectionSplittersShareQuotedLexing(t *testing.T) {
	for _, splitter := range []struct {
		name  string
		split func(string) []string
	}{
		{"RETURN", splitReturnExpressions},
		{"shared", splitTopLevelComma},
	} {
		for _, projection := range []string{
			"`left,right`", "`a``b,c`", "'escaped\\', comma'", "func([1, 2], {value: 3})",
		} {
			t.Run(splitter.name+"/"+projection, func(t *testing.T) {
				parts := splitter.split(projection + ", value")
				for index := range parts {
					parts[index] = strings.TrimSpace(parts[index])
				}
				require.Equal(t, []string{projection, "value"}, parts)
			})
		}
	}
}

// TestDeleteRouting_SimpleNode tests basic node deletion routing
func TestDeleteRouting_SimpleNode(t *testing.T) {
	query := "MATCH (n:Person) DELETE n"

	hasDelete := findKeywordIndex(query, "DELETE") > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for query: %s", query)
	}
}

// TestDeleteRouting_WithProperty tests deletion with property filter containing "Delete"
func TestDeleteRouting_WithProperty(t *testing.T) {
	// The word 'ToDelete' is inside a string literal and should be ignored
	query := "MATCH (n:Temp {name: 'ToDelete'}) DELETE n"

	deleteIdx := findKeywordIndex(query, "DELETE")

	// DELETE should be found OUTSIDE the string literal (after position 30)
	if deleteIdx <= 30 {
		t.Errorf("DELETE keyword found inside string literal at index %d, should be > 30", deleteIdx)
	}

	// Verify routing check passes
	hasDelete := deleteIdx > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for query: %s", query)
	}
}

// TestDeleteRouting_Relationship tests relationship deletion routing
func TestDeleteRouting_Relationship(t *testing.T) {
	query := "MATCH ()-[r:KNOWS]->() DELETE r"

	hasDelete := findKeywordIndex(query, "DELETE") > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for relationship query: %s", query)
	}
}

// TestDeleteRouting_DetachDelete tests DETACH DELETE routing
func TestDeleteRouting_DetachDelete(t *testing.T) {
	query := "MATCH (n:Person) DETACH DELETE n"

	hasDetachDelete := containsKeywordOutsideStrings(query, "DETACH DELETE")
	if !hasDetachDelete {
		t.Errorf("DETACH DELETE routing failed for query: %s", query)
	}
}

// TestDeleteRouting_MultipleVariables tests deletion of multiple variables
func TestDeleteRouting_MultipleVariables(t *testing.T) {
	query := "MATCH (a)-[r]->(b) DELETE a, r, b"

	hasDelete := findKeywordIndex(query, "DELETE") > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for multi-variable query: %s", query)
	}
}

// TestDeleteRouting_WithWhere tests DELETE with WHERE clause
func TestDeleteRouting_WithWhere(t *testing.T) {
	query := "MATCH (n:Person) WHERE n.age > 100 DELETE n"

	hasDelete := findKeywordIndex(query, "DELETE") > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for WHERE clause query: %s", query)
	}
}

// TestDeleteRouting_NotConfusedByStringContent ensures DELETE inside strings is ignored
func TestDeleteRouting_NotConfusedByStringContent(t *testing.T) {
	testCases := []struct {
		name  string
		query string
	}{
		{"action property", "MATCH (n {action: 'DELETE'}) DELETE n"},
		{"delete in name", "MATCH (n {name: 'DeleteMe'}) DELETE n"},
		{"delete substring", "MATCH (n:ToDelete) DELETE n"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// The DELETE keyword for the actual delete operation should be found
			// OUTSIDE any string literals
			deleteIdx := findKeywordIndex(tc.query, "DELETE")
			if deleteIdx <= 0 {
				t.Errorf("DELETE keyword not found in: %s", tc.query)
			}

			// Verify it's the correct DELETE (the operation, not string content)
			// by checking it appears after the closing paren
			closeParenIdx := strings.LastIndex(tc.query, ")")
			if deleteIdx < closeParenIdx {
				t.Errorf("DELETE found too early (idx=%d, closeParen=%d) - may be inside string: %s",
					deleteIdx, closeParenIdx, tc.query)
			}
		})
	}
}

// TestExecuteWithoutTransaction_DeleteRouting verifies the exact routing logic
func TestExecuteWithoutTransaction_DeleteRouting(t *testing.T) {
	testCases := []struct {
		name         string
		query        string
		shouldDelete bool
	}{
		{"simple node delete", "MATCH (n) DELETE n", true},
		{"node with label", "MATCH (n:Person) DELETE n", true},
		{"relationship delete", "MATCH ()-[r]->() DELETE r", true},
		{"detach delete", "MATCH (n) DETACH DELETE n", true},
		{"with where", "MATCH (n) WHERE n.x = 1 DELETE n", true},
		{"string with delete", "MATCH (n {x: 'DELETE'}) DELETE n", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hasDelete := findKeywordIndex(tc.query, "DELETE") > 0
			hasDetachDelete := containsKeywordOutsideStrings(tc.query, "DETACH DELETE")

			wouldRouteToDelete := hasDelete || hasDetachDelete

			if wouldRouteToDelete != tc.shouldDelete {
				t.Errorf("Routing mismatch for %q: got wouldRouteToDelete=%v, want %v",
					tc.query, wouldRouteToDelete, tc.shouldDelete)
			}
		})
	}
}

func TestContainsOutsideStrings_Branches(t *testing.T) {
	assert.False(t, containsOutsideStrings("MATCH (n)", ""))
	assert.True(t, containsOutsideStrings("MATCH (a)-[:R]->(b)", "->"))

	// Inside single quotes should be ignored.
	assert.False(t, containsOutsideStrings("MATCH (n {txt:'a->b'})", "->"))
	// Escaped quotes branch.
	assert.False(t, containsOutsideStrings("MATCH (n {txt:'a\\'->\\'b'})", "->"))
	// Doubled single-quote escape branch.
	assert.False(t, containsOutsideStrings("MATCH (n {txt:'a''->''b'})", "->"))

	// Inside double quotes should be ignored.
	assert.False(t, containsOutsideStrings("MATCH (n {txt:\"a->b\"})", "->"))
	// Backtick identifier branch.
	assert.False(t, containsOutsideStrings("MATCH (`a->b`)-[:R]->(n)", "a->b"))

	// Line comment branch.
	assert.False(t, containsOutsideStrings("MATCH (n) // -> in comment\nRETURN n", "->"))
	// Block comment branch.
	assert.False(t, containsOutsideStrings("MATCH (n) /* -> in comment */ RETURN n", "->"))
}

func TestRouting_ExactTranslationQuery_ExecutesViaMatchPath(t *testing.T) {
	const cypher = "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE t.language = 'fr' RETURN o, t, t.createdAt ORDER BY t.createdAt DESC LIMIT 10"

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	err := error(nil)
	seedTranslationQueryFamilyData(t, store, "", 15, 1)
	require.NoError(t, err)

	result, err := exec.executeWithoutTransaction(ctx, cypher, strings.ToUpper(cypher))
	require.NoError(t, err)
	require.Len(t, result.Rows, 10)
	require.Equal(t, []string{"o", "t", "t.createdAt"}, result.Columns)

	for i, row := range result.Rows {
		require.Len(t, row, 3)

		original, ok := row[0].(*storage.Node)
		require.True(t, ok)
		require.Contains(t, original.Labels, "OriginalText")

		translated, ok := row[1].(*storage.Node)
		require.True(t, ok)
		require.Contains(t, translated.Labels, "TranslatedText")
		require.Equal(t, "fr", translated.Properties["language"])

		createdAt, ok := row[2].(string)
		require.True(t, ok)
		require.Equal(t, translated.Properties["createdAt"], createdAt)

		if i > 0 {
			prev := result.Rows[i-1][2].(string)
			require.GreaterOrEqual(t, prev, createdAt)
		}
	}

	require.Equal(t, "2026-04-15T12:00:00Z", result.Rows[0][2])
	require.Equal(t, "2026-04-06T12:00:00Z", result.Rows[9][2])
}

func TestRouting_TranslationQueryFamily_ExecutesViaMatchPath(t *testing.T) {
	queries := []struct {
		name      string
		cypher    string
		wantCols  []string
		wantRows  int
		firstText string
	}{
		{
			name:      "exact shape",
			cypher:    "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE t.language = 'fr' RETURN o, t, t.createdAt ORDER BY t.createdAt DESC LIMIT 10",
			wantCols:  []string{"o", "t", "t.createdAt"},
			wantRows:  10,
			firstText: "fr-11",
		},
		{
			name:      "without projected sort key",
			cypher:    "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE t.language = 'fr' RETURN o, t ORDER BY t.createdAt DESC LIMIT 5",
			wantCols:  []string{"o", "t"},
			wantRows:  5,
			firstText: "fr-11",
		},
		{
			name:      "projected aliases",
			cypher:    "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE t.language = 'fr' RETURN o.textKey AS textKey, t.createdAt AS createdAt ORDER BY t.createdAt DESC LIMIT 3",
			wantCols:  []string{"textKey", "createdAt"},
			wantRows:  3,
			firstText: "fr-11",
		},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			base := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(base, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			seedTranslationQueryFamilyData(t, store, "", 12, 0)

			result, err := exec.executeWithoutTransaction(ctx, tc.cypher, strings.ToUpper(tc.cypher))
			require.NoError(t, err)
			require.Equal(t, tc.wantCols, result.Columns)
			require.Len(t, result.Rows, tc.wantRows)

			switch first := result.Rows[0][0].(type) {
			case *storage.Node:
				require.Equal(t, tc.firstText, first.Properties["textKey"])
			case string:
				require.Equal(t, tc.firstText, first)
			default:
				t.Fatalf("unexpected first column type %T", result.Rows[0][0])
			}
		})
	}
}
