package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestParseLeadingWithImportsClauseKeywordFirst: a clause keyword at the
// very start of a subquery's import list belongs to the list; the list ends
// at the next clause keyword (as when each keyword was scanned alone).
func TestParseLeadingWithImportsClauseKeywordFirst(t *testing.T) {
	vars, body, hasWith, err := parseLeadingWithImports("WITH MATCH (n) RETURN n")
	require.NoError(t, err)
	require.True(t, hasWith)
	require.Equal(t, []string{"MATCH (n)"}, vars)
	require.Equal(t, "RETURN n", body)
}

// TestWithExpressionFailureSlotOutsideAStatement: outside a running
// statement a context gets its own expression-failure slot, which it then
// keeps.
func TestWithExpressionFailureSlotOutsideAStatement(t *testing.T) {
	ctx := withExpressionFailureSlot(context.Background())
	require.NotNil(t, ctx.Value(expressionFailureKey{}))
	require.Equal(t, ctx, withExpressionFailureSlot(ctx))
}

// TestStartsWithKeywordsEmptyWord: an empty word matches nothing, as in
// findMultiWordKeywordIndex.
func TestStartsWithKeywordsEmptyWord(t *testing.T) {
	require.False(t, startsWithKeywords("CREATE DATABASE x", "", "DATABASE"))
	require.False(t, startsWithKeywords("CREATE DATABASE x", "CREATE", " "))
	require.Equal(t, -1, findMultiWordKeywordIndex("CREATE DATABASE x", "", "DATABASE"))
}

// TestFirstKeywordIndexFromDefaultKeywordLimit: the keyword set is bounded.
func TestFirstKeywordIndexFromDefaultKeywordLimit(t *testing.T) {
	keywords := make([]string, maxKeywordSet+1)
	for i := range keywords {
		keywords[i] = "MATCH"
	}
	require.Panics(t, func() { firstKeywordIndexFromDefault("MATCH (n) RETURN n", 0, keywords...) })
}

// recordingAccessRecorder records the entities a walk reports.
type recordingAccessRecorder struct{ ids []string }

func (r *recordingAccessRecorder) RecordMaterializedAccess(id string) { r.ids = append(r.ids, id) }

// TestMaterializedValueShapes: the access walk finds a node or relationship
// as a *storage.Node / *storage.Edge, as a map with _nodeId / _edgeId, and
// inside maps, lists and lists of maps; nil entities and scalars are none.
// Without a recorder it only reports whether there is one.
func TestMaterializedValueShapes(t *testing.T) {
	var nilNode *storage.Node
	var nilEdge *storage.Edge
	for _, value := range []interface{}{nilNode, nilEdge, "text", int64(1), map[string]interface{}{"a": 1}, []interface{}{}, []map[string]interface{}{{"a": 1}}} {
		require.False(t, materializedValue(nil, value), "%#v", value)
	}
	for _, tc := range []struct {
		value interface{}
		ids   []string
	}{
		{&storage.Node{ID: "n1"}, []string{"n1"}},
		{&storage.Edge{ID: "e1"}, []string{"e1"}},
		{map[string]interface{}{"_nodeId": "n2"}, []string{"n2"}},
		{map[string]interface{}{"_edgeId": "e2"}, []string{"e2"}},
		{map[string]interface{}{"x": &storage.Node{ID: "n3"}}, []string{"n3"}},
		{[]interface{}{&storage.Node{ID: "n4"}, "s"}, []string{"n4"}},
		{[]map[string]interface{}{{"_nodeId": "n5"}, {"_edgeId": "e5"}}, []string{"n5", "e5"}},
	} {
		require.True(t, materializedValue(nil, tc.value), "%#v", tc.value)
		recorder := &recordingAccessRecorder{}
		require.True(t, materializedValue(recorder, tc.value), "%#v", tc.value)
		require.Equal(t, tc.ids, recorder.ids, "%#v", tc.value)
	}
	require.True(t, resultHasMaterializedEntities(&ExecuteResult{Rows: [][]interface{}{{"a"}, {&storage.Edge{ID: "e"}}}}))
	require.False(t, resultHasMaterializedEntities(&ExecuteResult{Rows: [][]interface{}{{"a", int64(1)}}}))
	require.False(t, resultHasMaterializedEntities(nil))
}
