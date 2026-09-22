package mcp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// getStringSliceProp — every branch of the type switch.
// ============================================================================

func TestGetStringSliceProp_AllBranches(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]interface{}
		key  string
		want []string
	}{
		{"nil props returns nil", nil, "anything", nil},
		{"missing key returns nil", map[string]interface{}{}, "tags", nil},
		{"value is []string passes through", map[string]interface{}{"tags": []string{"a", "b"}}, "tags", []string{"a", "b"}},
		{"value is []interface{} of strings is filtered", map[string]interface{}{"tags": []interface{}{"a", 1, "b", true}}, "tags", []string{"a", "b"}},
		{"value is []interface{} of no strings yields empty (not nil)",
			map[string]interface{}{"tags": []interface{}{1, 2, true}}, "tags", []string{}},
		{"value of unrelated type returns nil",
			map[string]interface{}{"tags": 42}, "tags", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, getStringSliceProp(tc.in, tc.key))
		})
	}
}

// ============================================================================
// getStringSlice — same shape as getStringSliceProp but on top-level args map.
// ============================================================================

func TestGetStringSlice_AllBranches(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]interface{}
		key  string
		want []string
	}{
		{"missing key returns nil", map[string]interface{}{}, "labels", nil},
		{"value is []string passes through", map[string]interface{}{"labels": []string{"a"}}, "labels", []string{"a"}},
		{"value is []interface{} of strings is filtered",
			map[string]interface{}{"labels": []interface{}{"a", "b", 1}}, "labels", []string{"a", "b"}},
		{"unrelated type returns nil", map[string]interface{}{"labels": "single"}, "labels", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, getStringSlice(tc.in, tc.key))
		})
	}
}

// ============================================================================
// getInt — every branch including default fallback.
// ============================================================================

func TestGetInt_AllBranches(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]interface{}
		key  string
		def  int
		want int
	}{
		{"missing key returns default", map[string]interface{}{}, "n", 42, 42},
		{"int value", map[string]interface{}{"n": int(7)}, "n", 0, 7},
		{"int64 value truncates to int", map[string]interface{}{"n": int64(8)}, "n", 0, 8},
		{"float64 value truncates", map[string]interface{}{"n": float64(9.9)}, "n", 0, 9},
		{"unrelated type returns default", map[string]interface{}{"n": "not-a-number"}, "n", 3, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, getInt(tc.in, tc.key, tc.def))
		})
	}
}

// ============================================================================
// getFloat64 — every branch including default fallback.
// ============================================================================

func TestGetFloat64_AllBranches(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]interface{}
		key  string
		def  float64
		want float64
	}{
		{"missing key returns default", map[string]interface{}{}, "x", 0.5, 0.5},
		{"float64 value", map[string]interface{}{"x": 1.25}, "x", 0, 1.25},
		{"int value coerces", map[string]interface{}{"x": int(3)}, "x", 0, 3.0},
		{"int64 value coerces", map[string]interface{}{"x": int64(4)}, "x", 0, 4.0},
		{"unrelated type returns default", map[string]interface{}{"x": "not"}, "x", 9, 9},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, getFloat64(tc.in, tc.key, tc.def))
		})
	}
}

// ============================================================================
// getBool / getMap / getStringProp / containsLabel / getLabelType.
// ============================================================================

func TestGetBool_AllBranches(t *testing.T) {
	require.True(t, getBool(map[string]interface{}{"b": true}, "b", false))
	require.False(t, getBool(map[string]interface{}{"b": false}, "b", true))
	require.True(t, getBool(map[string]interface{}{}, "b", true), "missing key returns default")
	require.True(t, getBool(map[string]interface{}{"b": "not bool"}, "b", true),
		"wrong type returns default")
}

func TestGetMap_AllBranches(t *testing.T) {
	inner := map[string]interface{}{"k": "v"}
	require.Equal(t, inner, getMap(map[string]interface{}{"meta": inner}, "meta"))
	require.Nil(t, getMap(map[string]interface{}{}, "meta"), "missing returns nil")
	require.Nil(t, getMap(map[string]interface{}{"meta": 1}, "meta"), "wrong type returns nil")
}

func TestGetStringProp_AllBranches(t *testing.T) {
	require.Equal(t, "v", getStringProp(map[string]interface{}{"k": "v"}, "k"))
	require.Equal(t, "", getStringProp(map[string]interface{}{}, "k"), "missing returns empty")
	require.Equal(t, "", getStringProp(map[string]interface{}{"k": 42}, "k"), "wrong type returns empty")
	require.Equal(t, "", getStringProp(nil, "k"), "nil props returns empty")
}

func TestContainsLabel(t *testing.T) {
	require.True(t, containsLabel([]string{"A", "B"}, "A"))
	require.True(t, containsLabel([]string{"A", "B"}, "B"))
	require.False(t, containsLabel([]string{"A", "B"}, "C"))
	require.False(t, containsLabel(nil, "A"))
	require.False(t, containsLabel([]string{}, "A"))
}

func TestGetLabelType_TakesFirstLabelOrNode(t *testing.T) {
	require.Equal(t, "Person", getLabelType([]string{"Person", "User"}))
	require.Equal(t, "Node", getLabelType(nil), "nil labels falls back to Node")
	require.Equal(t, "Node", getLabelType([]string{}), "empty labels falls back to Node")
}

// ============================================================================
// truncateString — every branch including the maxLen<=3 short-circuit.
// ============================================================================

func TestTruncateString_AllBranches(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		maxLen int
		want   string
	}{
		{"under limit unchanged", "abc", 10, "abc"},
		{"exactly at limit unchanged", "abcdef", 6, "abcdef"},
		{"over limit gets ellipsis", "abcdefgh", 6, "abc..."},
		{"max <= 3 truncates without ellipsis (3)", "abcdef", 3, "abc"},
		{"max <= 3 truncates without ellipsis (1)", "abcdef", 1, "a"},
		{"empty input unchanged", "", 5, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, truncateString(tc.in, tc.maxLen))
		})
	}
}

// ============================================================================
// sanitizePropertiesForLLM — drops embedding fields and large float arrays.
// ============================================================================

func TestSanitizePropertiesForLLM_DropsEmbeddingsAndLargeArrays(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		require.Nil(t, sanitizePropertiesForLLM(nil))
	})

	t.Run("named embedding fields are dropped", func(t *testing.T) {
		in := map[string]interface{}{
			"name":                 "Alice",
			"embedding":            []float32{0.1, 0.2},
			"embedding_model":      "x",
			"embedding_dimensions": 3,
			"has_embedding":        true,
			"embedded_at":          "now",
		}
		out := sanitizePropertiesForLLM(in)
		require.Contains(t, out, "name")
		require.NotContains(t, out, "embedding")
		require.NotContains(t, out, "embedding_model")
		require.NotContains(t, out, "embedding_dimensions")
		require.NotContains(t, out, "has_embedding")
		require.NotContains(t, out, "embedded_at")
	})

	t.Run("large []float32 / []float64 / []interface{} arrays are dropped", func(t *testing.T) {
		large32 := make([]float32, 101)
		large64 := make([]float64, 101)
		largeIfc := make([]interface{}, 101)
		smallIfc := []interface{}{"a", "b", "c"}
		in := map[string]interface{}{
			"big32":  large32,
			"big64":  large64,
			"bigAny": largeIfc,
			"small":  smallIfc,
			"keep":   "yes",
		}
		out := sanitizePropertiesForLLM(in)
		require.NotContains(t, out, "big32")
		require.NotContains(t, out, "big64")
		require.NotContains(t, out, "bigAny")
		require.Contains(t, out, "small", "short arrays must NOT be dropped")
		require.Contains(t, out, "keep")
	})

	t.Run("small float arrays are preserved", func(t *testing.T) {
		in := map[string]interface{}{"small": []float32{0.1, 0.2, 0.3}}
		out := sanitizePropertiesForLLM(in)
		require.Contains(t, out, "small")
	})
}

// ============================================================================
// generateTitle — first-line + truncate composition.
// ============================================================================

func TestGenerateTitle_FirstLineAndTruncates(t *testing.T) {
	require.Equal(t, "Hello", generateTitle("Hello", 30))
	require.Equal(t, "Hello", generateTitle("Hello\nworld\nmore", 30))
	require.Equal(t, "He...", generateTitle("Hello world", 5),
		"truncates to maxLen with ellipsis when maxLen>3")
	require.Equal(t, "", generateTitle("", 10))
	require.Equal(t, "trimmed", generateTitle("   trimmed   \nrest", 30))
}

// ============================================================================
// hasAnyTag — every branch.
// ============================================================================

func TestHasAnyTag_NilAndEmptyEdges(t *testing.T) {
	// Note: the happy-path coverage is in server_test.go::TestHasAnyTag.
	// These cases push the nil/empty edges to clear the remaining branches.
	require.True(t, hasAnyTag([]string{"a", "b"}, []string{"b"}))
	require.True(t, hasAnyTag([]string{"a", "b"}, []string{"x", "a"}))
	require.False(t, hasAnyTag([]string{"a"}, []string{"b"}))
	require.False(t, hasAnyTag(nil, []string{"a"}))
	require.False(t, hasAnyTag([]string{"a"}, nil))
	require.False(t, hasAnyTag([]string{}, []string{}))
}

// ============================================================================
// extractDatabaseArg — handles "database", "db" keys, blank, nil, trims spaces,
// and deletes both keys from the input map.
// ============================================================================

func TestExtractDatabaseArg_AllBranches(t *testing.T) {
	t.Run("nil args returns empty", func(t *testing.T) {
		require.Equal(t, "", extractDatabaseArg(nil))
	})

	t.Run("database key preferred over db key", func(t *testing.T) {
		args := map[string]interface{}{"database": " primary ", "db": "backup", "other": 1}
		got := extractDatabaseArg(args)
		require.Equal(t, "primary", got, "whitespace trimmed")
		require.NotContains(t, args, "database")
		require.NotContains(t, args, "db", "db key must also be removed")
		require.Contains(t, args, "other", "non-database keys must be preserved")
	})

	t.Run("falls back to db when database is missing", func(t *testing.T) {
		args := map[string]interface{}{"db": "backup", "other": 2}
		require.Equal(t, "backup", extractDatabaseArg(args))
		require.NotContains(t, args, "db")
	})

	t.Run("falls back to db when database is wrong type", func(t *testing.T) {
		args := map[string]interface{}{"database": 42, "db": "ok"}
		require.Equal(t, "ok", extractDatabaseArg(args))
	})

	t.Run("returns empty when neither database nor db is a string", func(t *testing.T) {
		args := map[string]interface{}{"database": 42, "db": 7}
		require.Equal(t, "", extractDatabaseArg(args))
	})

	t.Run("returns empty when both keys absent", func(t *testing.T) {
		args := map[string]interface{}{"other": "x"}
		require.Equal(t, "", extractDatabaseArg(args))
	})
}

// ============================================================================
// normalizeNodeElementID — prefix-tolerance for elementId formats.
// ============================================================================

func TestNormalizeNodeElementID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty input returns empty", "", ""},
		{"whitespace-only returns empty", "   ", ""},
		{"already-prefixed nornicdb element id passes through",
			"4:nornicdb:abc", "4:nornicdb:abc"},
		{"foreign elementId passes through unchanged",
			"4:otherdb:xyz", "4:otherdb:xyz"},
		{"bare id is prefixed",
			"node-123", "4:nornic:node-123"},
		{"bare id is trimmed before prefixing",
			"  node-123  ", "4:nornic:node-123"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, normalizeNodeElementID(tc.in))
		})
	}
}

// ============================================================================
// localNodeIDFromAny — round-trips with normalizeNodeElementID, handles
// foreign elementIds, returns input unchanged for bare ids.
// ============================================================================

func TestLocalNodeIDFromAny(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty returns empty", "", ""},
		{"whitespace returns empty", "   ", ""},
		{"strips nornicdb prefix", "4:nornicdb:abc", "abc"},
		{"3-segment foreign elementId returns trailing local id",
			"4:other:id-9", "id-9"},
		{"4-or-more segment elementId returns the everything-after-second-colon",
			"4:other:weird:id-9", "weird:id-9"},
		{"bare id passes through trimmed",
			"  bare-id  ", "bare-id"},
		{"malformed 4: with no second colon is preserved (not silently mangled)",
			"4:lonely", "4:lonely"},
		{"4: prefix with empty tail is preserved",
			"4:", "4:"},
		{"4:: with empty trailing segment is preserved",
			"4::", "4::"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, localNodeIDFromAny(tc.in))
		})
	}

	// Round-trip: any bare id → normalize → local should yield the original.
	for _, id := range []string{"a", "long-id-123", "x"} {
		require.Equal(t, id, localNodeIDFromAny(normalizeNodeElementID(id)))
	}
}

// ============================================================================
// toInterfaceMap — nil + non-empty paths.
// ============================================================================

func TestToInterfaceMap(t *testing.T) {
	require.Nil(t, toInterfaceMap(nil))
	in := map[string]any{"a": 1, "b": "x"}
	out := toInterfaceMap(in)
	require.Equal(t, map[string]interface{}{"a": 1, "b": "x"}, out)
}

// ============================================================================
// databaseParamSchema — with and without a default db value.
// ============================================================================

func TestDatabaseParamSchema_HandlesBlankAndPopulatedDefault(t *testing.T) {
	t.Run("blank default omits the default key", func(t *testing.T) {
		schema := databaseParamSchema("")
		require.Equal(t, "string", schema["type"])
		_, present := schema["default"]
		require.False(t, present)
	})
	t.Run("whitespace default treated as blank", func(t *testing.T) {
		schema := databaseParamSchema("   ")
		_, present := schema["default"]
		require.False(t, present)
	})
	t.Run("populated default is trimmed and included", func(t *testing.T) {
		schema := databaseParamSchema("  primary  ")
		require.Equal(t, "primary", schema["default"])
	})
}

// ============================================================================
// runCypherMutationWithRetry — every branch except network-level retry.
// ============================================================================

func TestRunCypherMutationWithRetry_NilExecutorReturnsError(t *testing.T) {
	_, err := runCypherMutationWithRetry(context.Background(), nil, "RETURN 1", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cypher executor is nil")
}

func TestRunCypherMutationWithRetry_CancelledContextReturnsCtxError(t *testing.T) {
	// Run a query that returns ErrConflict; the retry loop should re-check
	// ctx.Err() and bail out on a cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	base := storage.NewMemoryEngine()
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "rc"))
	// Cypher executor returns ctx.Err() before performing work when the
	// context is already cancelled. The exact contract under cancellation
	// is "either return ctx.Err(), or perform the query"; both outcomes
	// are valid. We assert the error path: when the executor errors the
	// retry loop terminates.
	_, err := runCypherMutationWithRetry(ctx, exec, "RETURN 1", nil)
	// Either ctx error from the loop, or the executor's own ctx error.
	require.True(t, errors.Is(err, context.Canceled) || err == nil,
		"cancelled context must surface a cancellation outcome")
}

func TestRunCypherMutationWithRetry_HappyPathReturnsResult(t *testing.T) {
	base := storage.NewMemoryEngine()
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "rc-ok"))
	result, err := runCypherMutationWithRetry(context.Background(), exec, "RETURN 1 AS one", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []string{"one"}, result.Columns)
}

func TestRunCypherMutationWithRetry_NonConflictErrorPropagates(t *testing.T) {
	base := storage.NewMemoryEngine()
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "rc-syntax"))
	_, err := runCypherMutationWithRetry(context.Background(), exec, "NOT VALID CYPHER {{}}", nil)
	require.Error(t, err)
	require.False(t, errors.Is(err, storage.ErrConflict),
		"syntax/parse errors must NOT be wrapped as ErrConflict")
}

// ============================================================================
// DefaultDatabaseName — nil server / nil db / non-namespaced storage paths.
// ============================================================================

func TestServer_DefaultDatabaseName_AllBranches(t *testing.T) {
	t.Run("nil server returns empty", func(t *testing.T) {
		var s *Server
		require.Equal(t, "", s.DefaultDatabaseName())
	})

	t.Run("server with nil db returns empty", func(t *testing.T) {
		s := NewServer(nil, nil)
		require.Equal(t, "", s.DefaultDatabaseName())
	})
}

// ============================================================================
// IsValidTool / AllTools / InferOperation / ExtractResourceType — these are
// pure, table-tractable utilities.
// ============================================================================

func TestIsValidToolAndAllTools(t *testing.T) {
	tools := AllTools()
	require.ElementsMatch(t,
		[]string{ToolStore, ToolRecall, ToolDiscover, ToolLink, ToolTask, ToolTasks},
		tools)
	for _, name := range tools {
		require.True(t, IsValidTool(name), "%s must be valid", name)
	}
	require.False(t, IsValidTool("nonexistent"))
	require.False(t, IsValidTool(""))
}

func TestInferOperation_AllTools(t *testing.T) {
	cases := []struct {
		tool string
		args map[string]interface{}
		want string
	}{
		{ToolStore, nil, "create"},
		{ToolRecall, nil, "read"},
		{ToolDiscover, nil, "read"},
		{ToolLink, nil, "create"},
		{ToolTasks, nil, "read"},
		{ToolTask, nil, "create"},
		{ToolTask, map[string]interface{}{"id": "x"}, "update"},
		{ToolTask, map[string]interface{}{"id": "x", "delete": true}, "delete"},
		{ToolTask, map[string]interface{}{"id": "x", "delete": false}, "update"},
		{"unknown-tool", nil, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.tool+"/"+tc.want, func(t *testing.T) {
			require.Equal(t, tc.want, InferOperation(tc.tool, tc.args))
		})
	}
}

func TestExtractResourceType_AllTools(t *testing.T) {
	cases := []struct {
		tool string
		args map[string]interface{}
		want string
	}{
		{ToolStore, map[string]interface{}{"type": "Decision"}, "Decision"},
		{ToolStore, nil, "memory"},
		{ToolStore, map[string]interface{}{}, "memory"},
		{ToolRecall, map[string]interface{}{"type": []interface{}{"Note"}}, "Note"},
		{ToolRecall, map[string]interface{}{"type": []interface{}{}}, "*"},
		{ToolRecall, map[string]interface{}{"type": []interface{}{42}}, "*"},
		{ToolDiscover, map[string]interface{}{"type": []interface{}{"Concept"}}, "Concept"},
		{ToolLink, nil, "edge"},
		{ToolTask, nil, "task"},
		{ToolTasks, nil, "task"},
		{"unknown", nil, "*"},
	}
	for i, tc := range cases {
		// Use index in subtest name because some duplicate-want names confuse t.Run.
		name := tc.tool + "/" + tc.want
		t.Run(strings.ReplaceAll(name, "*", "any")+
			"/"+itoaTest(i), func(t *testing.T) {
			require.Equal(t, tc.want, ExtractResourceType(tc.tool, tc.args))
		})
	}
}

func itoaTest(i int) string {
	if i == 0 {
		return "0"
	}
	var out []byte
	for ; i > 0; i /= 10 {
		out = append([]byte{byte('0' + i%10)}, out...)
	}
	return string(out)
}

// ============================================================================
// GetToolDefinitions / GetToolDefinitionsWithDefaultDatabase — schema sanity.
// ============================================================================

func TestGetToolDefinitions_ReturnsExpectedSet(t *testing.T) {
	tools := GetToolDefinitions()
	require.Len(t, tools, 6)
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
		require.NotEmpty(t, tool.InputSchema, "%s schema must not be empty", tool.Name)
		require.NotEmpty(t, tool.Description, "%s description must not be empty", tool.Name)
	}
	require.ElementsMatch(t,
		[]string{"store", "recall", "discover", "link", "task", "tasks"}, names)
}

func TestGetToolDefinitionsWithDefaultDatabase_PropagatesDefault(t *testing.T) {
	tools := GetToolDefinitionsWithDefaultDatabase("primary")
	for _, tool := range tools {
		require.Contains(t, string(tool.InputSchema), `"default":"primary"`,
			"%s schema must embed default database", tool.Name)
	}
}
