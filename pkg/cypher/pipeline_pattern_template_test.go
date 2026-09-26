package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestPipelinePatternTemplateMatchesTextRoute: a pattern property map
// evaluated for a row directly (the template) holds the same values as the
// general route, which renders the row's values into the pattern text and
// parses it again.
func TestPipelinePatternTemplateMatchesTextRoute(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "template"))
	ctx := withParams(context.Background(), map[string]interface{}{"p": "param", "n": int64(7), "list": []interface{}{int64(1), "two"}})
	row := pipelineRow{
		"a":     &storage.Node{ID: "a1", Properties: map[string]interface{}{"id": int64(1), "name": "it's \"quoted\""}},
		"s":     "plain",
		"q":     "it's \\ \"both\"",
		"dot":   "a.id",
		"u":     "ünïcødé ✓",
		"i":     int64(-42),
		"big":   int64(9007199254740993),
		"f":     1.5,
		"whole": 1.0,
		"tiny":  0.1 + 0.2,
		"neg0":  -0.0,
		"b":     true,
		"l":     []interface{}{int64(1), 2.5, "x", nil, []interface{}{true}},
		"m":     map[string]interface{}{"k": "v", "n": int64(1), "inner": map[string]interface{}{"z": false}},
		"empty": []interface{}{},
		"none":  nil,
	}
	expressions := []string{
		"'literal'", "'a.id'", "\"double\"", "42", "-1.25", "true", "null", "[1, 'x']", "{k: 1}",
		"s", "q", "dot", "u", "i", "big", "f", "whole", "tiny", "neg0", "b", "l", "m", "empty", "none",
		"a.id", "a.name", "$p", "$n", "$list", "s + '!'", "i * 2", "size(l)", "toUpper(s)", "coalesce(none, 'd')",
		"date('2024-02-29')", "duration('P1D')",
	}
	for _, expression := range expressions {
		pattern := fmt.Sprintf("(x:L {k: %s})", expression)
		textRoute := exec.parseNodePattern(ctx, exec.materializePipelinePropertyExpressions(ctx, pattern, row)).properties
		pairs, ok := exec.pipelinePropertyExpressions(fmt.Sprintf("{k: %s}", expression))
		require.True(t, ok, expression)
		direct, evaluated := exec.evaluatePipelineProperties(ctx, pairs, row)
		if !evaluated {
			// The row takes the text route.
			continue
		}
		require.True(t, reflect.DeepEqual(textRoute, direct), "%s: text route %#v, template %#v", expression, textRoute, direct)
	}
}
