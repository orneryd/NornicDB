// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Public numeric round trips use independent kind/value oracles.
package storage

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	apocexport "github.com/orneryd/nornicdb/apoc/export"
	apocimport "github.com/orneryd/nornicdb/apoc/import"
	apocjson "github.com/orneryd/nornicdb/apoc/json"
	"github.com/orneryd/nornicdb/testing/numericoracle"
	"github.com/stretchr/testify/require"
)

func numericDomainProperties() map[string]interface{} {
	return map[string]interface{}{
		"u_signed_max": uint64(math.MaxInt64), "u_above_signed": uint64(math.MaxInt64) + 1, "u_max": uint64(math.MaxUint64),
		"i_min": int64(math.MinInt64), "i_max": int64(math.MaxInt64),
		"positive_before": int64(9007199254740991), "positive_at": int64(9007199254740992), "positive_after": int64(9007199254740993),
		"negative_before": int64(-9007199254740991), "negative_at": int64(-9007199254740992), "negative_after": int64(-9007199254740993),
		"whole": float64(2), "subnormal": math.SmallestNonzeroFloat64, "negative_zero": math.Copysign(0, -1), "largest": math.MaxFloat64, "decimal": 1.25,
		"nested": map[string]interface{}{"values": []interface{}{uint64(math.MaxUint64), int64(-9007199254740993), float64(3), math.SmallestNonzeroFloat64}},
		"text":   "18446744073709551615 is text",
	}
}

func numericDomainTransforms(t *testing.T) map[string]func(string) string {
	first := func(text string) string {
		var items []json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(text), &items))
		require.Len(t, items, 1)
		return string(items[0])
	}
	return map[string]func(string) string{
		"pretty": apocjson.Pretty, "compact": apocjson.Compact,
		"parse-stringify": func(text string) string { return apocjson.Stringify(apocjson.Parse(text)) },
		"path":            func(text string) string { return apocjson.Stringify(apocjson.Path(text, "$")) },
		"values":          func(text string) string { return apocjson.Stringify(apocjson.Values(`{"document":` + text + `}`)[0]) },
		"merge":           func(text string) string { return apocjson.Merge(text, `{"format_note":"retained"}`) },
		"set":             func(text string) string { return apocjson.Set(text, "$.format_note", "retained") },
		"delete": func(text string) string {
			return apocjson.Delete(apocjson.Set(text, "$.format_note", true), "$.format_note")
		},
		"flatten": apocjson.Flatten, "unflatten": apocjson.Unflatten,
		"flatten-unflatten": func(text string) string { return apocjson.Unflatten(apocjson.Flatten(text)) },
		"filter": func(text string) string {
			return first(apocjson.Filter("["+text+"]", func(interface{}) bool { return true }))
		},
		"map": func(text string) string {
			return first(apocjson.Map("["+text+"]", func(value interface{}) interface{} { return value }))
		},
		"reduce": func(text string) string {
			return apocjson.Stringify(apocjson.Reduce("["+text+"]", nil, func(_, value interface{}) interface{} { return value }))
		},
		"inspection": func(text string) string {
			require.True(t, apocjson.Validate(text))
			require.Equal(t, "object", apocjson.Type(text))
			require.Greater(t, apocjson.Size(text), 0)
			require.NotEmpty(t, apocjson.Keys(text))
			require.Equal(t, "number", apocjson.Type("18446744073709551615"))
			return text
		},
	}
}

func TestNumericDomainExportTransformRestore(t *testing.T) {
	ctx := t.Context()
	source := NewNamespacedEngine(newTestMemoryEngine(t), "test")
	t.Cleanup(func() { _ = source.Close() })
	properties := numericDomainProperties()
	_, err := source.CreateNode(ctx, &Node{ID: "domain", Labels: []string{"NumericDomain"}, Properties: properties})
	require.NoError(t, err)
	require.NoError(t, source.CreateEdge(ctx, &Edge{ID: "domain-edge", StartNode: "domain", EndNode: "domain", Type: "DOMAIN", Properties: properties}))
	path := filepath.Join(t.TempDir(), "export.json")
	require.NoError(t, SaveToNeo4jExport(source, path))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	for name, transform := range numericDomainTransforms(t) {
		t.Run(name, func(t *testing.T) {
			target := NewNamespacedEngine(newTestMemoryEngine(t), "test")
			t.Cleanup(func() { _ = target.Close() })
			path := filepath.Join(t.TempDir(), "formatted.json")
			require.NoError(t, os.WriteFile(path, []byte(transform(string(raw))), 0600))
			require.NoError(t, LoadFromNeo4jExport(target, path))
			node, err := target.GetNode(ctx, "domain")
			require.NoError(t, err)
			numericoracle.Equal(t, properties, node.Properties)
			edge, err := target.GetEdge(ctx, "domain-edge")
			require.NoError(t, err)
			numericoracle.Equal(t, properties, edge.Properties)
		})
	}
}

func TestNumericDomainExportJSONL(t *testing.T) {
	properties := numericDomainProperties()
	text, err := apocexport.Json([]*apocexport.Node{{ID: 1, Properties: properties}}, nil)
	require.NoError(t, err)
	document, err := apocimport.ParseJsonLine(text)
	require.NoError(t, err)
	nodes := document["nodes"].([]interface{})
	require.Len(t, nodes, 1)
	numericoracle.Equal(t, properties, nodes[0].(map[string]interface{})["Properties"])
}
