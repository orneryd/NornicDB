package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStoredLiteralUnicodeNameSourceRoundtrip(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			engine := newTestMemoryEngine(t)
			for _, node := range []*storage.Node{{ID: "nornic:historical", Labels: []string{"legacy\\u0062"}, Properties: map[string]any{"key\\u0062": int64(1)}}, {ID: "nornic:decoded", Labels: []string{"legacyb"}, Properties: map[string]any{"keyb": int64(2)}}} {
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
			}
			executor := NewStorageExecutor(engine)
			query := "MATCH (n:" + cyphertext.PrintIdentifier("legacy\\u0062", nil, cyphertext.RawSource) + ") RETURN n." + cyphertext.PrintIdentifier("key\\u0062", nil, cyphertext.RawSource) + " AS value"
			result, err := executor.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{int64(1)}}, result.Rows, "printing a stored literal name must not target its Unicode-decoded sibling")
			original, err := engine.GetNode("nornic:historical")
			require.NoError(t, err)
			require.Equal(t, []string{"legacy\\u0062"}, original.Labels)
			require.Equal(t, int64(1), original.Properties["key\\u0062"])
		})
	}
}
