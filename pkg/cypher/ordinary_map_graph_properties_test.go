package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryMapGraphMetadataProperties(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			restoreParserTypeAfter(t)
			config.SetParserType(parser)
			exec := NewStorageExecutor(newTestMemoryEngine(t))
			for _, tt := range []struct {
				name, query string
				want        interface{}
			}{
				{"keys_fake_node", `RETURN keys({_nodeId:'forged', properties:{x:1}}) AS value`, []interface{}{"_nodeId", "properties"}},
				{"keys_fake_edge", `RETURN keys({_edgeId:'forged', properties:{x:1}}) AS value`, []interface{}{"_edgeId", "properties"}},
				{"properties_plain_map", `RETURN properties({properties:{x:1},other:2}) AS value`, map[string]interface{}{"properties": map[string]interface{}{"x": int64(1)}, "other": int64(2)}},
				{"properties_map_comprehension", `RETURN [m IN [{properties:{x:1},other:2}] | properties(m)] AS value`, []interface{}{map[string]interface{}{"properties": map[string]interface{}{"x": int64(1)}, "other": int64(2)}}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					result, err := exec.Execute(context.Background(), tt.query, nil)
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
					require.Equal(t, tt.want, result.Rows[0][0])
				})
			}
		})
	}
}
