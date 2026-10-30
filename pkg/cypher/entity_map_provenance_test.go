package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEntityMapProvenanceErrors(t *testing.T) {
	tests := []struct{ name, query, code string }{
		{"id_plain_map", "UNWIND [{a: 1}] AS m RETURN id(m)", "Neo.ClientError.Statement.TypeError"},
		{"id_map_comprehension", "RETURN [x IN [{a: 1}] | id(x)]", "Neo.ClientError.Statement.TypeError"},
		{"id_forged_map", "UNWIND [{id: 'forged'}] AS m RETURN id(m)", "Neo.ClientError.Statement.TypeError"},
		{"elementid_forged_map", "UNWIND [{elementId: 'remote'}] AS m RETURN elementId(m)", "Neo.ClientError.Statement.TypeError"},
		{"startnode_plain_map", "UNWIND [{a: 1}] AS m RETURN startNode(m)", "Neo.ClientError.Statement.TypeError"},
	}
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			restoreParserTypeAfter(t)
			config.SetParserType(parser)
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
					result, err := NewStorageExecutor(engine).Execute(context.Background(), tt.query, nil)
					require.Error(t, err, "unexpected rows: %#v", result)
					var classified interface{ BoltErrorCode() string }
					require.True(t, errors.As(err, &classified), "%v", err)
					require.Equal(t, tt.code, classified.BoltErrorCode(), "%v", err)
				})
			}
		})
	}
}
