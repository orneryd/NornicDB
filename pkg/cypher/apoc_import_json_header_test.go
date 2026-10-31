// Author: declared-list-owners lane (Claude), 2026-10-30.

package cypher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestApocImportJSONStandaloneHeaderMatchesDeclaration calls apoc.import.json
// without YIELD on both parsers: the executed header must be the declared one,
// so the statement-schema check accepts the result.
func TestApocImportJSONStandaloneHeaderMatchesDeclaration(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(previous)
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			exec.SetAllowLocalAPOCFileAccess(true)
			graph, err := json.Marshal(map[string]interface{}{
				"nodes":         []interface{}{map[string]interface{}{"id": "i1", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"name": "Carol"}}},
				"relationships": []interface{}{},
			})
			require.NoError(t, err)
			file := filepath.Join(t.TempDir(), "graph.json")
			require.NoError(t, os.WriteFile(file, graph, 0o644))

			query := "CALL apoc.import.json($file)"
			declared, err := exec.StatementResultColumns(query, RawSource)
			require.NoError(t, err)
			result, err := exec.Execute(context.Background(), query, map[string]interface{}{"file": file})
			require.NoError(t, err)
			require.Equal(t, declared, result.Columns)
			require.Equal(t, []string{"source", "nodes", "relationships"}, result.Columns)
			require.Equal(t, [][]interface{}{{file, 1, 0}}, result.Rows)
		})
	}
}
