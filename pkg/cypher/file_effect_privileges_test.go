package cypher

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExportFileEffectRequiresWriteAndStreamKeepsRead(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "export_contract"))
	executor.SetAllowLocalAPOCExportFileAccess(true)
	executor.apocLocalFileAccessRoot = t.TempDir()
	readOnly := WithPermissionChecker(context.Background(), func(permission string) bool { return permission == "read" })
	writer := WithPermissionChecker(context.Background(), func(permission string) bool { return permission == "read" || permission == "write" })
	for _, procedure := range []string{"apoc.export.json.all", "apoc.export.json.query", "apoc.export.csv.all", "apoc.export.csv.query"} {
		t.Run(procedure, func(t *testing.T) {
			path := filepath.Join(executor.apocLocalFileAccessRoot, "sentinel")
			require.NoError(t, os.WriteFile(path, []byte("unchanged"), 0600))
			args := "$file, {}"
			if procedure == "apoc.export.json.query" || procedure == "apoc.export.csv.query" {
				args = "'RETURN 1 AS value', " + args
			}
			query := "CALL " + procedure + "(" + args + ")"
			_, err := executor.Execute(readOnly, query, map[string]interface{}{"file": "sentinel"})
			require.Error(t, err)
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, "unchanged", string(content))
			_, err = executor.Execute(readOnly, query, map[string]interface{}{"file": nil})
			require.NoError(t, err, "stream output needs read only")
			_, err = executor.Execute(writer, query, map[string]interface{}{"file": "sentinel"})
			require.NoError(t, err)
			content, err = os.ReadFile(path)
			require.NoError(t, err)
			require.NotEqual(t, "unchanged", string(content))
		})
	}
}
