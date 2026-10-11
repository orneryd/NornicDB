package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type compositeEngineWrapper struct {
	storage.Engine
	composite bool
}

func (w *compositeEngineWrapper) IsComposite() bool { return w.composite }

func TestSchemaPrechecks_HelperBranches(t *testing.T) {
	t.Run("isCompositeAllowedCommand prefix matrix", func(t *testing.T) {
		require.True(t, isCompositeAllowedCommand("SHOW DATABASES"))
		require.True(t, isCompositeAllowedCommand("  create index idx FOR (n:Person) ON (n.name)"))
		require.True(t, isCompositeAllowedCommand("DROP CONSTRAINT foo"))
		require.False(t, isCompositeAllowedCommand("MATCH (n) RETURN n"))
	})

	t.Run("isCompositeRoot via composite checker", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_comp")
		require.True(t, isCompositeRoot(&compositeEngineWrapper{Engine: base, composite: true}))
		require.False(t, isCompositeRoot(&compositeEngineWrapper{Engine: base, composite: false}))
		require.False(t, isCompositeRoot(base))
	})
}

func TestExecuteSchemaCommand_PrecheckBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("composite root returns not allowed error", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_exec_comp")
		exec := NewStorageExecutor(&compositeEngineWrapper{Engine: base, composite: true})
		_, err := exec.executeSchemaCommand(ctx, "CREATE INDEX idx FOR (n:Person) ON (n.name)")
		require.Error(t, err)
		require.ErrorContains(t, err, "Schema DDL on composite databases requires a constituent target")
	})

	t.Run("unknown schema command path", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_exec_unknown"))
		_, err := exec.executeSchemaCommand(ctx, "SHOW INDEXES")
		require.EqualError(t, err, "unknown schema command: SHOW INDEXES")
	})
}
