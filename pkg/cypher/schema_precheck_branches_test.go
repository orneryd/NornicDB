package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type asyncEngineWrapper struct {
	storage.Engine
	pending   bool
	flushErr  error
	flushRuns int
}

func (w *asyncEngineWrapper) HasPendingWrites() bool { return w.pending }

func (w *asyncEngineWrapper) Flush() error {
	w.flushRuns++
	return w.flushErr
}

func (w *asyncEngineWrapper) GetInnerEngine() storage.Engine { return w.Engine }

type innerEngineWrapper struct{ storage.Engine }

func (w *innerEngineWrapper) GetInnerEngine() storage.Engine { return w.Engine }

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

	t.Run("pauseAsyncWritesForSchemaDDL flushes nested wrappers", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_flush")
		async := &asyncEngineWrapper{Engine: base, pending: true}
		inner := &innerEngineWrapper{Engine: async}

		resume, err := pauseAsyncWritesForSchemaDDL(inner)
		require.NoError(t, err)
		resume()
		require.Equal(t, 1, async.flushRuns)
	})

	t.Run("pauseAsyncWritesForSchemaDDL returns flush error", func(t *testing.T) {
		flushErr := errors.New("flush boom")
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_flush_err")
		async := &asyncEngineWrapper{Engine: base, pending: true, flushErr: flushErr}

		resume, err := pauseAsyncWritesForSchemaDDL(async)
		require.Error(t, err)
		resume()
		require.ErrorContains(t, err, "flush pending async writes before schema DDL")
		require.ErrorContains(t, err, "flush boom")
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

	t.Run("flush error is surfaced", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_exec_flush")
		async := &asyncEngineWrapper{Engine: base, pending: true, flushErr: errors.New("flush failed")}
		exec := NewStorageExecutor(async)
		_, err := exec.executeSchemaCommand(ctx, "CREATE INDEX idx FOR (n:Person) ON (n.name)")
		require.Error(t, err)
		require.ErrorContains(t, err, "flush pending async writes before schema DDL")
		require.ErrorContains(t, err, "flush failed")
	})

	t.Run("unknown schema command path", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_exec_unknown"))
		_, err := exec.executeSchemaCommand(ctx, "SHOW INDEXES")
		require.EqualError(t, err, "unknown schema command: SHOW INDEXES")
	})
}
