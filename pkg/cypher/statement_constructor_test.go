package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type catalogReadCountingEngine struct {
	storage.Engine
	reads int
}

func (e *catalogReadCountingEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	e.reads++
	return e.Engine.GetNodesByLabel(label)
}

func TestStatementConstructionDoesNotReadOrReloadCatalog(t *testing.T) {
	engine := &catalogReadCountingEngine{Engine: newTestMemoryEngine(t)}
	executor := NewStatementExecutor(engine)
	require.Zero(t, engine.reads, "request construction must not scan storage")
	require.NoError(t, executor.ReloadProcedureCatalog())
	require.Equal(t, 1, engine.reads, "trusted catalog loading remains explicit")
	_ = NewStorageExecutor(engine)
	require.Equal(t, 2, engine.reads, "embedded startup preserves catalog loading")
}
