package cypher

import (
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// functionContextSink keeps the contexts built in
// TestFunctionContextGraphsAllocatesNothing, so the compiler can't drop them.
var functionContextSink cypherfn.Context

// TestFunctionContextGraphsAllocatesNothing: a function context is built for
// every function call on every row, and its graph catalog is the executor
// itself, which costs no allocation (a method value, e.CompositeGraphs,
// would build a closure each time).
func TestFunctionContextGraphsAllocatesNothing(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	allocs := testing.AllocsPerRun(100, func() {
		functionContextSink = cypherfn.Context{Graphs: exec}
	})
	require.Zero(t, allocs)
	graphs, composite := functionContextSink.Graphs.CompositeGraphs()
	require.False(t, composite)
	require.Nil(t, graphs)
}
