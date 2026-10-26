package cypher

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type transactionConditionalReads struct {
	*storage.MemoryEngine
	keys, history bool
}

func (e *transactionConditionalReads) GetInnerEngine() storage.Engine { return e.MemoryEngine }
func (e *transactionConditionalReads) ReadCapabilityAvailable(request reflect.Type) bool {
	if storage.ReadCapabilityIncludes[storage.PropertyKeyLookup](request) || storage.ReadCapabilityIncludes[storage.NamespacePropertyKeyLookup](request) {
		if !e.keys {
			return false
		}
	}
	if (storage.ReadCapabilityIncludes[interface {
		GetNodesByLabelVisibleAt(string, storage.MVCCVersion) ([]*storage.Node, error)
	}](request) ||
		storage.ReadCapabilityIncludes[interface {
			GetOutgoingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](request) ||
		storage.ReadCapabilityIncludes[interface {
			GetIncomingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](request) ||
		storage.ReadCapabilityIncludes[interface {
			GetEdgesByTypeVisibleAt(string, storage.MVCCVersion) ([]*storage.Edge, error)
		}](request) ||
		storage.ReadCapabilityIncludes[interface {
			GetEdgesBetweenVisibleAt(storage.NodeID, storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](request)) && !e.history {
		return false
	}
	return true
}

type transactionCombinedRead interface {
	storage.PropertyKeyLookup
	storage.MVCCIndexedVisibilityEngine
}

// A combined operation cannot become available because only its first
// dependency is available. Toggle both dependencies on a real bound transaction.
func TestEngineDecoratorsTransactionCombinedAvailability(t *testing.T) {
	base := storage.NewMemoryEngineWithMVCCHistory()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	conditional := &transactionConditionalReads{MemoryEngine: base, keys: true, history: true}
	executor := NewStorageExecutor(storage.NewNamespacedEngine(conditional, "tenant"))
	_, err := executor.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = executor.Execute(context.Background(), "ROLLBACK", nil) })
	_, err = executor.Execute(context.Background(), "CREATE (:CombinedAvailability {v:1})", nil)
	require.NoError(t, err)
	require.NotNil(t, executor.txContext.storageWrapper)
	for _, state := range []struct{ keys, history bool }{{true, true}, {true, false}, {false, true}, {false, false}, {true, true}} {
		conditional.keys, conditional.history = state.keys, state.history
		for name, engine := range engineCapabilityStacks(executor.txContext.storageWrapper) {
			_, keys := capabilityForTest[storage.PropertyKeyLookup](engine)
			_, history := capabilityForTest[storage.MVCCIndexedVisibilityEngine](engine)
			_, combined := capabilityForTest[transactionCombinedRead](engine)
			require.Equal(t, state.keys, keys, "%s key dependency", name)
			require.Equal(t, state.history, history, "%s history dependency", name)
			require.Equal(t, keys && history, combined, "%s all dependencies must be available", name)
		}
	}
}
