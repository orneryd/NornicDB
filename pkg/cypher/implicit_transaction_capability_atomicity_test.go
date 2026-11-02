package cypher

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Expose exactly the required Engine interface, hiding the real memory store's
// optional transaction and unwrapping capabilities. The second native create
// fails after the first has actually reached storage if execution is admitted.
type capabilityHiddenFailingCreateEngine struct {
	storage.Engine
	calls   int
	failure error
}

func (e *capabilityHiddenFailingCreateEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	e.calls++
	if e.calls == 2 {
		return "", e.failure
	}
	return e.Engine.CreateNode(ctx, node)
}

func TestImplicitWriteRequiresTransactionCapabilityBeforeExecution(t *testing.T) {
	const procedure = "atomiccapability.write"
	var procedureCalls atomic.Int64
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: procedure, Signature: procedure + "() :: (value :: INTEGER)", Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}}}, func(ctx context.Context, exec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		procedureCalls.Add(1)
		return exec.Execute(ctx, "CREATE (a:AtomicFallback {step: 1}) WITH a CREATE (b:AtomicFallback {step: 2}) RETURN a, b", nil)
	}))
	t.Cleanup(func() {
		globalProcedureRegistry.mu.Lock()
		delete(globalProcedureRegistry.user, procedure)
		delete(globalProcedureRegistry.userFold, procedure)
		globalProcedureRegistry.generation.Add(1)
		globalProcedureRegistry.mu.Unlock()
	})
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNICDB_PARSER", parser)
			for _, query := range []string{
				"CREATE (a:AtomicFallback {step: 1}) WITH a CREATE (b:AtomicFallback {step: 2}) RETURN a, b",
				"MERGE (n:AtomicFallback {step: 1}) RETURN n",
				"MATCH (n:Existing) SET n.value = 'changed' RETURN n",
				"MATCH (n:Existing) DELETE n",
				"CALL atomiccapability.write() YIELD value RETURN value",
				"CREATE INDEX atomic_capability_idx FOR (n:Existing) ON (n.value)",
				"CREATE (n:AtomicFallback {content: 'x'}) WITH EMBEDDING",
			} {
				t.Run(query, func(t *testing.T) {
					root := newTestMemoryEngine(t)
					store := storage.NewNamespacedEngine(root, "atomic_capability")
					id, err := store.CreateNode(t.Context(), &storage.Node{ID: "existing", Labels: []string{"Existing"}, Properties: map[string]interface{}{"value": "before"}})
					require.NoError(t, err)
					beforeIndexes := store.GetSchema().GetIndexes()
					before, err := store.GetNode(t.Context(), id)
					require.NoError(t, err)
					failure := errors.New("controlled second native CreateNode failure")
					backend := &capabilityHiddenFailingCreateEngine{Engine: store, failure: failure}
					exec := NewStorageExecutor(backend)
					exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "atomic-test", dims: 3})
					var mutations atomic.Int64
					exec.SetNodeMutatedCallback(func(string) { mutations.Add(1) })
					callsBefore := procedureCalls.Load()
					result, err := exec.Execute(t.Context(), query, nil)
					expected := localization.CypherTransactionsEngineUnsupported()
					if strings.Contains(query, "WITH EMBEDDING") {
						expected = localization.CypherCoreEmbeddingTransactionStorageRequired()
					}
					assert.ErrorContains(t, err, expected.Fallback)
					assert.Nil(t, result)
					assert.Zero(t, backend.calls, "reject before the first native create, not after backend failure")
					assert.Zero(t, mutations.Load(), "no mutation callbacks before transaction admission")
					assert.Equal(t, callsBefore, procedureCalls.Load(), "no procedure callback before transaction admission")
					count, countErr := root.NodeCount(t.Context())
					assert.NoError(t, countErr)
					assert.Equal(t, int64(1), count, "no partial creates or deletes in actual storage")
					after, readErr := store.GetNode(t.Context(), id)
					assert.NoError(t, readErr)
					if assert.NotNil(t, after) {
						assert.Equal(t, before.ID, after.ID)
						assert.Equal(t, before.Labels, after.Labels)
						assert.Equal(t, before.Properties, after.Properties, "rejection must preserve the existing graph")
					}
					nodes, readErr := store.GetNodesByLabel(t.Context(), "AtomicFallback")
					assert.NoError(t, readErr)
					assert.Empty(t, nodes)
					assert.ElementsMatch(t, beforeIndexes, store.GetSchema().GetIndexes(), "rejection must not publish schema metadata")
					read, readErr := exec.Execute(t.Context(), "MATCH (n:Existing) RETURN n.value", nil)
					assert.NoError(t, readErr)
					if assert.NotNil(t, read) {
						assert.Equal(t, [][]interface{}{{"before"}}, read.Rows)
					}
				})
			}
		})
	}
}

func TestNativeImplicitWritesRemainAtomicAndNamespaced(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNICDB_PARSER", parser)
			root := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(root, "atomic_native")
			other := storage.NewNamespacedEngine(root, "atomic_other")
			otherID, err := other.CreateNode(t.Context(), &storage.Node{ID: "kept", Labels: []string{"AtomicUnique"}, Properties: map[string]interface{}{"key": "duplicate"}})
			require.NoError(t, err)
			before, err := other.GetNode(t.Context(), otherID)
			require.NoError(t, err)
			exec := NewStorageExecutor(store)
			_, err = exec.Execute(t.Context(), "CREATE CONSTRAINT atomic_unique FOR (n:AtomicUnique) REQUIRE n.key IS UNIQUE", nil)
			require.NoError(t, err)
			result, err := exec.Execute(t.Context(), "CREATE (a:AtomicUnique {key:'duplicate'}), (b:AtomicUnique {key:'duplicate'}) RETURN a,b", nil)
			require.Error(t, err)
			var violation *storage.ConstraintViolationError
			require.ErrorAs(t, err, &violation, "real native commit constraint fault, not missing-capability admission")
			require.Nil(t, result)
			nodes, err := store.GetNodesByLabel(t.Context(), "AtomicUnique")
			require.NoError(t, err)
			require.Empty(t, nodes, "native failed statement must roll back both staged creates")
			result, err = exec.Execute(t.Context(), "CREATE (a:AtomicUnique {key:'duplicate'}), (b:AtomicUnique {key:'distinct'}) RETURN count(*)", nil)
			require.NoError(t, err)
			require.NotNil(t, result)
			nodes, err = store.GetNodesByLabel(t.Context(), "AtomicUnique")
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			after, err := other.GetNode(t.Context(), otherID)
			require.NoError(t, err)
			require.Equal(t, before.ID, after.ID)
			require.Equal(t, before.Labels, after.Labels)
			require.Equal(t, before.Properties, after.Properties)
			count, err := root.NodeCount(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(3), count)
		})
	}
}
