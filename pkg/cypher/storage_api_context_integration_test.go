package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type callerStorageContextKey struct{}

// The boundary cancels a real request immediately before the native read. It
// proves caller propagation, not the native engine's lock-admission behavior.
type callerContextReadStore struct {
	storage.Engine
	cancel context.CancelFunc
	seen   context.Context
	reads  int
}

func (s *callerContextReadStore) beforeRead(ctx context.Context) {
	s.seen = ctx
	if cancel := s.cancel; cancel != nil {
		s.cancel = nil
		cancel()
	}
}
func (s *callerContextReadStore) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	s.beforeRead(ctx)
	s.reads++
	return s.Engine.GetNode(ctx, id)
}
func (s *callerContextReadStore) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	s.beforeRead(ctx)
	return s.Engine.GetNode(ctx, id)
}

func TestStorageAPIRowEndpointUsesStatementContext(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%t", normalization), func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(normalization)
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			store := &callerContextReadStore{Engine: newTestMemoryEngine(t)}
			native := storage.NewNamespacedEngine(store, "caller_context")
			id, err := native.CreateNode(t.Context(), &storage.Node{ID: "from", Labels: []string{"Endpoint"}, Properties: map[string]interface{}{"name": "kept"}})
			require.NoError(t, err)
			exec := NewStorageExecutor(native)
			edge := &storage.Edge{ID: "r", StartNode: id, EndNode: id, Type: "R"}
			require.NoError(t, native.CreateEdge(t.Context(), edge))
			stored, err := native.GetNode(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, "kept", stored.Properties["name"])
			const query = "WITH $r AS r RETURN [startNode(r).name] AS names"
			request, cancel := context.WithCancel(context.WithValue(t.Context(), callerStorageContextKey{}, "request"))
			defer cancel()
			store.cancel = cancel
			result, err := exec.Execute(request, query, map[string]interface{}{"r": edge})
			require.NotNil(t, store.seen, "the cancellation seam must be reached")
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, result)
			require.NotNil(t, store.seen, "the public expression must reach the native point-read boundary")
			require.Equal(t, "request", store.seen.Value(callerStorageContextKey{}))
			reads := store.reads
			result, err = exec.Execute(t.Context(), query, map[string]interface{}{"r": edge})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{[]interface{}{"kept"}}}, result.Rows)
			require.Greater(t, store.reads, reads, "a canceled read cannot publish a null cache entry")
			count, err := native.NodeCount(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
		})
	}
}

type callerContextBeginStore struct {
	storage.Engine
	beginContext context.Context
	native       *storage.MemoryEngine
}

func (s *callerContextBeginStore) BeginTransaction(ctx context.Context) (*storage.BadgerTransaction, error) {
	s.beginContext = ctx
	return s.native.BeginTransaction(ctx)
}

func TestStorageAPIBeginContextOwnsAdmissionNotRetainedHandle(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%t", normalization), func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(normalization)
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			native := newTestMemoryEngine(t)
			store := &callerContextBeginStore{Engine: native, native: native}
			namespaced := storage.NewNamespacedEngine(store, "begin_context")
			exec := NewStorageExecutor(namespaced)
			t.Cleanup(func() {
				if exec.txContext != nil && exec.txContext.active {
					_, _ = exec.handleRollback(context.Background(), nil)
				}
			})
			request, cancel := context.WithCancel(context.WithValue(t.Context(), callerStorageContextKey{}, "begin"))
			defer cancel()
			_, err := exec.Execute(request, "BEGIN", nil)
			require.NoError(t, err)
			require.NotNil(t, store.beginContext)
			require.Equal(t, "begin", store.beginContext.Value(callerStorageContextKey{}))
			cancel()
			require.ErrorIs(t, store.beginContext.Err(), context.Canceled)
			_, err = exec.Execute(t.Context(), "CREATE (:Retained {name:'after begin cancellation'})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(t.Context(), "COMMIT", nil)
			require.NoError(t, err)
			count, err := namespaced.NodeCount(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
		})
	}
}

func TestStorageAPIRowMetadataDoesNotReplaceReadCancellation(t *testing.T) {
	store := &callerContextReadStore{Engine: newTestMemoryEngine(t)}
	native := storage.NewNamespacedEngine(store, "metadata_context")
	id, err := native.CreateNode(t.Context(), &storage.Node{ID: "from", Properties: map[string]interface{}{"name": "kept"}})
	require.NoError(t, err)
	exec := NewStorageExecutor(native)
	metadata := context.WithValue(context.Background(), ctxKeyUseDatabase, "metadata_context")
	edge := &storage.Edge{ID: "r", StartNode: id, EndNode: id, Type: "R"}
	require.NoError(t, native.CreateEdge(t.Context(), edge))
	values := pipelineRow{"r": edge, rowStatementContextKey: metadata}
	request, cancel := context.WithCancel(context.WithValue(t.Context(), callerStorageContextKey{}, "operation"))
	defer cancel()
	operation := withExpressionFailureSlot(request)
	store.cancel = cancel
	_, resolved := exec.evaluateRowExpressionWithContext(operation, "[startNode(r).name]", values)
	require.NotNil(t, store.seen, "the cancellation seam must be reached")
	require.False(t, resolved)
	require.ErrorIs(t, getExpressionFailure(operation), context.Canceled)
	require.NotNil(t, store.seen)
	require.Equal(t, "operation", store.seen.Value(callerStorageContextKey{}))
	require.NoError(t, metadata.Err(), "semantic metadata does not own native read cancellation")
	require.Same(t, metadata, values[rowStatementContextKey])
}
