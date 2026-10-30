package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSetPropagatesUpdateNodeErrorForMergeAssignment(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, err := store.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}})
	require.NoError(t, err)

	wantErr := errors.New("update node failed")
	errStore := &updateErrorEngine{Engine: store, nodeErr: wantErr}
	exec := NewStorageExecutor(errStore)

	_, err = exec.Execute(context.Background(), "MATCH (n:Person {name:'Alice'}) SET n += {status:'active'} RETURN n", nil)
	require.ErrorIs(t, err, wantErr)
	require.Contains(t, err.Error(), "SET n +=")
}

func TestSetPropagatesUpdateNodeErrorForWholeEntityReplacement(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, err := store.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}})
	require.NoError(t, err)

	wantErr := errors.New("replace node failed")
	errStore := &updateErrorEngine{Engine: store, nodeErr: wantErr}
	exec := NewStorageExecutor(errStore)

	_, err = exec.Execute(context.Background(), "MATCH (n:Person {name:'Alice'}) SET n = {status:'active'} RETURN n", nil)
	require.ErrorIs(t, err, wantErr)
	require.Contains(t, err.Error(), "SET n =")
}

func TestSetPropagatesUpdateEdgeErrorForWholeEntityReplacement(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, err := store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r1", StartNode: "a", EndNode: "b", Type: "KNOWS", Properties: map[string]any{"since": int64(2020)}}))

	wantErr := errors.New("replace edge failed")
	errStore := &updateErrorEngine{Engine: store, edgeErr: wantErr}
	exec := NewStorageExecutor(errStore)

	_, err = exec.Execute(context.Background(), "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) SET r = {weight:2} RETURN r", nil)
	require.ErrorIs(t, err, wantErr)
	require.Contains(t, err.Error(), "SET r =")
}
