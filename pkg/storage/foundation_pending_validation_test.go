// Author: Lane #1644 (Codex). A failed candidate validation cannot retire
// durable embedding work. Polling may return no candidate and retry later.
package storage_test

import (
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type foundationPendingValidation struct {
	storage.Engine
	candidate *storage.Node
	pending   bool
	failure   error
	visits    int
	retired   int
}

func (v *foundationPendingValidation) FindNodeNeedingEmbedding() *storage.Node {
	if v.pending {
		return storage.CopyNode(v.candidate)
	}
	return nil
}
func (v *foundationPendingValidation) MarkNodeEmbedded(storage.NodeID) {
	v.pending = false
	v.retired++
}
func (v *foundationPendingValidation) GetNode(id storage.NodeID) (*storage.Node, error) {
	v.visits++
	if v.failure != nil {
		return nil, v.failure
	}
	return v.Engine.GetNode(id)
}
func TestFoundationPublicPendingValidation(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	_, err := base.CreateNode(&storage.Node{ID: "tenant:a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "needs embedding"}})
	require.NoError(t, err)
	node, err := base.GetNode("tenant:a")
	require.NoError(t, err)
	view := &foundationPendingValidation{Engine: base, candidate: node, pending: true}
	tenant := storage.NewNamespacedEngine(view, "tenant")
	healthy := tenant.FindNodeNeedingEmbedding()
	require.NotNil(t, healthy)
	require.Equal(t, storage.NodeID("a"), healthy.ID)
	view.failure = errors.New("candidate validation unavailable")
	require.Nil(t, tenant.FindNodeNeedingEmbedding())
	require.Positive(t, view.visits)
	require.True(t, view.pending, "a failed validation must not claim the work was completed")
	require.Zero(t, view.retired)
	view.failure = nil
	require.NotNil(t, tenant.FindNodeNeedingEmbedding(), "work remains available after the read recovers")
	view.failure = storage.ErrNotFound
	require.Nil(t, tenant.FindNodeNeedingEmbedding())
	require.False(t, view.pending, "real absence still cleans up the stale candidate")
	require.Equal(t, 1, view.retired)
}
