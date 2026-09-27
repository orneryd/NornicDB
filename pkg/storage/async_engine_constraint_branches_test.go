package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type finderExportEngine struct {
	*MemoryEngine
	find *Node
	all  []*Node
	err  error
}

func (e *finderExportEngine) FindNodeNeedingEmbedding() *Node {
	return e.find
}

func (e *finderExportEngine) AllNodes() ([]*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	if e.all != nil {
		return e.all, nil
	}
	return e.MemoryEngine.AllNodes()
}

func newAsyncForBranches(t *testing.T, inner Engine) *AsyncEngine {
	t.Helper()
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })
	return ae
}

func TestAsyncEngine_BulkCreateValidationBranches(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	ae := newAsyncForBranches(t, inner)

	require.ErrorIs(t, ae.BulkCreateNodes([]*Node{nil}), ErrInvalidData)
	require.Error(t, ae.BulkCreateNodes([]*Node{{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"bad": func() {}}}}))

	require.ErrorIs(t, ae.BulkCreateEdges([]*Edge{nil}), ErrInvalidData)
	require.Error(t, ae.BulkCreateEdges([]*Edge{{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"bad": func() {}}}}))
}

func TestAsyncEngine_FindNodeNeedingEmbedding_Branches(t *testing.T) {
	base := &finderExportEngine{MemoryEngine: NewMemoryEngine()}
	t.Cleanup(func() { _ = base.Close() })
	ae := newAsyncForBranches(t, base)

	// Cache node marked for deletion should be skipped.
	deleted := &Node{ID: "test:del", Labels: []string{"Doc"}, Properties: map[string]any{"content": "x"}}
	ae.mu.Lock()
	ae.nodeCache[deleted.ID] = deleted
	ae.deleteNodes[deleted.ID] = true
	ae.mu.Unlock()
	require.Nil(t, ae.FindNodeNeedingEmbedding())

	// Underlying finder returns node, but cache already has embedding for it -> skip.
	base.find = &Node{ID: "test:emb", Labels: []string{"Doc"}, Properties: map[string]any{"content": "x"}}
	ae.mu.Lock()
	ae.nodeCache["test:emb"] = &Node{ID: "test:emb", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0.1}}}
	ae.mu.Unlock()
	require.Nil(t, ae.FindNodeNeedingEmbedding())

	// Finder path with delete marker skip then successful return.
	base.find = &Node{ID: "test:gone", Labels: []string{"Doc"}, Properties: map[string]any{"content": "x"}}
	ae.mu.Lock()
	ae.deleteNodes["test:gone"] = true
	ae.mu.Unlock()
	require.Nil(t, ae.FindNodeNeedingEmbedding())

	base.find = &Node{ID: "test:need", Labels: []string{"Doc"}, Properties: map[string]any{"content": "x"}}
	need := ae.FindNodeNeedingEmbedding()
	require.NotNil(t, need)
	require.Equal(t, NodeID("test:need"), need.ID)
}
