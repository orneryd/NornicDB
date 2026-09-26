package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type labelErrorEngine struct {
	*MemoryEngine
	err error
}

func (e *labelErrorEngine) GetNodesByLabel(label string) ([]*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.MemoryEngine.GetNodesByLabel(label)
}

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

func TestAsyncEngine_ValidateBulkNodeConstraints_Branches(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	ae := newAsyncForBranches(t, inner)

	schema := inner.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "uniq_email",
		Type:       ConstraintUnique,
		Label:      "User",
		Properties: []string{"email"},
	}))
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "key_user",
		Type:       ConstraintNodeKey,
		Label:      "User",
		Properties: []string{"tenant", "uid"},
	}))

	// Duplicate unique value in same batch.
	err := ae.validateBulkNodeConstraints([]*Node{
		{ID: "test:n1", Labels: []string{"User"}, Properties: map[string]any{"email": "u@x", "tenant": "t", "uid": "1"}},
		{ID: "test:n2", Labels: []string{"User"}, Properties: map[string]any{"email": "u@x", "tenant": "t", "uid": "2"}},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintUnique, cve.Type)

	// Node key null component branch.
	err = ae.validateBulkNodeConstraints([]*Node{
		{ID: "test:n3", Labels: []string{"User"}, Properties: map[string]any{"email": "n3@x", "tenant": "t"}},
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintNodeKey, cve.Type)

	// Duplicate node key in same batch.
	err = ae.validateBulkNodeConstraints([]*Node{
		{ID: "test:n4", Labels: []string{"User"}, Properties: map[string]any{"email": "n4@x", "tenant": "t", "uid": "9"}},
		{ID: "test:n5", Labels: []string{"User"}, Properties: map[string]any{"email": "n5@x", "tenant": "t", "uid": "9"}},
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintNodeKey, cve.Type)

	// resolveNamespace error path (missing prefix on non-namespaced engine).
	err = ae.validateBulkNodeConstraints([]*Node{{ID: "no-prefix", Labels: []string{"User"}, Properties: map[string]any{"email": "x"}}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be prefixed")
}

func TestAsyncEngine_CheckUniqueAndNodeKeyConstraint_Branches(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	ae := newAsyncForBranches(t, inner)

	uniq := Constraint{Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}
	key := Constraint{Type: ConstraintNodeKey, Label: "User", Properties: []string{"tenant", "uid"}}

	// nil properties branch in unique-check.
	require.NoError(t, ae.checkUniqueConstraint(&Node{ID: "test:a", Labels: []string{"User"}}, uniq, "test", true))

	ae.mu.Lock()
	ae.setCachedNodeLocked(&Node{ID: "test:cached", Labels: []string{"User"}, Properties: map[string]any{"email": "dup@x", "tenant": "t", "uid": "1"}})
	ae.mu.Unlock()

	// cache duplicate branch in unique-check.
	err := ae.checkUniqueConstraint(&Node{ID: "test:new", Labels: []string{"User"}, Properties: map[string]any{"email": "dup@x"}}, uniq, "test", true)
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintUnique, cve.Type)

	// node key nil-properties branch.
	err = ae.checkNodeKeyConstraint(&Node{ID: "test:nk-nil", Labels: []string{"User"}}, key, "test", true)
	require.Error(t, err)
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintNodeKey, cve.Type)

	// node key missing property branch.
	err = ae.checkNodeKeyConstraint(&Node{ID: "test:nk-miss", Labels: []string{"User"}, Properties: map[string]any{"tenant": "t"}}, key, "test", true)
	require.Error(t, err)
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintNodeKey, cve.Type)

	// cache duplicate branch in node-key check.
	err = ae.checkNodeKeyConstraint(&Node{ID: "test:nk-new", Labels: []string{"User"}, Properties: map[string]any{"tenant": "t", "uid": "1"}}, key, "test", true)
	require.Error(t, err)
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintNodeKey, cve.Type)
}

func TestAsyncEngine_CheckConstraint_BackendLookupErrorIgnored(t *testing.T) {
	inner := &labelErrorEngine{MemoryEngine: NewMemoryEngine(), err: errors.New("label scan fail")}
	t.Cleanup(func() { _ = inner.Close() })
	ae := newAsyncForBranches(t, inner)

	uniq := Constraint{Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}
	key := Constraint{Type: ConstraintNodeKey, Label: "User", Properties: []string{"tenant", "uid"}}

	// Backend label read errors are intentionally ignored in async pre-check.
	err := ae.checkUniqueConstraint(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x"}}, uniq, "test", true)
	require.NoError(t, err)

	err = ae.checkNodeKeyConstraint(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"tenant": "t", "uid": "1"}}, key, "test", true)
	require.NoError(t, err)
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
