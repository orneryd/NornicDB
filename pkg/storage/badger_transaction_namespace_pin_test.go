package storage

import (
	"errors"
	"strings"
	"testing"
)

// These tests assert the per-transaction namespace pin enforced at the
// storage layer. Every transactional write must carry a "<db>:<id>" prefix,
// and every subsequent write in the same transaction must share the
// namespace pinned by the first write. Per-database MVCC counters and
// per-namespace lifecycle bookkeeping depend on this invariant.

func newPrefixedNode(id, label string) *Node {
	return &Node{
		ID:         NodeID(id),
		Labels:     []string{label},
		Properties: map[string]interface{}{},
	}
}

func newPrefixedEdge(id, edgeType string, start, end string) *Edge {
	return &Edge{
		ID:         EdgeID(id),
		Type:       edgeType,
		StartNode:  NodeID(start),
		EndNode:    NodeID(end),
		Properties: map[string]interface{}{},
	}
}

func TestBadgerTransaction_FirstNodeWritePinsNamespace(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if got := tx.Namespace(); got != "" {
		t.Fatalf("namespace must be empty before first write, got %q", got)
	}

	if _, err := tx.CreateNode(newPrefixedNode("tenant_a:n1", "L")); err != nil {
		t.Fatalf("CreateNode(tenant_a:n1): %v", err)
	}
	if got := tx.Namespace(); got != "tenant_a" {
		t.Fatalf("namespace must be pinned to %q after first write, got %q", "tenant_a", got)
	}
}

func TestBadgerTransaction_SecondWriteSameNamespaceSucceeds(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.CreateNode(newPrefixedNode("nornic:a", "L")); err != nil {
		t.Fatalf("first CreateNode: %v", err)
	}
	if _, err := tx.CreateNode(newPrefixedNode("nornic:b", "L")); err != nil {
		t.Fatalf("second CreateNode in same namespace must succeed, got: %v", err)
	}
	if got := tx.Namespace(); got != "nornic" {
		t.Fatalf("namespace must remain %q, got %q", "nornic", got)
	}
}

func TestBadgerTransaction_CreateNodeCrossNamespaceRejected(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.CreateNode(newPrefixedNode("tenant_a:n1", "L")); err != nil {
		t.Fatalf("first CreateNode: %v", err)
	}
	_, err = tx.CreateNode(newPrefixedNode("tenant_b:n2", "L"))
	if err == nil {
		t.Fatalf("CreateNode in second namespace must fail")
	}
	if !errors.Is(err, ErrCrossNamespaceTransaction) {
		t.Fatalf("error must wrap ErrCrossNamespaceTransaction, got: %v", err)
	}
	if !strings.Contains(err.Error(), "tenant_a") || !strings.Contains(err.Error(), "tenant_b") {
		t.Fatalf("error must name both namespaces, got: %v", err)
	}
	if got := tx.Namespace(); got != "tenant_a" {
		t.Fatalf("rejected write must NOT change pinned namespace, still expected %q, got %q", "tenant_a", got)
	}
}

func TestBadgerTransaction_NodeMutationCrossNamespaceRejected(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*BadgerTransaction) error
	}{
		{"UpdateNode", func(tx *BadgerTransaction) error {
			return tx.UpdateGraphNode(newPrefixedNode("tenant_b:n1", "L"), ClientNodeEdits)
		}},
		{"DeleteNode", func(tx *BadgerTransaction) error { return tx.DeleteGraphNode(NodeID("tenant_b:n1"), CascadeEdges) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := newTestEngine(t)
			tx, err := engine.BeginTransaction()
			if err != nil {
				t.Fatalf("BeginTransaction: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.CreateNode(newPrefixedNode("tenant_a:n1", "L")); err != nil {
				t.Fatalf("CreateNode: %v", err)
			}
			err = tc.mutate(tx)
			if err == nil {
				t.Fatalf("%s in second namespace must fail", tc.name)
			}
			if !errors.Is(err, ErrCrossNamespaceTransaction) {
				t.Fatalf("error must wrap ErrCrossNamespaceTransaction, got: %v", err)
			}
		})
	}
}

func TestBadgerTransaction_UnprefixedNodeRejected(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.CreateNode(newPrefixedNode("unprefixed-id", "L"))
	if err == nil {
		t.Fatalf("CreateNode with unprefixed ID must fail")
	}
	if !strings.Contains(err.Error(), "must be prefixed") {
		t.Fatalf("error must mention 'must be prefixed', got: %v", err)
	}
	if got := tx.Namespace(); got != "" {
		t.Fatalf("rejected unprefixed write must NOT pin namespace, got %q", got)
	}
}

func TestBadgerTransaction_CreateEdgeCrossNamespaceEndpointsRejected(t *testing.T) {
	engine := newTestEngine(t)
	// Pre-create endpoints in two namespaces using direct engine API
	// (engine-level creates do not enforce the per-tx pin).
	if _, err := engine.CreateNode(newPrefixedNode("tenant_a:start", "L")); err != nil {
		t.Fatalf("create tenant_a:start: %v", err)
	}
	if _, err := engine.CreateNode(newPrefixedNode("tenant_b:end", "L")); err != nil {
		t.Fatalf("create tenant_b:end: %v", err)
	}

	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Edge ID lives in tenant_a; StartNode in tenant_a; EndNode in tenant_b.
	// pinEdgeNamespaceLocked must reject because the endpoints disagree.
	err = tx.CreateEdge(newPrefixedEdge("tenant_a:e1", "REL", "tenant_a:start", "tenant_b:end"))
	if err == nil {
		t.Fatalf("CreateEdge with mismatched endpoint namespaces must fail")
	}
	if !errors.Is(err, ErrCrossNamespaceTransaction) {
		t.Fatalf("error must wrap ErrCrossNamespaceTransaction, got: %v", err)
	}
}

func TestBadgerTransaction_CreateEdgeIDInDifferentNamespaceRejected(t *testing.T) {
	engine := newTestEngine(t)
	if _, err := engine.CreateNode(newPrefixedNode("tenant_a:start", "L")); err != nil {
		t.Fatalf("create tenant_a:start: %v", err)
	}
	if _, err := engine.CreateNode(newPrefixedNode("tenant_a:end", "L")); err != nil {
		t.Fatalf("create tenant_a:end: %v", err)
	}

	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Edge ID lives in tenant_b but endpoints live in tenant_a.
	err = tx.CreateEdge(newPrefixedEdge("tenant_b:e1", "REL", "tenant_a:start", "tenant_a:end"))
	if err == nil {
		t.Fatalf("CreateEdge whose ID does not share endpoint namespace must fail")
	}
	if !errors.Is(err, ErrCrossNamespaceTransaction) {
		t.Fatalf("error must wrap ErrCrossNamespaceTransaction, got: %v", err)
	}
}

func TestBadgerTransaction_BulkCreateEdgesMixedNamespaceRejected(t *testing.T) {
	engine := newTestEngine(t)
	for _, id := range []string{"tenant_a:s1", "tenant_a:e1", "tenant_b:s2", "tenant_b:e2"} {
		if _, err := engine.CreateNode(newPrefixedNode(id, "L")); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	edges := []*Edge{
		newPrefixedEdge("tenant_a:r1", "REL", "tenant_a:s1", "tenant_a:e1"),
		newPrefixedEdge("tenant_b:r2", "REL", "tenant_b:s2", "tenant_b:e2"),
	}
	err = tx.BulkCreateEdges(edges)
	if err == nil {
		t.Fatalf("BulkCreateEdges with mixed namespaces must fail")
	}
	if !errors.Is(err, ErrCrossNamespaceTransaction) {
		t.Fatalf("error must wrap ErrCrossNamespaceTransaction, got: %v", err)
	}
	if got := tx.Namespace(); got != "tenant_a" {
		t.Fatalf("first edge in batch must have pinned tenant_a, got %q", got)
	}
}

func TestBadgerTransaction_SetNamespacePinsExplicitly(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := tx.SetNamespace("tenant_a"); err != nil {
		t.Fatalf("SetNamespace: %v", err)
	}
	if got := tx.Namespace(); got != "tenant_a" {
		t.Fatalf("namespace must be %q after SetNamespace, got %q", "tenant_a", got)
	}

	// Idempotent: same namespace again succeeds.
	if err := tx.SetNamespace("tenant_a"); err != nil {
		t.Fatalf("SetNamespace idempotent: %v", err)
	}

	// Conflicting namespace must error.
	err = tx.SetNamespace("tenant_b")
	if err == nil {
		t.Fatalf("SetNamespace with different namespace must fail")
	}
	if !errors.Is(err, ErrCrossNamespaceTransaction) {
		t.Fatalf("error must wrap ErrCrossNamespaceTransaction, got: %v", err)
	}

	// Subsequent write in pinned namespace still works.
	if _, err := tx.CreateNode(newPrefixedNode("tenant_a:n1", "L")); err != nil {
		t.Fatalf("CreateNode after SetNamespace: %v", err)
	}
	// Write into the rejected namespace must still fail.
	_, err = tx.CreateNode(newPrefixedNode("tenant_b:n1", "L"))
	if !errors.Is(err, ErrCrossNamespaceTransaction) {
		t.Fatalf("CreateNode in tenant_b must remain rejected, got: %v", err)
	}
}

func TestBadgerTransaction_SetNamespaceRejectsEmpty(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := tx.SetNamespace(""); err == nil {
		t.Fatalf("SetNamespace must reject empty namespace")
	}
}

func TestBadgerTransaction_PinSurvivesRollbackOfFailedWrite(t *testing.T) {
	// A failed cross-namespace write must NOT alter the pinned namespace,
	// so a subsequent same-namespace write still succeeds. This exercises
	// the "pin only on success" property of pinNamespaceFromIDLocked.
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.CreateNode(newPrefixedNode("tenant_a:n1", "L")); err != nil {
		t.Fatalf("first CreateNode: %v", err)
	}
	if _, err := tx.CreateNode(newPrefixedNode("tenant_b:n1", "L")); !errors.Is(err, ErrCrossNamespaceTransaction) {
		t.Fatalf("expected ErrCrossNamespaceTransaction, got %v", err)
	}
	if got := tx.Namespace(); got != "tenant_a" {
		t.Fatalf("pin must remain %q after rejected write, got %q", "tenant_a", got)
	}
	// Same-namespace write must still succeed.
	if _, err := tx.CreateNode(newPrefixedNode("tenant_a:n2", "L")); err != nil {
		t.Fatalf("subsequent same-namespace write: %v", err)
	}
}
