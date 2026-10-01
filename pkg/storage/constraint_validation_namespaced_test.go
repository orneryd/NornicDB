package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMonster531ConcurrentCompositeUnique(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) {
			engine, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "cu1", Type: ConstraintUnique, Label: "CU", Properties: []string{"a", "b"}}))
			for iteration := 0; iteration < 20; iteration++ {
				start := make(chan struct{})
				results := make(chan error, 2)
				var writers sync.WaitGroup
				for writer := 0; writer < 2; writer++ {
					writers.Add(1)
					go func(writer int) {
						defer writers.Done()
						node := &Node{ID: NodeID(fmt.Sprintf("test:%d-%d", iteration, writer)), Labels: []string{"CU"}, Properties: map[string]interface{}{"a": int64(iteration), "b": int64(1)}}
						if explicit {
							transaction, err := engine.BeginTransaction()
							if err != nil {
								results <- err
								return
							}
							defer transaction.Rollback()
							<-start
							_, err = transaction.CreateNode(node)
							if err == nil {
								err = transaction.Commit()
							}
							results <- err
							return
						}
						<-start
						_, err := engine.CreateNode(node)
						results <- err
					}(writer)
				}
				close(start)
				writers.Wait()
				close(results)
				successes := 0
				for err := range results {
					if err == nil {
						successes++
					}
				}
				require.Equal(t, 1, successes, "duplicate tuple admitted by concurrent writers")
			}
		})
	}
}

func TestMonster531CompositeUniqueStorage(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	constraint := Constraint{Name: "cu1", Type: ConstraintUnique, Label: "CU", Properties: []string{"a", "b"}}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))
	require.NoError(t, engine.GetSchemaForNamespace("other").AddConstraint(constraint))
	node := func(id string, first, second interface{}) *Node {
		return &Node{ID: NodeID(id), Labels: []string{"CU"}, Properties: map[string]interface{}{"a": first, "b": second}}
	}
	_, err = engine.CreateNode(node("test:first", int64(1), "two"))
	require.NoError(t, err)
	_, err = engine.CreateNode(node("test:duplicate", float64(1), "two"))
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintUnique, violation.Type)
	_, err = engine.CreateNode(node("other:first", int64(1), "two"))
	require.NoError(t, err)
	_, err = engine.CreateNode(node("test:string", "1", "two"))
	require.NoError(t, err)
	_, err = engine.CreateNode(node("test:null1", int64(1), nil))
	require.NoError(t, err)
	_, err = engine.CreateNode(node("test:null2", int64(1), nil))
	require.NoError(t, err)
	require.NoError(t, ValidateConstraintOnCreationForEngine(NewNamespacedEngine(engine, "test"), constraint))

	transaction, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer transaction.Rollback()
	_, err = transaction.CreateNode(node("test:tx_duplicate", int64(1), "two"))
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintUnique, violation.Type)
	_, err = transaction.CreateNode(node("test:pending1", "a b", "c"))
	require.NoError(t, err)
	_, err = transaction.CreateNode(node("test:pending2", "a", "b c"))
	require.NoError(t, err)
	_, err = transaction.CreateNode(node("test:pending_duplicate", "a b", "c"))
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintUnique, violation.Type)
	_, err = transaction.CreateNode(node("test:pending_null1", nil, "c"))
	require.NoError(t, err)
	_, err = transaction.CreateNode(node("test:pending_null2", nil, "c"))
	require.NoError(t, err)
	require.NoError(t, transaction.Rollback())
	_, err = engine.GetNode("test:pending1")
	require.Error(t, err)
}

// TestRefreshUniqueConstraint_KeepsPrefixedIDs_UnderNamespacedEngine pins the
// fix at constraint_validation.go:71. Removing EnsureNodeIDDatabasePrefixForEngine
// from the rebuild path would re-introduce the documented "false UNIQUE on
// MATCH/SET against a pre-existing node" failure mode that downstream Bolt
// consumers pin as a contract.
//
// See docs/plans/consumer-pinned-error-contract-plan.md §2.3. Do not relax
// this test without coordinating with known consumers.
func TestRefreshUniqueConstraint_KeepsPrefixedIDs_UnderNamespacedEngine(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })

	const namespace = "tenant_a"
	ns := NewNamespacedEngine(inner, namespace)

	// 1. Bootstrap a UNIQUE constraint and one node BEFORE the rebuild,
	//    matching a real consumer's "constraint exists at first boot" case.
	schema := inner.GetSchemaForNamespace(namespace)
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "unique_uid",
		Type:       ConstraintUnique,
		Label:      "T",
		Properties: []string{"uid"},
	}))

	// Drive through the namespaced wrapper so the storage ID acquires the
	// namespace prefix the way real consumers do — NOT via the bare engine.
	_, err := ns.CreateNode(&Node{
		ID:         "n-1",
		Labels:     []string{"T"},
		Properties: map[string]any{"uid": "abc-123"},
	})
	require.NoError(t, err)

	// 2. Force the namespace-aware rebuild path. Pre-fix, this populated
	//    the cache with unprefixed IDs because AllNodes-via-the-namespaced-
	//    engine returns user-prefix-stripped nodes.
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(ns, schema))

	// 3. The cache must hold the storage-prefixed ID. CheckUniqueConstraint
	//    against the SAME node's storage ID must NOT raise a violation —
	//    that is the post-fix behavior.
	storageID := EnsureNodeIDDatabasePrefixForEngine(ns, "n-1")
	require.NoError(
		t,
		schema.CheckUniqueConstraint("T", "uid", "abc-123", storageID),
		"false UNIQUE on the matched node itself: see consumer-pinned-error-contract-plan.md §2.3",
	)

	// 4. A different storage ID claiming the same value MUST still violate.
	require.Error(t, schema.CheckUniqueConstraint("T", "uid", "abc-123", NodeID(namespace+":n-2")))
}
