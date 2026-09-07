package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_PendingCreateNodeOperationIndexLocked(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	tx.operations = []Operation{
		{Type: OpCreateNode, NodeID: "test:n1"},
		{Type: OpUpdateNode, NodeID: "test:n1"},
	}
	require.Equal(t, 0, tx.pendingCreateNodeOperationIndexLocked("test:n1"))

	tx.operations = append(tx.operations, Operation{Type: OpDeleteNode, NodeID: "test:n1"})
	require.Equal(t, -1, tx.pendingCreateNodeOperationIndexLocked("test:n1"))
	require.Equal(t, -1, tx.pendingCreateNodeOperationIndexLocked("test:missing"))
}

func TestBadgerTransaction_ConflictHelpers(t *testing.T) {
	engine := newTestEngine(t)

	// Seed a visible node/edge with current heads.
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"User"}, Properties: map[string]any{"email": "a@example.com"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"User"}, Properties: map[string]any{"email": "b@example.com"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()
	require.NoError(t, tx.SetDeferredConstraintValidation(true))
	require.NoError(t, tx.SetSkipCreateExistenceCheck(true))

	// Old readTS to force SI conflicts against current heads.
	tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(0, 0).UTC(), CommitSequence: 0}

	err = tx.checkNodeCreateConflict("test:a")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict)

	err = tx.checkEdgeCreateConflict("test:e1")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict)

	err = tx.checkEdgeWriteConflict("test:e1")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict)

	// Endpoint deleted-after-start branch -> conflict.
	_, err = tx.CreateNode(&Node{ID: "test:c", Labels: []string{"User"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx2.SetNamespace("test"))
	tx2.readTS = MVCCVersion{CommitTimestamp: time.Unix(0, 0).UTC(), CommitSequence: 0}
	defer tx2.Rollback()

	// Tombstone endpoint after tx2 readTS.
	require.NoError(t, engine.DeleteNode("test:c"))
	err = tx2.checkEdgeEndpointConflicts(&Edge{ID: "test:ec", StartNode: "test:c", EndNode: "test:b", Type: "REL"})
	require.Error(t, err)
}

func TestBadgerTransaction_GetNodesByLabelLocked_Branches(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:u1", Labels: []string{"User"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	// readTS == zero branch.
	nodes, err := tx.getNodesByLabelLocked("User")
	require.NoError(t, err)
	require.NotEmpty(t, nodes)

	// readTS != zero branch.
	tx.readTS = MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
	nodes, err = tx.getNodesByLabelLocked("User")
	require.NoError(t, err)
	require.NotEmpty(t, nodes)
}

func TestBadgerTransaction_ConflictHelperAdditionalBranches(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"User"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"User"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e", StartNode: "test:n1", EndNode: "test:n2", Type: "REL"}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	// Missing entities on create-conflict checks are non-conflicting.
	require.NoError(t, tx.checkNodeCreateConflict("test:missing"))
	require.NoError(t, tx.checkEdgeCreateConflict("test:missing"))
	// Existing heads already visible to this transaction are also non-conflicting.
	require.NoError(t, tx.checkNodeCreateConflict("test:n1"))
	require.NoError(t, tx.checkEdgeCreateConflict("test:e"))

	// snapshotIsolationConflict branches: same sequence (non-max) => false.
	tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(10, 0).UTC(), CommitSequence: 9}
	require.False(t, tx.snapshotIsolationConflict(MVCCVersion{
		CommitTimestamp: time.Unix(20, 0).UTC(),
		CommitSequence:  9,
	}))
	// Max sequence fallback to timestamp compare.
	tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(10, 0).UTC(), CommitSequence: maxMVCCCommitSequence}
	require.True(t, tx.snapshotIsolationConflict(MVCCVersion{
		CommitTimestamp: time.Unix(11, 0).UTC(),
		CommitSequence:  maxMVCCCommitSequence,
	}))

	// nil edge shortcut branch.
	require.NoError(t, tx.checkEdgeEndpointConflicts(nil))

	// Endpoint not found in either head/body => invalid edge.
	err = tx.checkEdgeEndpointConflicts(&Edge{ID: "test:e-missing", StartNode: "test:missing", EndNode: "test:n2", Type: "REL"})
	require.ErrorIs(t, err, ErrInvalidEdge)
}

func TestBadgerTransaction_CheckNodeAdjacencyConflict_Branches(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"v": int64(1)}}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	// Old read timestamp to force conflict after concurrent edge write.
	tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(0, 0).UTC(), CommitSequence: 0}
	require.NoError(t, engine.UpdateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"v": int64(2)}}))

	err = tx.checkNodeAdjacencyConflict("test:a")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict)

	// Node without dictionary/index entries uses empty-prefix path and should not fail.
	require.NoError(t, tx.checkNodeAdjacencyConflict("test:missing"))
}

func TestBadgerTransaction_CheckNodeAdjacencyConflict_IgnoresStaleIndexEntries(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(func(view *badger.Txn) error {
		prefix := engine.outgoingIndexPrefixString("test:a")
		require.NotNil(t, prefix)
		if err := view.Set(append(append([]byte{}, prefix...), []byte("bad")...), nil); err != nil {
			return err
		}
		nodeNum, ok := engine.idDict.lookupNodeNumID("test:a")
		require.True(t, ok)
		if err := view.Set(outgoingIndexKey(nodeNum, 999999999), nil); err != nil {
			return err
		}
		missingHeadKey, err := engine.outgoingIndexKeyString(view, "test:a", "test:missing-head")
		if err != nil {
			return err
		}
		return view.Set(missingHeadKey, nil)
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.checkNodeAdjacencyConflict("test:a"))
}

func TestBadgerTransaction_CheckNodeAdjacencyConflict_HeadDecodeErrorBranch(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e-bad-head", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	// Corrupt the edge MVCC head so loadEdgeMVCCHeadInTxn returns a decode error.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		headKey, err := engine.mvccEdgeHeadKeyString(txn, "test:e-bad-head")
		if err != nil {
			return err
		}
		return txn.Set(headKey, []byte{0x01})
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(0, 0).UTC(), CommitSequence: 0}

	err = tx.checkNodeAdjacencyConflict("test:a")
	require.Error(t, err)
}

func TestBadgerTransaction_CheckEdgeUniqueness_CompositeMissingPropertyBranches(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:         "test:e1",
		StartNode:  "test:a",
		EndNode:    "test:b",
		Type:       "REL",
		Properties: map[string]any{"k1": "v1", "k2": "v2"},
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	c := Constraint{
		Type:       ConstraintUnique,
		Label:      "REL",
		Properties: []string{"k1", "k2"},
	}
	candidate := &Edge{
		ID:         "test:new",
		StartNode:  "test:a",
		EndNode:    "test:b",
		Type:       "REL",
		Properties: map[string]any{"k1": "v1"}, // Missing k2 triggers allPresent=false branches.
	}

	// Pending-edge branch (allPresent=false => nil).
	tx.pendingEdges["test:pending"] = &Edge{
		ID:         "test:pending",
		Type:       "REL",
		Properties: map[string]any{"k1": "v1", "k2": "x"},
	}
	require.NoError(t, tx.checkEdgeUniqueness(candidate, c, "test"))

	// Committed-edge branch (allPresent=false => nil).
	tx.pendingEdges = map[EdgeID]*Edge{}
	require.NoError(t, tx.checkEdgeUniqueness(candidate, c, "test"))
}

func TestBadgerTransaction_NodeConstraintExtraBranches(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:          "domain_status",
		Type:          ConstraintDomain,
		Label:         "User",
		Properties:    []string{"status"},
		AllowedValues: []any{"ok", "ready"},
	}))
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "node_key_user",
		Type:       ConstraintNodeKey,
		Label:      "User",
		Properties: []string{"tenant", "email"},
	}))

	// Domain constraint branch: disallowed value.
	err = tx.validateNodeConstraints(&Node{
		ID:         "test:u1",
		Labels:     []string{"User"},
		Properties: map[string]any{"status": "bad"},
	})
	require.Error(t, err)

	// Node key pending compareValues mismatch branch.
	tx.pendingNodes["test:u2"] = &Node{
		ID:         "test:u2",
		Labels:     []string{"User"},
		Properties: map[string]any{"tenant": "t1", "email": "other@example.com"},
	}
	err = tx.checkNodeKeyConstraint(&Node{
		ID:         "test:u3",
		Labels:     []string{"User"},
		Properties: map[string]any{"tenant": "t1", "email": "u3@example.com"},
	}, Constraint{Type: ConstraintNodeKey, Label: "User", Properties: []string{"tenant", "email"}})
	require.NoError(t, err)
}

func TestBadgerTransaction_BulkCreateEdges_NodeVisibleDeletedBranch(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, tx.SetNamespace("test"))
	tx.deletedNodes["test:deleted"] = struct{}{}

	err = tx.BulkCreateEdges([]*Edge{{
		ID:        "test:e-del",
		StartNode: "test:deleted",
		EndNode:   "test:other",
		Type:      "REL",
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "start node")
}

func TestBadgerTransaction_PinNamespaceFromIDLocked_RefreshFailureBranch(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true, err: ErrMVCCResourcePressure}
	engine.SetLifecycleController(controller)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	defer tx.Rollback()

	tx.mu.Lock()
	err = tx.pinNamespaceFromIDLocked("test:n1")
	tx.mu.Unlock()
	require.ErrorIs(t, err, ErrMVCCResourcePressure)
	require.Equal(t, "", tx.namespace)
}

func TestBadgerTransaction_Commit_OperationCallbacksMatrix(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:s", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:e", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:cm-node-delete", Labels: []string{"User"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:cm-edge-deleted-with-node",
		StartNode: "test:cm-node-delete",
		EndNode:   "test:e",
		Type:      "REL",
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	require.NoError(t, tx.SetMetadata(map[string]interface{}{"trace_id": "abc123"}))

	tx.operations = []Operation{
		{
			Type: OpCreateNode,
			Node: &Node{
				ID:         "test:cm-node-create",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "create@example.com"},
			},
		},
		{
			Type: OpUpdateNode,
			Node: &Node{
				ID:         "test:cm-node-update",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "after@example.com"},
			},
			OldNode: &Node{
				ID:         "test:cm-node-update",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "before@example.com"},
			},
		},
		{
			Type:           OpDeleteNode,
			NodeID:         "test:cm-node-delete",
			OldNode:        &Node{ID: "test:cm-node-delete", Labels: []string{"User"}, Properties: map[string]any{"email": "gone@example.com"}},
			DeletedEdgeIDs: []EdgeID{"test:cm-edge-deleted-with-node"},
		},
		{
			Type: OpCreateEdge,
			Edge: &Edge{ID: "test:cm-edge-create", StartNode: "test:s", EndNode: "test:e", Type: "REL"},
		},
		{
			Type:    OpUpdateEdge,
			Edge:    &Edge{ID: "test:cm-edge-update", StartNode: "test:s", EndNode: "test:e", Type: "REL2"},
			OldEdge: &Edge{ID: "test:cm-edge-update", StartNode: "test:s", EndNode: "test:e", Type: "REL1"},
		},
		{
			Type:    OpDeleteEdge,
			EdgeID:  "test:cm-edge-delete",
			OldEdge: &Edge{ID: "test:cm-edge-delete", StartNode: "test:s", EndNode: "test:e", Type: "REL"},
		},
		{
			Type: OpUpdateEmbedding,
			Node: &Node{ID: "test:cm-embed"},
		},
	}

	require.NoError(t, tx.Commit())
	require.Equal(t, TxStatusCommitted, tx.Status)
	require.NotZero(t, tx.CommitVersion.CommitSequence)
}

func TestBadgerTransaction_Commit_WritesWithoutNamespaceBranch(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	tx.operations = []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:cm-no-ns", Labels: []string{"N"}}}}
	tx.namespace = ""

	err = tx.Commit()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no pinned namespace")
	require.Equal(t, TxStatusRolledBack, tx.Status)
}
