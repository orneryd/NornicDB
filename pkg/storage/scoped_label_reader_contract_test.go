package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func scopedLabelOrderFixture(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, id := range []NodeID{"a:z", "b:z", "a:a", "a:m", "ab:foreign"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Person"}, Properties: map[string]any{"id": string(id), "extra": true}})
		require.NoError(t, err)
	}
	_, err = engine.CreateNode(&Node{ID: "a:lower", Labels: []string{"person"}})
	require.NoError(t, err)
	return engine
}

func orderedScopedIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func TestScopedLabelReadersPreserveOrderAndExactMembership(t *testing.T) {
	engine := scopedLabelOrderFixture(t)
	want := []NodeID{"a:z", "a:a", "a:m"}
	nodes, err := engine.GetNodesByLabelInScope("a:", "Person")
	require.NoError(t, err)
	require.Equal(t, want, orderedScopedIDs(nodes))
	first, err := engine.GetFirstNodeByLabelInScope("a:", "Person")
	require.NoError(t, err)
	require.Equal(t, want[0], first.ID)
	var streamed []*Node
	require.NoError(t, engine.StreamNodesByLabelProjectedInScope("a:", "Person", []string{"id"}, func(node *Node) error {
		require.Equal(t, map[string]any{"id": string(node.ID)}, node.Properties)
		streamed = append(streamed, node)
		return nil
	}))
	require.Equal(t, want, orderedScopedIDs(streamed))
	nodes, err = engine.GetNodesByLabelVisibleAtInScope("a:", "Person", engine.currentMVCCReadVersion("a"))
	require.NoError(t, err)
	require.Equal(t, want, orderedScopedIDs(nodes))
	for _, query := range []struct{ scope, label string }{{"missing:", "Person"}, {"a:", "Missing"}} {
		nodes, err := engine.GetNodesByLabelInScope(query.scope, query.label)
		require.NoError(t, err)
		require.Empty(t, nodes)
		first, err := engine.GetFirstNodeByLabelInScope(query.scope, query.label)
		require.NoError(t, err)
		require.Nil(t, first)
	}
	nodes, err = engine.GetNodesByLabelInScope("a:", "person")
	require.NoError(t, err)
	require.Equal(t, []NodeID{"a:lower"}, orderedScopedIDs(nodes))
	nodes, err = engine.GetNodesByLabelInScope("", "Person")
	require.NoError(t, err)
	require.Equal(t, []NodeID{"a:z", "b:z", "a:a", "a:m", "ab:foreign"}, orderedScopedIDs(nodes))
	stop := errors.New("caller stopped")
	visits := 0
	require.ErrorIs(t, engine.StreamNodesByLabelProjectedInScope("a:", "Person", nil, func(*Node) error {
		visits++
		return stop
	}), stop)
	require.Equal(t, 1, visits)
}

func TestScopedLabelSnapshotResumeSurvivesLiveDictionaryEviction(t *testing.T) {
	engine := scopedLabelOrderFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("a"))
	defer tx.Rollback()
	require.ErrorIs(t, tx.StreamNodesByLabelProjected("Person", nil, func(node *Node) error {
		require.Equal(t, NodeID("a:z"), node.ID)
		return ErrIterationStopped
	}), ErrIterationStopped)
	// The BEGIN snapshot, including its resume cursor, outlives a deletion
	// and removal of that ID from the latest-state dictionary cache.
	require.NoError(t, engine.DeleteNode("a:z"))
	engine.idDict.forgetNodeCache("a:z")
	engine.idDict.forgetNodeCache("a:m")
	var ids []NodeID
	require.NoError(t, tx.StreamNodesByLabelProjected("Person", nil, func(node *Node) error {
		ids = append(ids, node.ID)
		return nil
	}))
	require.Equal(t, []NodeID{"a:z", "a:a", "a:m"}, ids)
}

func TestScopedLabelReadersKeepBufferedReplacementVisibility(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	for _, id := range []NodeID{"test:remove-label", "test:delete", "other:foreign"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Person"}})
		require.NoError(t, err)
	}
	_, err := engine.CreateNode(&Node{ID: "test:add-label", Labels: []string{"Other"}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:remove-label", Labels: []string{"Other"}}))
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:add-label", Labels: []string{"Person"}, Properties: map[string]any{"v": int64(7)}}))
	require.NoError(t, tx.DeleteNode("test:delete"))
	_, err = tx.CreateNode(&Node{ID: "test:new", Labels: []string{"Person", "NewOnly"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	check := func() {
		first, err := engine.GetFirstNodeByLabelInScope("test:", "Person")
		require.NoError(t, err)
		require.NotNil(t, first)
		require.Contains(t, []NodeID{"test:add-label", "test:new"}, first.ID)
		first, err = engine.GetFirstNodeByLabelInScope("test:", "NewOnly")
		require.NoError(t, err)
		require.NotNil(t, first)
		require.Equal(t, NodeID("test:new"), first.ID)
		nodes, err := engine.GetNodesByLabelInScope("test:", "Person")
		require.NoError(t, err)
		require.Equal(t, []string{"test:add-label", "test:new"}, sortedIDs(nodes))
		var streamed []*Node
		require.NoError(t, engine.StreamNodesByLabelProjectedInScope("test:", "Person", []string{"v"}, func(node *Node) error {
			streamed = append(streamed, node)
			return nil
		}))
		require.Equal(t, []string{"test:add-label", "test:new"}, sortedIDs(streamed))
		count, err := engine.NodeCountByLabelInNamespace("test", "Person")
		require.NoError(t, err)
		require.Equal(t, int64(2), count)
	}
	check()
	require.NoError(t, engine.FlushWriteBehind())
	check()
}

func TestScopedLabelBufferedMembershipUsesNewestGeneration(t *testing.T) {
	apply := newBlockingApply()
	buffer := NewWriteBehindBuffer(time.Hour, 0, apply.fn)
	defer buffer.Close()
	defer func() {
		close(apply.release)
		require.NoError(t, buffer.Flush())
	}()
	person := &Node{ID: "test:n", Labels: []string{"Person"}}
	other := &Node{ID: "test:n", Labels: []string{"Other"}}
	buffer.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: person}}})
	buffer.FlushAsync()
	select {
	case <-apply.started:
	case <-time.After(5 * time.Second):
		t.Fatal("buffer did not begin draining")
	}
	buffer.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpUpdateNode, OldNode: person, Node: other}}})
	nodes, touched := buffer.LabelOverlay("Person")
	require.Empty(t, nodes)
	require.True(t, touched[person.ID], "newer label removal shadows the draining membership")
	require.Empty(t, buffer.LabelNodes("Person"))
	require.Equal(t, []NodeID{other.ID}, orderedScopedIDs(buffer.LabelNodes("Other")))
	// One generation has now touched both labels. Replacing its current
	// body again must neither emit stale memberships nor duplicate the ID.
	buffer.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpUpdateNode, OldNode: other, Node: person}}})
	require.Equal(t, []NodeID{person.ID}, orderedScopedIDs(buffer.LabelNodes("Person")))
	require.Empty(t, buffer.LabelNodes("Other"))
}

func TestScopedLabelSnapshotCursorErrors(t *testing.T) {
	engine := scopedLabelOrderFixture(t)
	for _, scope := range []string{"", "a:"} {
		var ids []NodeID
		err := engine.streamNodesByLabelFromPhysicalSnapshotAfter(scope, "Person", engine.withView, nil, true, "missing:n", func(node *Node) error {
			ids = append(ids, node.ID)
			return nil
		})
		require.ErrorIs(t, err, ErrNotFound)
		require.Empty(t, ids)
	}
	var resumed []NodeID
	require.NoError(t, engine.streamNodesByLabelFromPhysicalSnapshotAfter("", "Person", engine.withView, nil, true, "a:z", func(node *Node) error {
		resumed = append(resumed, node.ID)
		return nil
	}))
	require.Equal(t, []NodeID{"b:z", "a:a", "a:m", "ab:foreign"}, resumed)
	// A damaged cursor mapping is an error, not a different resume position.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(nodeIDForwardKey("a:z"), []byte{1})
	}))
	require.ErrorIs(t, engine.streamNodesByLabelFromPhysicalSnapshotAfter("a:", "Person", engine.withView, nil, true, "a:z", func(*Node) error {
		t.Fatal("corrupt cursor must not produce rows")
		return nil
	}), ErrInvalidData)
}

func TestScopedLabelIgnoresMalformedAndUnresolvedMemberships(t *testing.T) {
	engine := scopedLabelOrderFixture(t)
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(labelIndexPrefix("Person"), nil); err != nil {
			return err
		}
		return txn.Set(labelIndexKey("Person", ^uint64(0)), nil)
	}))
	for _, scope := range []string{"a:", ""} {
		nodes, err := engine.GetNodesByLabelInScope(scope, "Person")
		require.NoError(t, err)
		want := []NodeID{"a:z", "a:a", "a:m"}
		if scope == "" {
			want = []NodeID{"a:z", "b:z", "a:a", "a:m", "ab:foreign"}
		}
		require.Equal(t, want, orderedScopedIDs(nodes))
	}
}
