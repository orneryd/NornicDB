package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMergePendingPropertyMatches: a property-index lookup of committed
// state, merged with a transaction's own writes, is what the transaction
// sees — created and changed nodes by their current values, changed and
// deleted nodes without their committed entries. The pending index is built
// by the first lookup and kept current by every later write.
func TestMergePendingPropertyMatches(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, node := range []*Node{
		{ID: "db:kept", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "a"}},
		{ID: "db:moved", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "a"}},
		{ID: "db:gone", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "a"}},
		{ID: "db:unlabeled", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "a"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	// What a (P, k) index holds for "a" and "b" in committed state.
	committedA := func() []NodeID { return []NodeID{"db:kept", "db:moved", "db:gone", "db:unlabeled"} }
	lookup := func(tx *BadgerTransaction, committed []NodeID, label, property string, value interface{}) []string {
		ids := tx.MergePendingPropertyMatches(committed, label, property, value)
		out := make([]string, 0, len(ids))
		for _, id := range ids {
			out = append(out, string(id))
		}
		sort.Strings(out)
		return out
	}

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// No writes: the committed lookup is returned as it is, and no pending
	// index is built.
	require.Equal(t, []string{"db:gone", "db:kept", "db:moved", "db:unlabeled"}, lookup(tx, committedA(), "P", "k", "a"))
	require.Nil(t, tx.pendingIndex)

	// A few pending nodes are compared directly, without an index.
	_, err = tx.CreateNode(&Node{ID: "db:early", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "e", "n": int64(1)}})
	require.NoError(t, err)
	require.Equal(t, []string{"db:early"}, lookup(tx, nil, "P", "k", "e"))
	require.Equal(t, []string{"db:early"}, lookup(tx, nil, "P", "n", float64(1)))
	require.Empty(t, lookup(tx, nil, "Q", "k", "e"))
	require.Empty(t, lookup(tx, nil, "P", "k", "other"))
	require.Nil(t, tx.pendingIndex)

	// With more of them, writes made before the first lookup are indexed by
	// that lookup.
	for i := 0; i < pendingIndexMinNodes; i++ {
		_, err = tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("db:fill%d", i)), Labels: []string{"P"}, Properties: map[string]interface{}{"k": "fill"}})
		require.NoError(t, err)
	}
	_, err = tx.CreateNode(&Node{ID: "db:new", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "a"}})
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{ID: "db:moved", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "b"}}))
	require.NoError(t, tx.DeleteNode("db:gone"))
	require.NoError(t, tx.UpdateNode(&Node{ID: "db:unlabeled", Labels: []string{"Q"}, Properties: map[string]interface{}{"k": "a"}}))
	require.Nil(t, tx.pendingIndex)
	require.Equal(t, []string{"db:kept", "db:new"}, lookup(tx, committedA(), "P", "k", "a"))
	require.NotNil(t, tx.pendingIndex)
	require.Equal(t, []string{"db:moved"}, lookup(tx, nil, "P", "k", "b"))
	require.Equal(t, []string{"db:unlabeled"}, lookup(tx, nil, "Q", "k", "a"))
	require.Empty(t, lookup(tx, nil, "P", "k", "missing"))

	// Writes made after it keep the index current: a second update of a
	// pending node, a delete of a pending node, a create.
	require.NoError(t, tx.UpdateNode(&Node{ID: "db:moved", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "c"}}))
	require.NoError(t, tx.DeleteNode("db:new"))
	_, err = tx.CreateNode(&Node{ID: "db:later", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "b"}})
	require.NoError(t, err)
	require.Equal(t, []string{"db:kept"}, lookup(tx, committedA(), "P", "k", "a"))
	require.Equal(t, []string{"db:later"}, lookup(tx, nil, "P", "k", "b"))
	require.Equal(t, []string{"db:moved"}, lookup(tx, nil, "P", "k", "c"))

	// A second property is indexed by its first lookup, for the nodes
	// already pending.
	require.NoError(t, tx.UpdateNode(&Node{ID: "db:kept", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "a", "other": int64(7)}}))
	require.Equal(t, []string{"db:kept"}, lookup(tx, nil, "P", "other", int64(7)))
	require.Equal(t, []string{"db:kept"}, lookup(tx, nil, "P", "other", float64(7)), "numeric values match as the property indexes match them")

	// A value no index can hold (a list) matches nothing, and still drops
	// the committed entries the transaction superseded.
	require.Equal(t, []string{"db:unseen"}, lookup(tx, []NodeID{"db:unseen", "db:gone"}, "P", "k", []interface{}{"a"}))
}
