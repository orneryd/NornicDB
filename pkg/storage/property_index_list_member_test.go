package storage

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sortedNodeIDs(ids []NodeID) []string {
	seen := make(map[NodeID]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, string(id))
	}
	sort.Strings(out)
	return out
}

// An arity-1 index on a list property files each element too, so a
// membership lookup (value IN n.prop) lists the nodes whose list holds the
// value, and those whose property equals it. Equality lookups, ordered scans
// and index statistics read whole values only, as before.
func TestPropertyIndexListMemberLookup(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("alias_keys", "Identity", []string{"alias_keys"}))
	sm.MarkPropertyIndexesFilled()
	for id, value := range map[NodeID]interface{}{
		"i1":     []interface{}{"acme", "acme corp", "acme"},
		"i2":     []string{"globex", "acme"},
		"i3":     []interface{}{int64(1), "x"},
		"scalar": "acme",
		"bytes":  []interface{}{[]byte("raw"), "initech"},
		"empty":  []interface{}{},
		"nulls":  []interface{}{nil, "hooli"},
	} {
		require.NoError(t, sm.PropertyIndexInsert("Identity", "alias_keys", id, value))
	}

	member := func(value interface{}) []string {
		return sortedNodeIDs(sm.PropertyIndexMemberLookup("Identity", "alias_keys", value))
	}
	require.Equal(t, []string{"i1", "i2", "scalar"}, member("acme"))
	require.Equal(t, []string{"i1"}, member("acme corp"))
	require.Equal(t, []string{"i3"}, member(int64(1)))
	require.Equal(t, []string{"i3"}, member(float64(1)), "1 and 1.0 are equal in Cypher")
	require.Equal(t, []string{"bytes"}, member("initech"), "a list is filed by element even without a whole-value key")
	require.Equal(t, []string{"nulls"}, member("hooli"))
	require.Empty(t, member("missing"))
	require.Empty(t, member(nil))
	require.Nil(t, sm.PropertyIndexMemberLookup("Identity", "other", "acme"), "no index")

	// Equality still reads whole values: a list equals a list, not its element.
	require.Equal(t, []string{"i2"}, sortedNodeIDs(sm.PropertyIndexLookup("Identity", "alias_keys", []interface{}{"globex", "acme"})))
	require.Equal(t, []string{"scalar"}, sortedNodeIDs(sm.PropertyIndexLookup("Identity", "alias_keys", "acme")))
	// Ordered scans list scalar values only, once per node.
	require.Equal(t, []string{"scalar"}, sortedNodeIDs(sm.PropertyIndexAllNonNil("Identity", "alias_keys", false)))

	// Statistics count whole values only.
	for _, stats := range sm.GetIndexStats() {
		if stats.Name == "alias_keys" {
			require.Equal(t, int64(6), stats.TotalEntries)
			require.Equal(t, int64(6), stats.UniqueValues)
		}
	}

	// Deleting a value removes its element entries.
	require.NoError(t, sm.PropertyIndexDelete("Identity", "alias_keys", "i1", []interface{}{"acme", "acme corp", "acme"}))
	require.Equal(t, []string{"i2", "scalar"}, member("acme"))
	require.Empty(t, member("acme corp"))
}

// Node writes keep the element entries current: create, update and delete
// through the engine, and an index created over existing nodes (backfill).
func TestPropertyIndexListMemberMaintainedOnWrite(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	schema := engine.GetSchemaForNamespace("db")
	require.NoError(t, schema.AddPropertyIndex("alias_keys", "Identity", []string{"alias_keys"}))
	schema.MarkPropertyIndexesFilled()
	member := func(value interface{}) []string {
		return sortedNodeIDs(schema.PropertyIndexMemberLookup("Identity", "alias_keys", value))
	}

	_, err = engine.CreateNode(&Node{ID: "db:a", Labels: []string{"Identity"}, Properties: map[string]interface{}{"alias_keys": []interface{}{"x", "y"}}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db:b", Labels: []string{"Identity"}, Properties: map[string]interface{}{"alias_keys": []interface{}{"y"}}})
	require.NoError(t, err)
	require.Equal(t, []string{"db:a"}, member("x"))
	require.Equal(t, []string{"db:a", "db:b"}, member("y"))

	require.NoError(t, engine.UpdateNode(&Node{ID: "db:a", Labels: []string{"Identity"}, Properties: map[string]interface{}{"alias_keys": []interface{}{"z"}}}))
	require.Empty(t, member("x"))
	require.Equal(t, []string{"db:b"}, member("y"))
	require.Equal(t, []string{"db:a"}, member("z"))

	require.NoError(t, engine.UpdateNode(&Node{ID: "db:b", Labels: []string{"Other"}, Properties: map[string]interface{}{"alias_keys": []interface{}{"y"}}}))
	require.Empty(t, member("y"), "a node that lost the label leaves the index")

	require.NoError(t, engine.DeleteNode("db:a"))
	require.Empty(t, member("z"))

	// Backfill of an index created over existing nodes files elements too.
	require.NoError(t, schema.AddPropertyIndex("tags", "Doc", []string{"tags"}))
	require.NoError(t, schema.BackfillPropertyIndex("Doc", "tags", map[NodeID]interface{}{"db:d": []interface{}{"t1", "t2"}}))
	require.Equal(t, []string{"db:d"}, sortedNodeIDs(schema.PropertyIndexMemberLookup("Doc", "tags", "t2")))
}

// A transaction's membership lookups see its own writes: created and
// changed nodes by their current lists, changed and deleted nodes without
// their committed entries, both below and above the pending-index size.
func TestMergePendingPropertyMemberMatches(t *testing.T) {
	for _, fill := range []int{0, pendingIndexMinNodes + 1} {
		t.Run(fmt.Sprintf("fill=%d", fill), func(t *testing.T) {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			for _, node := range []*Node{
				{ID: "db:kept", Labels: []string{"P"}, Properties: map[string]interface{}{"k": []interface{}{"a", "b"}}},
				{ID: "db:moved", Labels: []string{"P"}, Properties: map[string]interface{}{"k": []interface{}{"a"}}},
				{ID: "db:gone", Labels: []string{"P"}, Properties: map[string]interface{}{"k": []interface{}{"a"}}},
			} {
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
			}
			committed := func() []NodeID { return []NodeID{"db:kept", "db:moved", "db:gone"} }
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			for i := 0; i < fill; i++ {
				_, err = tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("db:fill%d", i)), Labels: []string{"P"}, Properties: map[string]interface{}{"k": []interface{}{"fill"}}})
				require.NoError(t, err)
			}
			_, err = tx.CreateNode(&Node{ID: "db:new", Labels: []string{"P"}, Properties: map[string]interface{}{"k": []interface{}{"c", "a"}}})
			require.NoError(t, err)
			_, err = tx.CreateNode(&Node{ID: "db:scalar", Labels: []string{"P"}, Properties: map[string]interface{}{"k": "a"}})
			require.NoError(t, err)
			require.NoError(t, tx.UpdateNode(&Node{ID: "db:moved", Labels: []string{"P"}, Properties: map[string]interface{}{"k": []interface{}{"b"}}}))
			require.NoError(t, tx.DeleteNode("db:gone"))

			lookup := func(ids []NodeID, value interface{}) []string {
				return sortedNodeIDs(tx.MergePendingPropertyMemberMatches(ids, "P", "k", value))
			}
			require.Equal(t, []string{"db:kept", "db:new", "db:scalar"}, lookup(committed(), "a"))
			require.Equal(t, []string{"db:moved"}, lookup(nil, "b"))
			require.Equal(t, []string{"db:new"}, lookup(nil, "c"))
			require.Empty(t, lookup(nil, "missing"))
			// Equality lookups are unchanged: a list's element is not its value.
			require.Equal(t, []string{"db:scalar"}, sortedNodeIDs(tx.MergePendingPropertyMatches(nil, "P", "k", "a")))
		})
	}
}

// Buffered (not yet flushed) writes are seen by membership lookups through
// the pending view, as by equality lookups.
func TestWriteBehind_PropertyIndexMemberSeesBufferedNodes(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddPropertyIndex("alias_keys", "Identity", []string{"alias_keys"}))
	schema.MarkPropertyIndexesFilled()

	bufferedCreate(t, engine, &Node{ID: "test:i1", Labels: []string{"Identity"}, Properties: map[string]any{"alias_keys": []interface{}{"acme", "globex"}}})
	require.Equal(t, []string{"test:i1"}, sortedNodeIDs(schema.PropertyIndexMemberLookup("Identity", "alias_keys", "globex")),
		"membership lookup must see buffered nodes before the flush")
	require.Empty(t, schema.PropertyIndexMemberLookup("Identity", "alias_keys", "initech"))

	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, []string{"test:i1"}, sortedNodeIDs(schema.PropertyIndexMemberLookup("Identity", "alias_keys", "globex")))

	// A buffered rewrite supersedes the committed entries.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:i1", Labels: []string{"Identity"}, Properties: map[string]any{"alias_keys": []interface{}{"initech"}}}))
	require.NoError(t, tx.Commit())
	require.Empty(t, schema.PropertyIndexMemberLookup("Identity", "alias_keys", "globex"))
	require.Equal(t, []string{"test:i1"}, sortedNodeIDs(schema.PropertyIndexMemberLookup("Identity", "alias_keys", "initech")))
	require.NoError(t, engine.FlushWriteBehind())
	require.Empty(t, schema.PropertyIndexMemberLookup("Identity", "alias_keys", "globex"))
	require.Equal(t, []string{"test:i1"}, sortedNodeIDs(schema.PropertyIndexMemberLookup("Identity", "alias_keys", "initech")))
}
