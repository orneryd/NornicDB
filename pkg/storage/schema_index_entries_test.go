package storage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeIndexReplaceNodeSingleProperty(t *testing.T) {
	idx := &CompositeIndex{Label: "Doc", Properties: []string{"id"}}
	idx.ReplaceNode("a", nil, map[string]interface{}{"id": "k"})
	idx.ReplaceNode("b", nil, map[string]interface{}{"id": "k"})
	require.Equal(t, []NodeID{"a", "b"}, idx.LookupFull("k"))

	// An unchanged key keeps its entry in place: no removal, no re-sort.
	idx.keysDirty = false
	idx.ReplaceNode("a", map[string]interface{}{"id": "k", "c": 1}, map[string]interface{}{"id": "k", "c": 2})
	require.Equal(t, []NodeID{"a", "b"}, idx.LookupFull("k"))
	require.False(t, idx.keysDirty)

	// A changed key moves the node.
	idx.ReplaceNode("a", map[string]interface{}{"id": "k"}, map[string]interface{}{"id": "m"})
	require.Equal(t, []NodeID{"b"}, idx.LookupFull("k"))
	require.Equal(t, []NodeID{"a"}, idx.LookupFull("m"))

	// Losing the key, or the index's label (nil new properties), removes it;
	// a key-less value files nothing.
	idx.ReplaceNode("a", map[string]interface{}{"id": "m"}, map[string]interface{}{"other": 1})
	idx.ReplaceNode("b", map[string]interface{}{"id": "k"}, nil)
	idx.ReplaceNode("c", nil, map[string]interface{}{"id": nil})
	require.Empty(t, idx.values)
	require.True(t, idx.keysDirty)

	// A node missing from the index is filed by an update that keeps its key.
	idx.ReplaceNode("d", map[string]interface{}{"id": "k"}, map[string]interface{}{"id": "k"})
	require.Equal(t, []NodeID{"d"}, idx.LookupFull("k"))
	idx.RemoveNode("absent", map[string]interface{}{"id": "zzz"})
	require.Equal(t, []NodeID{"d"}, idx.LookupFull("k"))
}

func TestCompositeIndexReplaceNodeComposite(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddCompositeIndex("loc", "Place", []string{"country", "city"}))
	idx, ok := sm.GetCompositeIndex("loc")
	require.True(t, ok)
	full := map[string]interface{}{"country": "US", "city": "NYC"}
	require.NoError(t, idx.IndexNode("a", full))
	require.Equal(t, []NodeID{"a"}, idx.LookupFull("US", "NYC"))
	require.Equal(t, []NodeID{"a"}, idx.LookupPrefix("US"))

	// Unchanged values keep both entries.
	idx.ReplaceNode("a", full, map[string]interface{}{"country": "US", "city": "NYC", "pop": 8})
	require.Equal(t, []NodeID{"a"}, idx.LookupFull("US", "NYC"))
	require.Equal(t, []NodeID{"a"}, idx.LookupPrefix("US"))

	// Changing the trailing value keeps the shared prefix and moves the full key.
	idx.ReplaceNode("a", full, map[string]interface{}{"country": "US", "city": "LA"})
	require.Empty(t, idx.LookupFull("US", "NYC"))
	require.Equal(t, []NodeID{"a"}, idx.LookupFull("US", "LA"))
	require.Equal(t, []NodeID{"a"}, idx.LookupPrefix("US"))

	// Dropping the trailing property leaves only the prefix entry; restoring
	// it files the full key again.
	idx.ReplaceNode("a", map[string]interface{}{"country": "US", "city": "LA"}, map[string]interface{}{"country": "US"})
	require.Empty(t, idx.LookupFull("US", "LA"))
	require.Equal(t, []NodeID{"a"}, idx.LookupPrefix("US"))
	idx.ReplaceNode("a", map[string]interface{}{"country": "US"}, full)
	require.Equal(t, []NodeID{"a"}, idx.LookupFull("US", "NYC"))

	// Changing the leading value moves every entry; removal empties the index.
	idx.ReplaceNode("a", full, map[string]interface{}{"country": "CA", "city": "NYC"})
	require.Empty(t, idx.LookupPrefix("US"))
	require.Equal(t, []NodeID{"a"}, idx.LookupFull("CA", "NYC"))
	idx.RemoveNode("a", map[string]interface{}{"country": "CA", "city": "NYC"})
	require.Empty(t, idx.fullIndex)
	require.Empty(t, idx.prefixIndex)
}

// TestCompositeIndexReplaceNodeNeverHidesUnchangedKey races seeks against
// updates that keep the key: every seek must see the node.
func TestCompositeIndexReplaceNodeNeverHidesUnchangedKey(t *testing.T) {
	single := &CompositeIndex{Label: "Doc", Properties: []string{"id"}}
	pair := &CompositeIndex{Label: "Doc", Properties: []string{"tenant", "id"}, fullIndex: map[string][]NodeID{}, prefixIndex: map[string][]NodeID{}}
	props := map[string]interface{}{"tenant": "t", "id": "k"}
	single.ReplaceNode("n", nil, props)
	pair.ReplaceNode("n", nil, props)
	var stop atomic.Bool
	var misses atomic.Int64
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if len(single.LookupFull("k")) != 1 || len(pair.LookupFull("t", "k")) != 1 || len(pair.LookupPrefix("t")) != 1 {
					misses.Add(1)
				}
			}
		}()
	}
	for i := 0; i < 20000; i++ {
		next := map[string]interface{}{"tenant": "t", "id": "k", "c": i}
		single.ReplaceNode("n", props, next)
		pair.ReplaceNode("n", props, next)
		props = next
	}
	stop.Store(true)
	wg.Wait()
	require.Zero(t, misses.Load())
}

func TestReplaceNodeIndexEntriesFollowsLabels(t *testing.T) {
	engine := createTestBadgerEngine(t)
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("doc_id", "Doc", []string{"id"}))
	require.NoError(t, sm.AddPropertyIndex("arc_id", "Archive", []string{"id"}))
	docs, _ := sm.GetCompositeIndex("doc_id")
	archive, _ := sm.GetCompositeIndex("arc_id")

	const id NodeID = "nornic:replace-follows-labels"
	var mu sync.Mutex
	replaced := 0
	restore := SetNodeIndexEntriesReplacedHookForTesting(func(updated NodeID) {
		if updated == id {
			mu.Lock()
			replaced++
			mu.Unlock()
		}
	})
	defer restore()

	doc := &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "k"}}
	engine.maintainCompositeIndexesOnNodeCreated(doc, sm)
	require.Equal(t, []NodeID{id}, docs.LookupFull("k"))

	// Moving the node from Doc to Archive moves its entry between the indexes.
	archived := &Node{ID: id, Labels: []string{"Archive"}, Properties: map[string]interface{}{"id": "k"}}
	engine.maintainCompositeIndexesOnNodeUpdated(archived, doc, sm)
	require.Empty(t, docs.LookupFull("k"))
	require.Equal(t, []NodeID{id}, archive.LookupFull("k"))

	engine.maintainCompositeIndexesOnNodeDeleted(id, archived, sm)
	require.Empty(t, archive.LookupFull("k"))
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 3, replaced)
}
