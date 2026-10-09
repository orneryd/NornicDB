package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A key name is known in its namespace once a write stores a value under it,
// through every layer of the stack: a buffered async write before it flushes,
// a transaction's write even when rolled back, and an encoded write. Reads,
// null values and other namespaces know nothing (PropertyKeyRegistry, #907).
func TestPropertyKeyRegistryAcrossStack(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()
	walBacking, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	wal := NewWALEngine(badger, walBacking)
	async := NewAsyncEngine(wal, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = async.Close() })
	namespaced := NewNamespacedEngine(async, "ns")
	other := NewNamespacedEngine(async, "other")

	require.False(t, namespaced.PropertyKeyKnown("a"))
	_, err = namespaced.CreateNode(&Node{ID: "n1", Properties: map[string]any{"a": int64(1), "z": nil}})
	require.NoError(t, err)
	// Buffered in the async layer, not yet encoded.
	require.True(t, namespaced.PropertyKeyKnown("a"))
	require.False(t, namespaced.PropertyKeyKnown("z"))
	require.False(t, other.PropertyKeyKnown("a"))
	require.True(t, wal.PropertyKeyKnownInNamespace("ns", "a"))
	require.NoError(t, async.Flush())
	require.True(t, badger.PropertyKeyKnownInNamespace("ns", "a"))

	node, err := namespaced.GetNode("n1")
	require.NoError(t, err)
	node.Properties["b"] = "x"
	require.NoError(t, namespaced.UpdateNode(node))
	_, err = namespaced.CreateNode(&Node{ID: "n2"})
	require.NoError(t, err)
	require.NoError(t, namespaced.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "R", Properties: map[string]any{"c": true}}))
	edge, err := namespaced.GetEdge("e1")
	require.NoError(t, err)
	edge.Properties["d"] = int64(2)
	require.NoError(t, namespaced.UpdateEdge(edge))
	require.NoError(t, namespaced.BulkCreateNodes([]*Node{{ID: "n3", Properties: map[string]any{"e": 1.5}}}))
	require.NoError(t, namespaced.BulkCreateEdges([]*Edge{{ID: "e2", StartNode: "n2", EndNode: "n3", Type: "R", Properties: map[string]any{"f": "y"}}}))
	for _, name := range []string{"b", "c", "d", "e", "f"} {
		require.True(t, namespaced.PropertyKeyKnown(name), name)
	}
	require.NoError(t, async.Flush())

	// A transaction's writes make their names known as they are taken, and a
	// rollback keeps them, as Neo4j's tokens survive it.
	tx, err := badger.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "ns:t1", Properties: map[string]any{"g": int64(1)}})
	require.NoError(t, err)
	committed, err := tx.GetNode("ns:n1")
	require.NoError(t, err)
	committed.Properties["h"] = int64(1)
	require.NoError(t, tx.UpdateNode(committed))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "ns:te1", StartNode: "ns:n1", EndNode: "ns:t1", Type: "R", Properties: map[string]any{"i": int64(1)}}))
	committedEdge, err := tx.GetEdge("ns:e1")
	require.NoError(t, err)
	committedEdge.Properties["j"] = int64(1)
	require.NoError(t, tx.UpdateEdge(committedEdge))
	require.NoError(t, tx.Rollback())
	for _, name := range []string{"g", "h", "i", "j"} {
		require.True(t, namespaced.PropertyKeyKnown(name), name)
		require.False(t, other.PropertyKeyKnown(name), name)
	}
	require.False(t, namespaced.PropertyKeyKnown("never"))
}

// A noted name gets no ID: encoding it later allocates and persists one as
// for any new name.
func TestPropertyKeyDictionaryNoteAllocatesNothing(t *testing.T) {
	dictionary := newPropertyKeyDictionary()
	dictionary.noteProperties("ns", map[string]interface{}{"a": 1, "z": nil})
	dictionary.noteProperties("ns", map[string]interface{}{"a": 2})
	require.False(t, dictionary.known("ns", "z"))
	require.True(t, dictionary.known("ns", "a"))
	require.False(t, dictionary.known("other", "a"))
	_, allocated := dictionary.lookupID("ns", "a")
	require.False(t, allocated)
}
