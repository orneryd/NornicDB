package storage

import (
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func headerSummary(edges []*Edge) []string {
	out := make([]string, 0, len(edges))
	for _, edge := range edges {
		out = append(out, string(edge.ID)+":"+edge.Type+":"+string(edge.StartNode)+"->"+string(edge.EndNode))
	}
	sort.Strings(out)
	return out
}

// Adjacency entries carry each relationship's type and other end, written by
// every write path, so headers list without reading the records; an entry
// written before (empty value) is answered from the record.
func TestEdgeHeadersFromAdjacencyEntries(t *testing.T) {
	eng := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "K", Properties: map[string]any{"w": int64(1)}}))
	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "L"}))
	require.NoError(t, tx.Commit())

	out, answered, err := eng.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.True(t, answered)
	require.Equal(t, []string{"test:e1:K:test:a->test:b", "test:e2:L:test:a->test:c"}, headerSummary(out))
	in, answered, err := eng.IncomingEdgeHeaders("test:c")
	require.NoError(t, err)
	require.True(t, answered)
	require.Equal(t, []string{"test:e2:L:test:a->test:c"}, headerSummary(in))

	// A type change and an endpoint change rewrite the entries, in and out of
	// a transaction.
	require.NoError(t, eng.UpdateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "K2"}))
	tx, err = eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "L2"}))
	require.NoError(t, tx.Commit())
	require.NoError(t, eng.UpdateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:c", Type: "K2"}))
	tx, err = eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:e2", StartNode: "test:b", EndNode: "test:c", Type: "L2"}))
	require.NoError(t, tx.Commit())
	out, _, err = eng.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e1:K2:test:a->test:c"}, headerSummary(out))
	out, _, err = eng.OutgoingEdgeHeaders("test:b")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e2:L2:test:b->test:c"}, headerSummary(out))
	require.NoError(t, eng.UpdateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "L2"}))

	// An entry written before the values existed reads the record.
	aNum, _ := eng.idDict.lookupNodeNumID("test:a")
	e2Num, _ := eng.idDict.lookupEdgeNumID("test:e2")
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(outgoingIndexKey(aNum, e2Num), []byte{})
	}))
	eng.edgeCacheMu.Lock()
	clear(eng.edgeCache)
	eng.edgeCacheMu.Unlock()
	out, _, err = eng.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e1:K2:test:a->test:c", "test:e2:L2:test:a->test:c"}, headerSummary(out))

	// Unknown and empty node IDs.
	out, answered, err = eng.OutgoingEdgeHeaders("test:missing")
	require.NoError(t, err)
	require.True(t, answered)
	require.Empty(t, out)
	_, _, err = eng.OutgoingEdgeHeaders("")
	require.ErrorIs(t, err, ErrInvalidID)

	// Decay filtering can hide a relationship, so headers aren't answered.
	eng.SetDecayEnabled(true)
	_, answered, err = eng.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.False(t, answered)
}

// Every layer of the server's stack lists headers the way it lists full
// relationships: the async overlay adds staged relationships and hides
// staged deletes, and the namespaced engine applies its prefix.
func TestEdgeHeadersAcrossStack(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()
	walBacking, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	wal := NewWALEngine(badger, walBacking)
	async := NewAsyncEngine(wal, &AsyncEngineConfig{FlushInterval: time.Hour})
	namespaced := NewNamespacedEngine(async, "ns")
	for _, id := range []NodeID{"a", "b", "c"} {
		_, err := namespaced.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, namespaced.CreateEdge(&Edge{ID: "flushed", StartNode: "a", EndNode: "b", Type: "K"}))
	require.NoError(t, namespaced.CreateEdge(&Edge{ID: "dropped", StartNode: "a", EndNode: "c", Type: "K"}))
	require.NoError(t, async.Flush())
	require.NoError(t, namespaced.DeleteEdge("dropped"))
	require.NoError(t, namespaced.CreateEdge(&Edge{ID: "staged", StartNode: "a", EndNode: "c", Type: "L"}))

	out, answered, err := namespaced.OutgoingEdgeHeaders("a")
	require.NoError(t, err)
	require.True(t, answered)
	require.Equal(t, []string{"flushed:K:a->b", "staged:L:a->c"}, headerSummary(out))
	in, answered, err := namespaced.IncomingEdgeHeaders("c")
	require.NoError(t, err)
	require.True(t, answered)
	require.Equal(t, []string{"staged:L:a->c"}, headerSummary(in))
	walOut, answered, err := wal.OutgoingEdgeHeaders("ns:a")
	require.NoError(t, err)
	require.True(t, answered)
	require.Equal(t, []string{"ns:dropped:K:ns:a->ns:c", "ns:flushed:K:ns:a->ns:b"}, headerSummary(walOut))
	_, answered, err = wal.IncomingEdgeHeaders("ns:b")
	require.NoError(t, err)
	require.True(t, answered)

	// Layers above an engine without headers don't answer.
	plain := nonHeaderEngine{Engine: badger}
	for _, reader := range []EdgeHeaderReader{
		NewNamespacedEngine(plain, "ns"),
		NewWALEngine(plain, walBacking),
	} {
		_, answered, err = reader.OutgoingEdgeHeaders("a")
		require.NoError(t, err)
		require.False(t, answered)
		_, answered, err = reader.IncomingEdgeHeaders("a")
		require.NoError(t, err)
		require.False(t, answered)
	}
	plainAsync := NewAsyncEngine(plain, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer plainAsync.Close()
	_, answered, err = plainAsync.OutgoingEdgeHeaders("ns:a")
	require.NoError(t, err)
	require.False(t, answered)

	// Decay below makes every layer decline.
	badger.SetDecayEnabled(true)
	_, answered, err = namespaced.OutgoingEdgeHeaders("a")
	require.NoError(t, err)
	require.False(t, answered)
}

// nonHeaderEngine hides EdgeHeaderReader from the engine it wraps.
type nonHeaderEngine struct{ Engine }

// Header listing skips entries it can't resolve and answers an entry without
// a value from the cached or stored record.
func TestEdgeHeadersSkipUnresolvableEntries(t *testing.T) {
	eng := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:cached", StartNode: "test:a", EndNode: "test:b", Type: "K"}))
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:gone", StartNode: "test:a", EndNode: "test:b", Type: "K"}))
	aNum, _ := eng.idDict.lookupNodeNumID("test:a")
	cachedNum, _ := eng.idDict.lookupEdgeNumID("test:cached")
	goneNum, _ := eng.idDict.lookupEdgeNumID("test:gone")
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		// No value, record in the edge cache.
		if err := txn.Set(outgoingIndexKey(aNum, cachedNum), []byte{}); err != nil {
			return err
		}
		// No value, record gone.
		if err := txn.Set(outgoingIndexKey(aNum, goneNum), []byte{}); err != nil {
			return err
		}
		if err := txn.Delete(edgeKey("test:gone")); err != nil {
			return err
		}
		// A malformed key, an unknown relationship, an unknown other end.
		if err := txn.Set(append(outgoingIndexPrefix(aNum), 1, 2, 3), []byte{}); err != nil {
			return err
		}
		if err := txn.Set(outgoingIndexKey(aNum, 1<<40), adjacencyEntryValue(aNum, "K")); err != nil {
			return err
		}
		return nil
	}))
	// A relationship with an unreadable record and no value is skipped.
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:corrupt", StartNode: "test:a", EndNode: "test:b", Type: "K"}))
	corruptNum, _ := eng.idDict.lookupEdgeNumID("test:corrupt")
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(edgeKey("test:corrupt"), []byte{0xff}); err != nil {
			return err
		}
		return txn.Set(outgoingIndexKey(aNum, corruptNum), []byte{})
	}))
	// A relationship whose stored other end the dictionary doesn't know.
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:odd", StartNode: "test:a", EndNode: "test:b", Type: "K"}))
	oddNum, _ := eng.idDict.lookupEdgeNumID("test:odd")
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(outgoingIndexKey(aNum, oddNum), adjacencyEntryValue(1<<42, "K"))
	}))
	eng.edgeCacheMu.Lock()
	clear(eng.edgeCache)
	eng.edgeCacheMu.Unlock()
	eng.cacheStoreEdge(&Edge{ID: "test:cached", StartNode: "test:a", EndNode: "test:b", Type: "K"})
	out, answered, err := eng.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.True(t, answered)
	require.Equal(t, []string{"test:cached:K:test:a->test:b"}, headerSummary(out))
}

// The adjacency value of an unknown node is empty, and rewriting the values
// in a transaction that can't write reports the error.
func TestAdjacencyValueHelpers(t *testing.T) {
	eng := newTestEngine(t)
	require.Empty(t, eng.adjacencyValueFor("test:unknown", "K"))
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "K"}))
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		require.Error(t, eng.setAdjacencyValuesInTxn(txn, &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "K2"}))
		require.NoError(t, eng.setAdjacencyValuesInTxn(txn, &Edge{ID: "test:unknown", StartNode: "test:a", EndNode: "test:b", Type: "K2"}))
		return nil
	}))
	// Only the incoming entry is known: its write error is reported.
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		require.Error(t, eng.setAdjacencyValuesInTxn(txn, &Edge{ID: "test:e", StartNode: "test:unknown", EndNode: "test:b", Type: "K2"}))
		return nil
	}))
}

// The async overlay skips cached relationships that a staged delete hides or
// a staged update moved to another start node.
func TestAsyncEdgeHeadersSkipMovedAndDeletedStagedEdges(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()
	async := NewAsyncEngine(badger, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()
	for _, id := range []NodeID{"ns:a", "ns:b", "ns:c"} {
		_, err := async.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, async.CreateEdge(&Edge{ID: "ns:moved", StartNode: "ns:a", EndNode: "ns:c", Type: "K"}))
	require.NoError(t, async.CreateEdge(&Edge{ID: "ns:kept", StartNode: "ns:a", EndNode: "ns:b", Type: "K"}))
	async.mu.Lock()
	async.edgeCache["ns:moved"] = &Edge{ID: "ns:moved", StartNode: "ns:b", EndNode: "ns:c", Type: "K"}
	async.deleteEdges["ns:kept"] = true
	async.mu.Unlock()
	out, answered, err := async.OutgoingEdgeHeaders("ns:a")
	require.NoError(t, err)
	require.True(t, answered)
	require.Empty(t, headerSummary(out))
}
