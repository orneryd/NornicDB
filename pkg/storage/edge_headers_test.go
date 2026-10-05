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
	out, _, err = eng.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e1:K2:test:a->test:c", "test:e2:L2:test:a->test:c"}, headerSummary(out))

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
