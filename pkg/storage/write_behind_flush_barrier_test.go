package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// FlushWriteBehind is a barrier: when it returns, every commit acknowledged
// before it is durable in Badger and published (its property index entry is
// in place), and its listener notification is readied in commit order -
// whether the call or the background flusher applied the generation. The
// flusher runs every millisecond here, so it claims many of them.
func TestFlushWriteBehindIsAPublicationBarrier(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Millisecond)
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddPropertyIndex("flush_l_v", "L", []string{"v"}))
	sm.MarkPropertyIndexesFilled()
	const rounds = 200
	created := make(chan NodeID, rounds)
	engine.OnNodeCreated(func(node *Node) { created <- node.ID })

	for i := 0; i < rounds; i++ {
		id := NodeID(fmt.Sprintf("test:n%d", i))
		bufferedCreate(t, engine, &Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": int64(i)}})
		require.NoError(t, engine.FlushWriteBehind())
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(nodeKey(id))
			return err
		}), "commit %d durable in Badger when FlushWriteBehind returns", i)
		require.Equal(t, []NodeID{id}, sm.PropertyIndexLookup("L", "v", int64(i)), "commit %d published when FlushWriteBehind returns", i)
	}
	for i := 0; i < rounds; i++ {
		select {
		case id := <-created:
			require.Equal(t, NodeID(fmt.Sprintf("test:n%d", i)), id, "notifications in commit order")
		case <-time.After(10 * time.Second):
			t.Fatalf("notification %d never dispatched", i)
		}
	}
}

// A listener may flush from its callback, as a BEGIN or a snapshot open in
// it does: FlushWriteBehind from inside a dispatched notification returns,
// also while another buffered commit is pending, since it doesn't wait for
// listeners to complete.
func TestFlushWriteBehindFromAListenerReturns(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	flushed := make(chan error, 1)
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "test:first" {
			return
		}
		tx, err := engine.BeginTransaction()
		if err == nil {
			err = tx.SetImplicit(true)
		}
		if err == nil {
			err = tx.SetNamespace("test")
		}
		if err == nil {
			_, err = tx.CreateNode(&Node{ID: "test:second", Labels: []string{"L"}})
		}
		if err == nil {
			err = tx.Commit()
		}
		if err == nil {
			err = engine.FlushWriteBehind()
		}
		flushed <- err
	})
	bufferedCreate(t, engine, &Node{ID: "test:first", Labels: []string{"L"}})
	require.NoError(t, engine.FlushWriteBehind())
	select {
	case err := <-flushed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("a FlushWriteBehind from a listener didn't return")
	}
	require.NoError(t, engine.FlushWriteBehind())
	_, err := engine.GetNode("test:second")
	require.NoError(t, err)
}
