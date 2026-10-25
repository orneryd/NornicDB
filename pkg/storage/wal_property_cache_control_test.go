package storage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Base-compatible control: this old-format buffered record has a complete
// source drain, whose IDs conflict with names already present in the destination.
// No snapshot or semantic catalog decoder participates in this control.
func TestBufferedWALConflictingDrainCacheControl(t *testing.T) {
	open := func() *BadgerEngine {
		b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, b.Close()) })
		return b
	}
	source, destination := open(), open()
	seed := func(b *BadgerEngine, name string) {
		_, err := b.CreateNode(&Node{ID: NodeID("binding:seed-" + name), Properties: map[string]any{name: "keep-" + name}})
		require.NoError(t, err)
		require.NoError(t, b.FlushWriteBehind())
	}
	seed(source, "alpha")
	seed(source, "beta")
	seed(destination, "beta")
	seed(destination, "alpha")
	assertInverse := func(b *BadgerEngine) {
		d := b.propKeyDict
		d.mu.RLock()
		defer d.mu.RUnlock()
		for ns, forward := range d.forward {
			for name, id := range forward {
				require.Equal(t, name, d.reverse[ns][id], "forward/reverse namespace=%s id=%d", ns, id)
			}
		}
		for ns, reverse := range d.reverse {
			for id, name := range reverse {
				require.Equal(t, id, d.forward[ns][name], "reverse/forward namespace=%s name=%s", ns, name)
			}
		}
	}
	assertInverse(source)
	assertInverse(destination)
	require.Equal(t, uint64(1), source.propKeyDict.forward["binding"]["alpha"])
	require.Equal(t, uint64(1), destination.propKeyDict.forward["binding"]["beta"])
	node := &Node{ID: "binding:imported", Properties: map[string]any{"alpha": "after-alpha", "beta": "after-beta"}}
	_, err := source.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, source.FlushWriteBehind())
	var body []byte
	require.NoError(t, source.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(nodeKey(node.ID))
		if err == nil {
			body, err = item.ValueCopy(nil)
		}
		return err
	}))
	payload, err := json.Marshal(WALBufferedCommitData{
		TxID: "conflicting-source-drain", Namespace: "binding",
		Ops:    []Operation{{Type: OpCreateNode, Node: node}},
		Writes: map[string][]byte{string(nodeKey(node.ID)): body},
		PropKeys: []WALPropKeyEntry{
			{Namespace: "binding", Name: "alpha", ID: 1},
			{Namespace: "binding", Name: "beta", ID: 2},
		},
		PropKeyCounters: map[string]uint64{"binding": 2},
	})
	require.NoError(t, err)
	require.NoError(t, ReplayWALEntry(destination, WALEntry{Operation: OpBufferedCommit, Data: payload}))
	assertInverse(destination)
	got, err := destination.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, node.Properties, got.Properties)
	for _, name := range []string{"alpha", "beta"} {
		got, err := destination.GetNode(NodeID("binding:seed-" + name))
		require.NoError(t, err)
		require.Equal(t, map[string]any{name: "keep-" + name}, got.Properties)
	}
}
