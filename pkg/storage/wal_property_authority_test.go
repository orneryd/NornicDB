package storage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestWALPropertyAuthorityRejectsInvalidRecords(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	b, w, c := bindingCostFixture(t, 1)
	uninitialized := newPropertyKeyDictionary()
	require.ErrorContains(t, w.AppendBufferedCommit(c, uninitialized), "identity is uninitialized")
	entries, err := ReadWALEntriesFromDir(w.Config().Dir)
	require.NoError(t, err)
	require.Empty(t, entries, "invalid export must not append an ambiguous record")
	for _, payload := range [][]byte{
		[]byte(`{`),
		[]byte(`{"_nornicdb_encoding":"unknown"}`),
		[]byte(`{"_nornicdb_encoding":"storage-msgpack-v1"}`),
		[]byte(`{"_nornicdb_encoding":"storage-msgpack-v1","_nornicdb_data":"wQ=="}`),
	} {
		require.Error(t, ReplayWALEntry(b, WALEntry{Operation: OpBufferedCommit, Data: payload}))
	}
	for _, bindings := range [][]byte{{0}, {1, 0x80}, appendWALPropertyBinding(appendWALPropertyBinding([]byte{1}, "perf", 1, "first"), "perf", 1, "second")} {
		payload, err := json.Marshal(WALBufferedCommitData{Writes: c.writes, PropertyBindings: bindings})
		require.NoError(t, err)
		require.Error(t, ReplayWALEntry(b, WALEntry{Operation: OpBufferedCommit, Data: payload}))
	}
	assertPropertyDictionaryBijection(t, b)
	node, err := b.GetNode("perf:a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"property_000": int64(0)}, node.Properties)
}

func TestWALPropertyAuthorityRejectsCorruptIdentity(t *testing.T) {
	dir := t.TempDir()
	b := reviewBindingEngine(t, dir)
	require.NoError(t, b.db.Update(func(tx *badger.Txn) error {
		return tx.Set(walPropertyIdentityKey, []byte("invalid-identity"))
	}))
	require.NoError(t, b.Close())
	_, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.ErrorContains(t, err, "invalid property dictionary identity")
}

func TestWALPropertyOwnLegacyStagedDrain(t *testing.T) {
	b, data := r2BindingData(t, 1)
	data.PropKeys = legacyBindingEntries(t, data.PropertyBindings)
	data.PropertyBindings = nil
	data.PropertyDictionaryID = b.walPropertySource.identity
	c, err := b.importBufferedWALRecord(&data, walOwnDirectoryAtOpen)
	require.NoError(t, err)
	require.Len(t, c.propKeyDrain.pending, 1)
	require.Equal(t, data.PropKeys[0].ID, c.propKeyDrain.pending[0].id)
	require.NoError(t, b.applyBufferedCommits([]bufferedCommit{c}))
	assertPropertyDictionaryBijection(t, b)
}

func TestWALPropertyLosslessLegacyEdge(t *testing.T) {
	source := reviewBindingEngine(t, t.TempDir())
	destination := reviewBindingEngine(t, t.TempDir())
	for _, b := range []*BadgerEngine{source, destination} {
		for _, id := range []NodeID{"named:a", "named:b"} {
			_, err := b.CreateNode(&Node{ID: id})
			require.NoError(t, err)
		}
	}
	_, err := destination.CreateNode(&Node{ID: "named:sentinel", Properties: map[string]any{"other": "unchanged"}})
	require.NoError(t, err)
	edge := &Edge{ID: "named:r", StartNode: "named:a", EndNode: "named:b", Type: "R", Properties: map[string]any{"stamp": time.Date(2026, 10, 23, 1, 2, 3, 4000, time.UTC)}}
	require.NoError(t, source.CreateEdge(edge))
	var body []byte
	require.NoError(t, source.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(edgeKey(edge.ID))
		if err == nil {
			body, err = item.ValueCopy(nil)
		}
		return err
	}))
	data := WALBufferedCommitData{TxID: "named-edge", Namespace: "named", Writes: map[string][]byte{string(edgeKey(edge.ID)): body}, Ops: []Operation{{Type: OpCreateEdge, Edge: edge}}}
	data.NumericBindings, err = captureWALNumericBindings(source.idDict, data.Writes, nil, nil)
	require.NoError(t, err)
	transformRecoveryEntities(&data, prepareRecoveryNode, prepareRecoveryEdge)
	typed, err := encodeValue(&data)
	require.NoError(t, err)
	payload, err := json.Marshal(walRecoverySidecar{Encoding: walRecoveryEncoding, Data: typed})
	require.NoError(t, err)
	require.NoError(t, ReplayWALEntry(destination, WALEntry{Operation: OpBufferedCommit, Data: payload}))
	got, err := destination.GetEdge(edge.ID)
	require.NoError(t, err)
	stamp, ok := got.Properties["stamp"].(time.Time)
	require.True(t, ok)
	require.True(t, edge.Properties["stamp"].(time.Time).Equal(stamp))
	assertPropertyDictionaryBijection(t, destination)
	// An incomplete named image must fail instead of borrowing a prior record.
	data.TxID = "incomplete-edge"
	data.Ops[0].Edge.Properties = nil
	typed, err = encodeValue(&data)
	require.NoError(t, err)
	payload, err = json.Marshal(walRecoverySidecar{Encoding: walRecoveryEncoding, Data: typed})
	require.NoError(t, err)
	require.ErrorContains(t, ReplayWALEntry(destination, WALEntry{Operation: OpBufferedCommit, Data: payload}), "after-image is incomplete")
	assertPropertyDictionaryBijection(t, destination)
}
