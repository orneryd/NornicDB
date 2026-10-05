package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A transaction lists relationship headers as GetOutgoingEdges /
// GetIncomingEdges list relationships at its snapshot: adjacency values give
// the type and other end, a peer's later change doesn't show, the
// transaction's own writes and deletes are merged, and an entry without a
// value or a head newer than the read version reads the record.
func TestTransactionEdgeHeaders(t *testing.T) {
	eng := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d"} {
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	w := map[string]any{"w": int64(1)}
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "K", Properties: w}))
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "K", Properties: w}))
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:e3", StartNode: "test:a", EndNode: "test:d", Type: "K", Properties: w}))
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:e4", StartNode: "test:b", EndNode: "test:c", Type: "K", Properties: w}))

	// e3's entry predates the values.
	aNum, _ := eng.idDict.lookupNodeNumID("test:a")
	e3Num, _ := eng.idDict.lookupEdgeNumID("test:e3")
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(outgoingIndexKey(aNum, e3Num), []byte{})
	}))

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("test"))

	// A peer retypes e1 after the snapshot.
	require.NoError(t, eng.UpdateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "K2", Properties: w}))

	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e5", StartNode: "test:a", EndNode: "test:b", Type: "L"}))
	require.NoError(t, tx.DeleteEdge("test:e2"))

	same := func(nodeID NodeID) {
		t.Helper()
		headers, answered, err := tx.OutgoingEdgeHeaders(nodeID)
		require.NoError(t, err)
		require.True(t, answered)
		full, err := tx.GetOutgoingEdges(nodeID)
		require.NoError(t, err)
		require.Equal(t, headerSummary(full), headerSummary(headers), nodeID)
		headers, answered, err = tx.IncomingEdgeHeaders(nodeID)
		require.NoError(t, err)
		require.True(t, answered)
		full, err = tx.GetIncomingEdges(nodeID)
		require.NoError(t, err)
		require.Equal(t, headerSummary(full), headerSummary(headers), nodeID)
	}
	headers, _, err := tx.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e1:K:test:a->test:b", "test:e3:K:test:a->test:d", "test:e5:L:test:a->test:b"}, headerSummary(headers))
	for _, header := range headers {
		if header.ID == "test:e1" {
			require.Nil(t, header.Properties, "e1 comes from its adjacency value")
		}
	}
	// The second listing uses the cached adjacency and, after GetOutgoingEdges,
	// the cached records.
	same("test:a")
	same("test:a")
	same("test:b")
	same("test:c")

	// A read version older than a relationship's head reads its record.
	e4Head := MVCCHead{}
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		var err error
		e4Head, err = eng.loadEdgeMVCCHeadInTxn(txn, "test:e4")
		return err
	}))
	tx.snapshotEdgeByID = nil
	tx.snapshotOutgoingAdjacency.clear()
	readTS := tx.readTS
	tx.readTS = e4Head.Version
	require.NoError(t, eng.UpdateEdge(&Edge{ID: "test:e4", StartNode: "test:b", EndNode: "test:c", Type: "K", Properties: map[string]any{"w": int64(2)}}))
	tx2, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx2.Rollback() }()
	require.NoError(t, tx2.SetNamespace("test"))
	tx2.readTS = e4Head.Version
	headers, answered, err := tx2.OutgoingEdgeHeaders("test:b")
	require.NoError(t, err)
	require.True(t, answered)
	require.Len(t, headers, 1)
	require.Equal(t, int64(1), headers[0].Properties["w"], "the record at the read version")
	tx.readTS = readTS

	// Decay filtering and a missing read version decline; a finished
	// transaction errors.
	eng.SetDecayEnabled(true)
	_, answered, err = tx.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.False(t, answered)
	eng.SetDecayEnabled(false)
	tx.readTS = MVCCVersion{}
	_, answered, err = tx.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.False(t, answered)
	tx.readTS = readTS
	_, _, err = tx.OutgoingEdgeHeaders("other:a")
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
	_, _, err = tx.IncomingEdgeHeaders("test:a")
	require.Error(t, err)
}

// A transaction's header listing reads the record for an entry it can't
// resolve, lists nothing for an unknown node, and returns the error of a
// record it can't read, as GetOutgoingEdges does.
func TestTransactionEdgeHeadersUnresolvableEntries(t *testing.T) {
	eng := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:odd", StartNode: "test:a", EndNode: "test:b", Type: "K"}))
	aNum, _ := eng.idDict.lookupNodeNumID("test:a")
	bNum, _ := eng.idDict.lookupNodeNumID("test:b")
	oddNum, _ := eng.idDict.lookupEdgeNumID("test:odd")
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		// A malformed key, an unknown relationship, an unknown other end.
		if err := txn.Set(append(outgoingIndexPrefix(aNum), 1, 2, 3), []byte{}); err != nil {
			return err
		}
		if err := txn.Set(outgoingIndexKey(aNum, 1<<40), encodeEdgeCompactHeader(edgeFormatCompactV2, &Edge{Type: "K"}, aNum, bNum, 0)); err != nil {
			return err
		}
		return txn.Set(outgoingIndexKey(aNum, oddNum), encodeEdgeCompactHeader(edgeFormatCompactV2, &Edge{Type: "X"}, 1<<42, bNum, 0))
	}))

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	headers, answered, err := tx.OutgoingEdgeHeaders("test:a")
	require.NoError(t, err)
	require.True(t, answered)
	require.Equal(t, []string{"test:odd:K:test:a->test:b"}, headerSummary(headers))
	headers, answered, err = tx.OutgoingEdgeHeaders("test:missing")
	require.NoError(t, err)
	require.True(t, answered)
	require.Empty(t, headers)
	require.NoError(t, tx.Rollback())

	require.NoError(t, eng.CreateEdge(&Edge{ID: "test:corrupt", StartNode: "test:a", EndNode: "test:b", Type: "K"}))
	corruptNum, _ := eng.idDict.lookupEdgeNumID("test:corrupt")
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(edgeKey("test:corrupt"), []byte{0xff}); err != nil {
			return err
		}
		return txn.Set(outgoingIndexKey(aNum, corruptNum), []byte{})
	}))
	eng.edgeCacheMu.Lock()
	clear(eng.edgeCache)
	eng.edgeCacheMu.Unlock()
	tx, err = eng.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("test"))
	_, fullErr := tx.GetOutgoingEdges("test:a")
	require.Error(t, fullErr)
	_, _, err = tx.OutgoingEdgeHeaders("test:a")
	require.Error(t, err)
}
