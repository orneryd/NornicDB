package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func streamLabelNodes(t *testing.T, eng *BadgerEngine, scope, label string, properties []string) []*Node {
	t.Helper()
	var nodes []*Node
	require.NoError(t, eng.StreamNodesByLabelProjectedInScope(scope, label, properties, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	}))
	return nodes
}

// labelScanFixture creates count nodes in reverse ID order, so creation
// (label-index) order differs from node-record order. Every tenth also has
// the Rare label.
func labelScanFixture(t *testing.T, count int) (*BadgerEngine, []NodeID) {
	t.Helper()
	eng := newTestEngine(t)
	created := make([]NodeID, 0, count)
	for i := count - 1; i >= 0; i-- {
		labels := []string{"Common"}
		if i%10 == 0 {
			labels = append(labels, "Rare")
		}
		id := NodeID(fmt.Sprintf("test:n%04d", i))
		_, err := eng.CreateNode(&Node{ID: id, Labels: labels, Properties: map[string]any{"i": int64(i), "big": "x"}})
		require.NoError(t, err)
		created = append(created, id)
	}
	_, err := eng.CreateNode(&Node{ID: "test:other", Labels: []string{"Other"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&Node{ID: "elsewhere:n0", Labels: []string{"Common"}})
	require.NoError(t, err)
	// Scans read the stored records, not nodes cached when they were written.
	eng.nodeCacheMu.Lock()
	clear(eng.nodeCache)
	eng.nodeCacheMu.Unlock()
	return eng, created
}

// A scan past labelScanPointLookups of a label on most nodes reads the rest
// from the node records in one pass, and still visits every node of the
// label in that database in label-index (creation) order with the requested
// properties. A rarer label keeps one lookup per node.
func TestStreamNodesByLabelProjectedReadsRemainingNodesInOnePass(t *testing.T) {
	const count = labelScanPointLookups + 300
	eng, created := labelScanFixture(t, count)

	require.True(t, eng.labelCoversScope("test:", "Common"))
	require.False(t, eng.labelCoversScope("test:", "Rare"))
	require.False(t, eng.labelCoversScope("test:", "Missing"))
	require.True(t, eng.labelCoversScope("", "Common"))
	require.False(t, eng.labelCoversScope("", "Rare"))

	// A node cached after it was written is read from the cache.
	eng.cacheStoreNode(&Node{ID: created[count-2], Labels: []string{"Common"}, Properties: map[string]any{"i": int64(-1)}})

	nodes := streamLabelNodes(t, eng, "test:", "Common", []string{"i"})
	require.Len(t, nodes, count)
	for index, node := range nodes {
		require.Equal(t, created[index], node.ID)
		require.NotContains(t, node.Properties, "big")
	}
	require.Equal(t, int64(-1), nodes[count-2].Properties["i"])

	var rare []NodeID
	for _, id := range created {
		var i int
		_, _ = fmt.Sscanf(string(id), "test:n%04d", &i)
		if i%10 == 0 {
			rare = append(rare, id)
		}
	}
	nodes = streamLabelNodes(t, eng, "test:", "Rare", nil)
	require.Len(t, nodes, len(rare))
	for index, node := range nodes {
		require.Equal(t, rare[index], node.ID)
		require.Equal(t, "x", node.Properties["big"])
	}

	// Every database, whole nodes.
	nodes = streamLabelNodes(t, eng, "", "Common", nil)
	require.Len(t, nodes, count+1)
	require.Equal(t, NodeID("elsewhere:n0"), nodes[count].ID)

	// A visit error stops the scan in either phase.
	stop := fmt.Errorf("stop")
	visited := 0
	require.ErrorIs(t, eng.StreamNodesByLabelProjectedInScope("test:", "Common", nil, func(*Node) error {
		visited++
		if visited == labelScanPointLookups+10 {
			return stop
		}
		return nil
	}), stop)
	require.ErrorIs(t, eng.StreamNodesByLabelProjectedInScope("test:", "Common", nil, func(*Node) error { return stop }), stop)
}

// A node whose label entry a knowledge policy de-indexed stays hidden in
// both phases of the scan.
func TestStreamNodesByLabelProjectedHidesDeindexedNodes(t *testing.T) {
	const count = labelScanPointLookups + 50
	eng, created := labelScanFixture(t, count)
	eng.SetDecayEnabled(true)
	hidden := []NodeID{created[0], created[count-1]}
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		for _, id := range hidden {
			num, ok := eng.idDict.lookupNodeNumID(id)
			require.True(t, ok)
			if err := putIndexTombstoneInTxn(txn, labelIndexKey("Common", num)); err != nil {
				return err
			}
		}
		return nil
	}))
	nodes := streamLabelNodes(t, eng, "test:", "Common", nil)
	require.Len(t, nodes, count-len(hidden))
	for _, node := range nodes {
		require.NotContains(t, hidden, node.ID)
	}
}

// A label entry whose node record is unreadable or gone is skipped in both
// phases of the scan.
func TestStreamNodesByLabelProjectedSkipsUnreadableRecords(t *testing.T) {
	const count = labelScanPointLookups + 50
	eng, created := labelScanFixture(t, count)
	unreadable, gone := created[0], created[count-1]
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(nodeKey(unreadable), []byte{0xff}); err != nil {
			return err
		}
		return txn.Delete(nodeKey(gone))
	}))
	for _, properties := range [][]string{nil, {"i"}} {
		nodes := streamLabelNodes(t, eng, "test:", "Common", properties)
		require.Len(t, nodes, count-2)
		for _, node := range nodes {
			require.NotEqual(t, unreadable, node.ID)
			require.NotEqual(t, gone, node.ID)
		}
	}
}
