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
	eng.nodeCacheMu.Lock()
	clear(eng.nodeCache)
	eng.nodeCacheMu.Unlock()
	return eng, created
}

func TestStreamNodesByLabelProjectedReadsRemainingNodesInOnePass(t *testing.T) {
	const count = labelScanPointLookups + 300
	eng, created := labelScanFixture(t, count)

	require.True(t, eng.labelCoversScope("test:", "Common"))
	require.False(t, eng.labelCoversScope("test:", "Rare"))
	require.False(t, eng.labelCoversScope("test:", "Missing"))
	require.True(t, eng.labelCoversScope("", "Common"))
	require.False(t, eng.labelCoversScope("", "Rare"))

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

	nodes = streamLabelNodes(t, eng, "", "Common", nil)
	require.Len(t, nodes, count+1)
	require.Equal(t, NodeID("elsewhere:n0"), nodes[count].ID)

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

func TestStreamNodesByLabelProjectedSkipsMissingRecords(t *testing.T) {
	const count = labelScanPointLookups + 50
	eng, created := labelScanFixture(t, count)
	gone := []NodeID{created[1], created[count-1]}
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		for _, id := range gone {
			if err := txn.Delete(nodeKey(id)); err != nil {
				return err
			}
		}
		return nil
	}))
	for _, properties := range [][]string{nil, {"i"}} {
		nodes := streamLabelNodes(t, eng, "test:", "Common", properties)
		require.Len(t, nodes, count-len(gone))
		for _, node := range nodes {
			require.NotContains(t, gone, node.ID)
		}
		tx, err := eng.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		var txNodes []*Node
		require.NoError(t, tx.StreamNodesByLabelProjected("Common", properties, func(node *Node) error {
			txNodes = append(txNodes, node)
			return nil
		}))
		require.Len(t, txNodes, count-len(gone))
		for _, node := range txNodes {
			require.NotContains(t, gone, node.ID)
		}
		stop := fmt.Errorf("stop")
		visited := 0
		require.ErrorIs(t, tx.StreamNodesByLabelProjected("Common", properties, func(*Node) error {
			visited++
			if visited == labelScanPointLookups+5 {
				return stop
			}
			return nil
		}), stop)
		require.NoError(t, tx.Rollback())
	}
}

func TestStreamNodesByLabelProjectedRejectsCorruptRecords(t *testing.T) {
	for _, phase := range []string{"point-lookups", "one-pass"} {
		t.Run(phase, func(t *testing.T) {
			const count = labelScanPointLookups + 50
			eng, created := labelScanFixture(t, count)
			corrupt := created[0]
			if phase == "one-pass" {
				corrupt = created[count-1]
			}
			require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
				return txn.Set(nodeKey(corrupt), []byte{0xff})
			}))
			for _, properties := range [][]string{nil, {"i"}} {
				name := "full"
				if properties != nil {
					name = "projected"
				}
				t.Run(name, func(t *testing.T) {
					require.Error(t, eng.StreamNodesByLabelProjectedInScope("test:", "Common", properties, func(*Node) error {
						return nil
					}))
					tx, err := eng.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace("test"))
					defer func() { _ = tx.Rollback() }()
					require.Error(t, tx.StreamNodesByLabelProjected("Common", properties, func(*Node) error {
						return nil
					}))
				})
			}
		})
	}
}

func TestTransactionLabelScanReadsRemainingNodesInOnePass(t *testing.T) {
	const count = labelScanPointLookups + 200
	eng, created := labelScanFixture(t, count)
	for _, properties := range [][]string{nil, {"i"}} {
		tx, err := eng.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		require.NotNil(t, tx.snapshotTx)
		var nodes []*Node
		require.NoError(t, tx.StreamNodesByLabelProjected("Common", properties, func(node *Node) error {
			nodes = append(nodes, node)
			return nil
		}))
		require.NoError(t, tx.Rollback())
		require.Len(t, nodes, count)
		for index, node := range nodes {
			require.Equal(t, created[index], node.ID)
			if properties == nil {
				require.Equal(t, "x", node.Properties["big"])
			} else {
				require.NotContains(t, node.Properties, "big")
			}
		}

	}
}

func TestLabelScanReportsCorruptionInVisitOrder(t *testing.T) {
	for _, properties := range [][]string{nil, {"i"}} {
		name := "full"
		if properties != nil {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			const count = labelScanPointLookups + 50
			eng, created := labelScanFixture(t, count)
			// Reverse creation order puts this last label candidate first in
			// the one-pass record walk, before valid pending candidates.
			require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
				return txn.Set(nodeKey(created[count-1]), []byte{0xff})
			}))
			for _, transaction := range []bool{false, true} {
				name := "engine"
				if transaction {
					name = "transaction"
				}
				t.Run(name, func(t *testing.T) {
					scan := func(visit func(*Node) error) error {
						return eng.StreamNodesByLabelProjectedInScope("test:", "Common", properties, visit)
					}
					if transaction {
						tx, err := eng.BeginTransaction()
						require.NoError(t, err)
						require.NoError(t, tx.SetNamespace("test"))
						defer func() { _ = tx.Rollback() }()
						scan = func(visit func(*Node) error) error {
							return tx.StreamNodesByLabelProjected("Common", properties, visit)
						}
					}

					stop := fmt.Errorf("stop")
					visited := 0
					require.ErrorIs(t, scan(func(node *Node) error {
						require.Equal(t, created[visited], node.ID)
						visited++
						if visited == labelScanPointLookups+1 {
							return stop
						}
						return nil
					}), stop)
					require.Equal(t, labelScanPointLookups+1, visited)

					visited = 0
					require.Error(t, scan(func(node *Node) error {
						require.Equal(t, created[visited], node.ID)
						visited++
						return nil
					}))
					require.Equal(t, count-1, visited, "valid preceding label candidates must be visited before corruption is reported")
				})
			}
		})
	}
}

func TestReadNodeRecordsInOnePassStopsOnCallbackError(t *testing.T) {
	eng, ids := labelScanFixture(t, 3)
	stop := fmt.Errorf("stop reading records")
	visited := 0
	require.ErrorIs(t, eng.withView(func(txn *badger.Txn) error {
		return readNodeRecordsInOnePass(txn, "test:", ids, func(NodeID, *badger.Item) error {
			visited++
			return stop
		})
	}), stop)
	require.Equal(t, 1, visited)
}

// BenchmarkLabelRecordReads compares record reads and projected decoding, not
// full queries or the label scan's initial point-lookup prefix.
func BenchmarkLabelRecordReads(b *testing.B) {
	eng, err := NewBadgerEngineInMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = eng.Close() })
	const count = 40000
	const batchSize = 250
	ids := make([]NodeID, count)
	for offset := 0; offset < count; offset += batchSize {
		nodes := make([]*Node, 0, batchSize)
		for index := offset; index < offset+batchSize; index++ {
			ids[index] = NodeID(fmt.Sprintf("test:n%05d", count-index-1))
			nodes = append(nodes, &Node{
				ID:         ids[index],
				Labels:     []string{"L"},
				Properties: map[string]any{"i": int64(index), "big": "x"},
			})
		}
		require.NoError(b, eng.BulkCreateNodes(nodes))
	}
	txn, readTs := eng.db.beginTxn(false)
	b.Cleanup(func() {
		txn.Discard()
		eng.db.endRead(readTs)
	})
	include := propertyProjectionSet([]string{"i"})
	for _, strategy := range []struct {
		name string
		read func(func(NodeID, *badger.Item) error) error
	}{
		{
			name: "point_lookups",
			read: func(read func(NodeID, *badger.Item) error) error {
				for _, id := range ids {
					item, err := txn.Get(nodeKey(id))
					if err != nil {
						return err
					}
					if err := read(id, item); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			name: "one_pass",
			read: func(read func(NodeID, *badger.Item) error) error {
				return readNodeRecordsInOnePass(txn, "test:", ids, read)
			},
		},
	} {
		b.Run(strategy.name, func(b *testing.B) {
			visited := 0
			read := func(id NodeID, item *badger.Item) error {
				return item.Value(func(value []byte) error {
					node, err := eng.decodeNodeProjected(namespaceForNodeID(id), value, include)
					if err != nil {
						return err
					}
					if node == nil {
						return fmt.Errorf("missing decoded node %s", id)
					}
					visited++
					return nil
				})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				visited = 0
				if err := strategy.read(read); err != nil {
					b.Fatal(err)
				}
				if visited != count {
					b.Fatalf("read %d nodes, want %d", visited, count)
				}
			}
		})
	}
}
