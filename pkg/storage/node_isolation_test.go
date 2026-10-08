package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAsyncEngineQueuedNodesAreIsolated: the async engine keeps its own copy
// of a queued write and hands readers copies, so neither the writer nor a
// reader changing its node object changes what is queued (#963).
func TestAsyncEngineQueuedNodesAreIsolated(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })

	node := &Node{ID: "test:iso", Labels: []string{"Doc"}, Properties: map[string]any{"text": "queued"}}
	_, err := ae.CreateNode(node)
	require.NoError(t, err)
	node.Properties["text"] = "writer changed it afterwards"

	read, err := ae.GetNode("test:iso")
	require.NoError(t, err)
	require.Equal(t, "queued", read.Properties["text"])
	read.Properties["text"] = "reader changed it"
	read.Labels[0] = "Other"

	for _, readBack := range []func() (*Node, error){
		func() (*Node, error) { return ae.GetNode("test:iso") },
		func() (*Node, error) { return ae.GetFirstNodeByLabel("Doc") },
		func() (*Node, error) {
			nodes, err := ae.GetNodesByLabel("Doc")
			if err != nil || len(nodes) != 1 {
				return nil, err
			}
			nodes[0].Properties["text"] = "list reader changed it"
			return nodes[0], nil
		},
		func() (*Node, error) {
			nodes, err := ae.BatchGetNodes([]NodeID{"test:iso"})
			if err != nil {
				return nil, err
			}
			nodes["test:iso"].Properties["text"] = "batch reader changed it"
			return nodes["test:iso"], nil
		},
	} {
		_, err := readBack()
		require.NoError(t, err)
		again, err := ae.GetNode("test:iso")
		require.NoError(t, err)
		require.Equal(t, "queued", again.Properties["text"])
		require.Equal(t, []string{"Doc"}, again.Labels)
	}

	require.NoError(t, ae.Flush())
	stored, err := engine.GetNode("test:iso")
	require.NoError(t, err)
	require.Equal(t, "queued", stored.Properties["text"])
}

// TestTransactionLabelReadsAreIsolated: a node a transaction hands out from
// its label scan is the caller's own, so changing it before UpdateNode leaves
// the old version the update starts from (and the transaction's snapshot)
// unchanged (#963).
func TestTransactionLabelReadsAreIsolated(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(&Node{ID: "test:scan", Labels: []string{"Doc"}, Properties: map[string]any{"text": "stored"}})
	require.NoError(t, err)

	tx, err := engine.BadgerEngine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for round := 0; round < 2; round++ { // a fresh scan, then the cached one
		var nodes []*Node
		require.NoError(t, tx.StreamNodesByLabelProjected("Doc", nil, func(node *Node) error {
			nodes = append(nodes, node)
			return nil
		}))
		require.Len(t, nodes, 1)
		require.Equal(t, "stored", nodes[0].Properties["text"])
		nodes[0].Properties["text"] = "changed by the caller"
	}
	snapshot, err := tx.GetNode("test:scan")
	require.NoError(t, err)
	require.Equal(t, "stored", snapshot.Properties["text"])
	require.NoError(t, tx.Rollback())
}
