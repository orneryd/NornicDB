package storage

// Label counts through the AsyncEngine come from the inner engine's per-label
// counters plus the cached writes, without reading the label's nodes (#843).

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// labelReadCountingEngine counts label reads on the wrapped engine; GetNode
// can be made to fail.
type labelReadCountingEngine struct {
	*BadgerEngine
	labelReads int
	getNodeErr error
}

func (e *labelReadCountingEngine) GetNodesByLabel(label string) ([]*Node, error) {
	e.labelReads++
	return e.BadgerEngine.GetNodesByLabel(label)
}

func (e *labelReadCountingEngine) GetNode(id NodeID) (*Node, error) {
	if e.getNodeErr != nil {
		return nil, e.getNodeErr
	}
	return e.BadgerEngine.GetNode(id)
}

func TestAsyncEngineLabelCountsUseStoredCountsAndCachedWrites(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	inner := &labelReadCountingEngine{BadgerEngine: badger}
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })

	node := func(id string, labels ...string) *Node {
		return &Node{ID: NodeID(id), Labels: labels, Properties: map[string]interface{}{"id": id}}
	}
	for i := 0; i < 5; i++ {
		_, err := ae.CreateNode(node(fmt.Sprintf("a:p%d", i), "Person"))
		require.NoError(t, err)
	}
	_, err = ae.CreateNode(node("a:x", "Other"))
	require.NoError(t, err)
	_, err = ae.CreateNode(node("b:p", "Person"))
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	check := func(step string, all, inA int64) {
		t.Helper()
		inner.labelReads = 0
		count, err := ae.NodeCountByLabel("Person")
		require.NoError(t, err)
		require.Equal(t, all, count, step)
		count, err = ae.NodeCountByLabelInNamespace("a", "Person")
		require.NoError(t, err)
		require.Equal(t, inA, count, step)
		require.Zero(t, inner.labelReads, "%s: a label count must not read the label's nodes", step)
		nodes, err := ae.GetNodesByLabel("Person")
		require.NoError(t, err)
		require.Len(t, nodes, int(all), step)
	}
	check("stored", 6, 5)

	// Cached writes, not yet flushed.
	_, err = ae.CreateNode(node("a:new", "Person"))
	require.NoError(t, err)
	check("cached create", 7, 6)
	require.NoError(t, ae.UpdateNode(node("a:x", "Other", "Person")))
	check("cached update adds the label", 8, 7)
	require.NoError(t, ae.UpdateNode(node("a:p0", "Other")))
	check("cached update removes the label", 7, 6)
	require.NoError(t, ae.UpdateNode(node("a:p1", "Person")))
	check("cached update keeps the label", 7, 6)
	require.NoError(t, ae.DeleteNode("a:p2"))
	check("cached delete of a stored node", 6, 5)
	_, err = ae.CreateNode(node("a:temp", "Person"))
	require.NoError(t, err)
	require.NoError(t, ae.DeleteNode("a:temp"))
	check("created and deleted in the cache", 6, 5)
	require.NoError(t, ae.DeleteNode("b:p"))
	check("cached delete in another namespace", 5, 5)

	require.NoError(t, ae.Flush())
	check("flushed", 5, 5)
}

func TestAsyncEngineLabelCountReportsStoreErrors(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	inner := &labelReadCountingEngine{BadgerEngine: badger}
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })
	_, err = ae.CreateNode(&Node{ID: "a:p", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, ae.Flush())
	_, err = ae.CreateNode(&Node{ID: "a:q", Labels: []string{"Person"}})
	require.NoError(t, err)

	failure := errors.New("read failed")
	inner.getNodeErr = failure
	_, err = ae.NodeCountByLabel("Person")
	require.ErrorIs(t, err, failure)

	inner.getNodeErr = nil
	require.NoError(t, ae.Flush())
	require.NoError(t, ae.DeleteNode("a:p"))
	inner.getNodeErr = failure
	_, err = ae.NodeCountByLabelInNamespace("a", "Person")
	require.ErrorIs(t, err, failure)
}

// staleLabelCountEngine reports a stored label count below the stored nodes.
type staleLabelCountEngine struct {
	*BadgerEngine
}

func (e *staleLabelCountEngine) NodeCountByLabel(label string) (int64, error) {
	return 0, nil
}

func TestAsyncEngineLabelCountClampsBelowZero(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	ae := NewAsyncEngine(&staleLabelCountEngine{BadgerEngine: badger}, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })
	_, err = ae.CreateNode(&Node{ID: "a:p", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, ae.Flush())
	require.NoError(t, ae.DeleteNode("a:p"))

	count, err := ae.NodeCountByLabel("Person")
	require.NoError(t, err)
	require.Zero(t, count)
}
