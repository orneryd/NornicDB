package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var factKey = Constraint{Name: "fact_key", Type: ConstraintNodeKey, Label: "Fact", Properties: []string{"record_id", "attribute", "version"}}

func factNode(id string, record int64, attribute string, version int64) *Node {
	return &Node{ID: NodeID(prefixTestID(id)), Labels: []string{"Fact"}, Properties: map[string]interface{}{"record_id": record, "attribute": attribute, "version": version}}
}

func countNodeKeyScans(t *testing.T) *atomic.Int64 {
	t.Helper()
	var scans atomic.Int64
	hook := func() { scans.Add(1) }
	previous := nodeKeyScanHook.Swap(&hook)
	t.Cleanup(func() { nodeKeyScanHook.Store(previous) })
	return &scans
}

// A multi-property NODE KEY owns an arity-N equality index; once filled,
// transactional validation looks the key up instead of scanning the label
// and still finds every collision the scan found.
func TestNodeKeyIndex_ValidationUsesFilledIndex(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factKey))

	idx, owned := sm.ConstraintPropertyIndex(factKey.Name)
	require.True(t, owned, "a multi-property NODE KEY owns an equality index")
	require.Equal(t, factKey.Properties, idx.Properties)
	require.Nil(t, sm.filledKeyIndex("Fact", factKey.Properties), "the owned index starts unfilled")
	require.NoError(t, sm.BackfillCompositeIndex(idx.Name, nil))
	require.NotNil(t, sm.filledKeyIndex("Fact", factKey.Properties))

	var nodes []*Node
	for r := 0; r < 40; r++ {
		nodes = append(nodes, factNode(fmt.Sprintf("f-%d-1", r), int64(r), "name", 1), factNode(fmt.Sprintf("f-%d-2", r), int64(r), "name", 2))
	}
	commitNodes(t, engine, nodes...)
	scans := countNodeKeyScans(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(factNode("dup", 5, "name", 2))
	require.Error(t, err, "the full key of a committed node is taken")
	_, err = tx.CreateNode(factNode("dup-numeric", 6, "name", 1).withProp("version", float64(1)))
	require.Error(t, err, "a numerically equal key value of another type is the same key")
	_, err = tx.CreateNode(factNode("other-version", 5, "name", 3))
	require.NoError(t, err)
	_, err = tx.CreateNode(factNode("other-attribute", 5, "status", 1))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// A committed node the transaction rewrites or deletes frees its key.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: factNode("f-7-1", 7, "name", 99), Policy: ClientNodeEdits}))
	_, err = tx.CreateNode(factNode("reuse-7", 7, "name", 1))
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode(NodeID(prefixTestID("f-8-1")), CascadeEdges))
	_, err = tx.CreateNode(factNode("reuse-8", 8, "name", 1))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	require.Zero(t, scans.Load(), "validation with a filled key index must not scan the label")
}

// While the owned index is unfilled (a constraint over data written before
// it, until a backfill or rebuild), validation keeps the label scan.
func TestNodeKeyIndex_UnfilledIndexScans(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	commitNodes(t, engine, factNode("pre", 1, "name", 1))
	require.NoError(t, sm.AddConstraint(factKey))
	scans := countNodeKeyScans(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(factNode("dup", 1, "name", 1))
	require.Error(t, err, "a collision with a node written before the constraint")
	require.NoError(t, tx.Rollback())
	require.NotZero(t, scans.Load())
}

func TestNodeKeyIndex_DropRemovesOwnedIndex(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factKey))
	_, owned := sm.ConstraintPropertyIndex(factKey.Name)
	require.True(t, owned)
	require.NoError(t, sm.DropConstraint(factKey.Name))
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	require.Nil(t, sm.compositeKeyIndexLocked("Fact", factKey.Properties), "dropping the constraint drops its index")
}

func (n *Node) withProp(k string, v interface{}) *Node {
	n.Properties[k] = v
	return n
}

// The owned key index is in memory only: reopening a database rebuilds and
// fills it, so validation keeps using it after a restart.
func TestNodeKeyIndex_FilledAfterReopen(t *testing.T) {
	engine, dir := createTestBadgerEngineOnDisk(t)
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factKey))
	commitNodes(t, engine, factNode("a", 1, "name", 1))
	require.NoError(t, engine.Close())

	reopened, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	defer reopened.Close()
	sm = reopened.GetSchemaForNamespace("test")
	require.NotNil(t, sm.filledKeyIndex("Fact", factKey.Properties), "the owned key index is filled on reopen")
	scans := countNodeKeyScans(t)
	tx, err := reopened.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(factNode("dup", 1, "name", 1))
	require.Error(t, err, "a key stored before the restart is still taken")
	require.NoError(t, tx.Rollback())
	require.Zero(t, scans.Load())
}
