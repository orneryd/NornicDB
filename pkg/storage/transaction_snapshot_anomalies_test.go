package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestTransaction_WriteSkew_IsAllowedUnderSnapshotIsolation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	doctor1 := NodeID(prefixTestID("doctor-1"))
	doctor2 := NodeID(prefixTestID("doctor-2"))
	_, err := engine.CreateNode(&Node{ID: doctor1, Labels: []string{"Doctor"}, Properties: map[string]interface{}{"on_call": true}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: doctor2, Labels: []string{"Doctor"}, Properties: map[string]interface{}{"on_call": true}})
	require.NoError(t, err)

	tx1, err := engine.BeginTransaction()
	require.NoError(t, err)
	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)
	tx3, err := engine.BeginTransaction()
	require.NoError(t, err)

	count1 := countNodesWithBoolProperty(t, tx1, "Doctor", "on_call")
	count2 := countNodesWithBoolProperty(t, tx2, "Doctor", "on_call")
	count3 := countNodesWithBoolProperty(t, tx3, "Doctor", "on_call")
	require.Equal(t, 2, count1)
	require.Equal(t, 2, count2)
	require.Equal(t, 2, count3)

	require.NoError(t, tx1.UpdateNode(&Node{ID: doctor1, Labels: []string{"Doctor"}, Properties: map[string]interface{}{"on_call": false}}))
	require.NoError(t, tx2.UpdateNode(&Node{ID: doctor2, Labels: []string{"Doctor"}, Properties: map[string]interface{}{"on_call": false}}))
	require.NoError(t, tx3.UpdateNode(&Node{ID: doctor1, Labels: []string{"Doctor"}, Properties: map[string]interface{}{"on_call": false}}))

	require.NoError(t, tx1.Commit())
	require.NoError(t, tx2.Commit())
	require.ErrorIs(t, tx3.Commit(), ErrConflict)

	verifyTx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer verifyTx.Rollback()
	require.Equal(t, 0, countNodesWithBoolProperty(t, verifyTx, "Doctor", "on_call"))
}

func TestNormalizeTransactionCommitError_MapsBadgerConflict(t *testing.T) {
	err := normalizeTransactionCommitError(badger.ErrConflict)
	require.ErrorIs(t, err, ErrConflict)
	require.ErrorIs(t, err, badger.ErrConflict)
	require.True(t, strings.Contains(err.Error(), "concurrent transaction modified data before commit"))
}

func TestTransaction_ReadYourWritesDoesNotBreakAnchoredSnapshot(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	nodeID := NodeID(prefixTestID("anchored-read-node"))

	tx1, err := engine.BeginTransaction()
	require.NoError(t, err)
	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)

	_, err = tx1.CreateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"User"},
		Properties: map[string]interface{}{"name": "anchored"},
	})
	require.NoError(t, err)

	created, err := tx1.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, nodeID, created.ID)

	_, err = tx2.GetNode(nodeID)
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, tx1.Commit())

	_, err = tx2.GetNode(nodeID)
	require.ErrorIs(t, err, ErrNotFound)

	tx3, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx3.Rollback()

	committed, err := tx3.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, nodeID, committed.ID)

	require.NoError(t, tx2.Rollback())
}

func TestTransaction_LabelScanRepeatabilityWithinSnapshot(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	for i := 0; i < 3; i++ {
		_, err := engine.CreateNode(&Node{
			ID:         NodeID(prefixTestID(fmt.Sprintf("user-%d", i))),
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"index": i},
		})
		require.NoError(t, err)
	}

	txScan, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txScan.Rollback()

	before, err := txScan.getNodesByLabelLocked("User")
	require.NoError(t, err)
	beforeIDs := sortedNodeIDs(before)

	txInsert, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = txInsert.CreateNode(&Node{
		ID:         NodeID(prefixTestID("user-new")),
		Labels:     []string{"User"},
		Properties: map[string]interface{}{"index": 99},
	})
	require.NoError(t, err)
	require.NoError(t, txInsert.Commit())

	after, err := txScan.getNodesByLabelLocked("User")
	require.NoError(t, err)
	afterIDs := sortedNodeIDs(after)
	require.Equal(t, beforeIDs, afterIDs)

	txFresh, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txFresh.Rollback()
	fresh, err := txFresh.getNodesByLabelLocked("User")
	require.NoError(t, err)
	require.Len(t, fresh, 4)
}

func TestTransaction_AllNodesRepeatabilityWithinSnapshot(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	for i := 0; i < 3; i++ {
		_, err := engine.CreateNode(&Node{
			ID:         NodeID(prefixTestID(fmt.Sprintf("all-node-%d", i))),
			Labels:     []string{"Node"},
			Properties: map[string]interface{}{"index": i},
		})
		require.NoError(t, err)
	}

	txScan, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txScan.Rollback()

	before, err := txScan.AllNodes()
	require.NoError(t, err)
	beforeIDs := sortedNodeIDs(before)

	txInsert, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = txInsert.CreateNode(&Node{
		ID:         NodeID(prefixTestID("all-node-new")),
		Labels:     []string{"Node"},
		Properties: map[string]interface{}{"index": 99},
	})
	require.NoError(t, err)
	require.NoError(t, txInsert.Commit())

	after, err := txScan.AllNodes()
	require.NoError(t, err)
	afterIDs := sortedNodeIDs(after)
	require.Equal(t, beforeIDs, afterIDs)

	txFresh, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txFresh.Rollback()
	fresh, err := txFresh.AllNodes()
	require.NoError(t, err)
	require.Len(t, fresh, 4)
}

func TestTransaction_CreateEdgeConflictsWithConcurrentNodeDelete(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	start := NodeID(prefixTestID("edge-after-delete-start"))
	target := NodeID(prefixTestID("edge-after-delete-target"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: target, Labels: []string{"Node"}})
	require.NoError(t, err)

	txCreateEdge, err := engine.BeginTransaction()
	require.NoError(t, err)
	txDelete, err := engine.BeginTransaction()
	require.NoError(t, err)

	require.NoError(t, txCreateEdge.CreateEdge(&Edge{
		ID:        EdgeID(prefixTestID("edge-created-before-delete-commit")),
		StartNode: start,
		EndNode:   target,
		Type:      "LINKS",
	}))
	require.NoError(t, txDelete.DeleteNode(target))

	require.NoError(t, txDelete.Commit())
	require.ErrorIs(t, txCreateEdge.Commit(), ErrConflict)

	_, err = engine.GetNode(target)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge(EdgeID(prefixTestID("edge-created-before-delete-commit")))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTransaction_AdjacencyRepeatabilityWithinSnapshot(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	start := NodeID(prefixTestID("adjacency-snapshot-start"))
	target := NodeID(prefixTestID("adjacency-snapshot-target"))
	firstEdgeID := EdgeID(prefixTestID("adjacency-snapshot-edge-first"))

	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: target, Labels: []string{"Node"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: firstEdgeID, StartNode: start, EndNode: target, Type: "LINKS"}))

	txRead, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txRead.Rollback()

	outBefore, err := txRead.GetOutgoingEdges(start)
	require.NoError(t, err)
	inBefore, err := txRead.GetIncomingEdges(target)
	require.NoError(t, err)
	require.Equal(t, []string{string(firstEdgeID)}, sortedEdgeIDStrings(outBefore))
	require.Equal(t, []string{string(firstEdgeID)}, sortedEdgeIDStrings(inBefore))

	laterTarget := NodeID(prefixTestID("adjacency-snapshot-target-later"))
	laterEdgeID := EdgeID(prefixTestID("adjacency-snapshot-edge-later"))
	txInsert, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = txInsert.CreateNode(&Node{ID: laterTarget, Labels: []string{"Node"}})
	require.NoError(t, err)
	require.NoError(t, txInsert.CreateEdge(&Edge{ID: laterEdgeID, StartNode: start, EndNode: laterTarget, Type: "LINKS"}))
	require.NoError(t, txInsert.Commit())

	outAfter, err := txRead.GetOutgoingEdges(start)
	require.NoError(t, err)
	inAfter, err := txRead.GetIncomingEdges(laterTarget)
	require.NoError(t, err)
	require.Equal(t, sortedEdgeIDStrings(outBefore), sortedEdgeIDStrings(outAfter))
	require.Empty(t, inAfter)

	txFresh, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txFresh.Rollback()
	freshOut, err := txFresh.GetOutgoingEdges(start)
	require.NoError(t, err)
	require.Equal(t, []string{string(firstEdgeID), string(laterEdgeID)}, sortedEdgeIDStrings(freshOut))
}

func TestTransaction_EdgeTraversalRemainsSnapshotConsistentAcrossConcurrentDelete(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	start := NodeID(prefixTestID("snapshot-edge-start"))
	target := NodeID(prefixTestID("snapshot-edge-target"))
	edgeID := EdgeID(prefixTestID("snapshot-edge"))

	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: target, Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "target"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: target, Type: "LINKS"}))

	txDelete, err := engine.BeginTransaction()
	require.NoError(t, err)
	txRead, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txRead.Rollback()

	beforeEdge, err := txRead.getCommittedEdgeLocked(edgeID)
	require.NoError(t, err)
	beforeNode, err := txRead.getCommittedNodeLocked(target)
	require.NoError(t, err)
	betweenBefore, err := engine.GetEdgesBetweenVisibleAt(start, target, txRead.readTS)
	require.NoError(t, err)
	require.Len(t, betweenBefore, 1)
	require.Equal(t, edgeID, beforeEdge.ID)
	require.Equal(t, target, beforeNode.ID)

	require.NoError(t, txDelete.DeleteNode(target))
	require.NoError(t, txDelete.Commit())

	afterEdge, err := txRead.getCommittedEdgeLocked(edgeID)
	require.NoError(t, err)
	afterNode, err := txRead.getCommittedNodeLocked(target)
	require.NoError(t, err)
	betweenAfter, err := engine.GetEdgesBetweenVisibleAt(start, target, txRead.readTS)
	require.NoError(t, err)
	require.Len(t, betweenAfter, 1)
	require.Equal(t, edgeID, afterEdge.ID)
	require.Equal(t, target, afterNode.ID)

	_, err = engine.GetNode(target)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge(edgeID)
	require.ErrorIs(t, err, ErrNotFound)

	txFresh, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txFresh.Rollback()
	_, err = txFresh.GetNode(target)
	require.ErrorIs(t, err, ErrNotFound)
	betweenFresh, err := engine.GetEdgesBetweenVisibleAt(start, target, txFresh.readTS)
	require.NoError(t, err)
	require.Len(t, betweenFresh, 0)
}

func BenchmarkTransaction_HighContentionAbortRate(b *testing.B) {
	engine := NewMemoryEngine()
	b.Cleanup(func() { _ = engine.Close() })

	hotNodeIDs := make([]NodeID, 5)
	for i := range hotNodeIDs {
		hotNodeIDs[i] = NodeID(prefixTestID(fmt.Sprintf("hot-node-%d", i)))
		_, err := engine.CreateNode(&Node{
			ID:         hotNodeIDs[i],
			Labels:     []string{"Hot"},
			Properties: map[string]interface{}{"version": 0},
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	const workers = 100
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var commits atomic.Uint64
	var aborts atomic.Uint64
	var started atomic.Uint64
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for job := range jobs {
			tx, err := engine.BeginTransaction()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			targetID := hotNodeIDs[job%len(hotNodeIDs)]
			node, err := tx.GetNode(targetID)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			updated := copyNode(node)
			updated.Properties["version"] = job
			if err := tx.UpdateNode(updated); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			started.Add(1)
			if err := tx.Commit(); err != nil {
				if errors.Is(err, ErrConflict) {
					aborts.Add(1)
					continue
				}
				select {
				case errCh <- err:
				default:
				}
				return
			}
			commits.Add(1)
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	b.StopTimer()

	select {
	case err := <-errCh:
		b.Fatal(err)
	default:
	}

	total := started.Load()
	if total == 0 {
		b.Fatal("expected contention attempts")
	}
	abortRate := (float64(aborts.Load()) / float64(total)) * 100
	b.ReportMetric(float64(commits.Load()), "commits")
	b.ReportMetric(float64(aborts.Load()), "aborts")
	b.ReportMetric(abortRate, "abort_pct")
}

func countNodesWithBoolProperty(t *testing.T, tx *BadgerTransaction, label, property string) int {
	t.Helper()
	nodes, err := tx.getNodesByLabelLocked(label)
	require.NoError(t, err)
	count := 0
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if value, ok := node.Properties[property].(bool); ok && value {
			count++
		}
	}
	return count
}

func sortedNodeIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		if node != nil {
			ids = append(ids, node.ID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i]) < string(ids[j]) })
	return ids
}

func sortedEdgeIDStrings(edges []*Edge) []string {
	ids := make([]string, 0, len(edges))
	for _, edge := range edges {
		if edge != nil {
			ids = append(ids, string(edge.ID))
		}
	}
	sort.Strings(ids)
	return ids
}
