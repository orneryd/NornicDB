package storage

import "strings"

// visitOperationTotalCounts describes only cardinality-changing operations.
// An update's entity overlay is not a newly created entity. The same deltas
// feed acknowledged reads and the persisted/cache publication boundary.
func visitOperationTotalCounts(op Operation, visit func(string, bool, int64)) {
	switch op.Type {
	case OpCreateNode:
		if op.Node != nil {
			visit(string(op.Node.ID), false, 1)
		}
	case OpDeleteNode:
		visit(string(op.NodeID), false, -1)
		for _, id := range op.DeletedEdgeIDs {
			visit(string(id), true, -1)
		}
		// Historical WAL records may carry only the cascade count. Their writer
		// attributed those relationships to the deleted node's namespace.
		if remaining := op.EdgesDeleted - int64(len(op.DeletedEdgeIDs)); remaining > 0 {
			visit(string(op.NodeID), true, -remaining)
		}
	case OpCreateEdge:
		if op.Edge != nil && !op.ReplacesExisting {
			visit(string(op.Edge.ID), true, 1)
		}
	case OpDeleteEdge:
		visit(string(op.EdgeID), true, -1)
	}
}

// Caller holds totalCountWriteMu until the matching countsApplied flag is set.
func (b *BadgerEngine) publishBufferedTotalCounts(commits []bufferedCommit) {
	visit := func(id string, edge bool, delta int64) {
		if edge {
			b.edgeCount.Add(delta)
			b.addNamespaceEdgeCount(EdgeID(id), delta)
		} else {
			b.nodeCount.Add(delta)
			b.addNamespaceNodeCount(NodeID(id), delta)
		}
	}
	for _, commit := range commits {
		for _, op := range commit.ops {
			visitOperationTotalCounts(op, visit)
		}
	}
}

// bufferedCardinalityDeltas is indexed when a write is appended. Count reads
// do not walk a statement history (many updates may touch the same entity), and
// the standalone append API can coexist with committed transaction records.
type bufferedCardinalityDeltas struct {
	nodes, edges         map[string]int64
	nodeTotal, edgeTotal int64
}

func (d *bufferedCardinalityDeltas) add(id string, edge bool, change int64) {
	values := &d.nodes
	if edge {
		values = &d.edges
		d.edgeTotal += change
	} else {
		d.nodeTotal += change
	}
	if *values == nil {
		*values = make(map[string]int64)
	}
	next := (*values)[id] + change
	if next == 0 {
		delete(*values, id)
	} else {
		(*values)[id] = next
	}
}

func (d *bufferedCardinalityDeltas) count(prefix string, edge bool) int64 {
	if prefix == "" {
		if edge {
			return d.edgeTotal
		}
		return d.nodeTotal
	}
	values := d.nodes
	if edge {
		values = d.edges
	}
	var count int64
	for id, delta := range values {
		if strings.HasPrefix(id, prefix) {
			count += delta
		}
	}
	return count
}

func (w *WriteBehindBuffer) totalCountDeltaForPrefix(prefix string, edge bool) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var delta int64
	for _, buf := range w.liveBuffers() {
		if !buf.countsApplied.Load() {
			delta += buf.totalCounts.count(prefix, edge)
		}
	}
	return delta
}
