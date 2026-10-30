package storage

// pendingAdjacencyLocked indexes the transaction's incident edges on first
// adjacency use. Writes maintain it thereafter; unrelated edges never enter a
// node's read/DELETE working set. All access is under tx.mu.
func (tx *BadgerTransaction) pendingAdjacencyLocked(id NodeID) map[EdgeID]*Edge {
	if tx.pendingAdjacency == nil && len(tx.pendingEdges) == 0 {
		return nil
	}
	if tx.pendingAdjacency == nil {
		tx.pendingAdjacency = make(map[NodeID]map[EdgeID]*Edge)
		for _, edge := range tx.pendingEdges {
			tx.indexPendingEdgeLocked(edge)
		}
	}
	return tx.pendingAdjacency[id]
}

func (tx *BadgerTransaction) indexPendingEdgeLocked(edge *Edge) {
	if edge == nil {
		return
	}
	for _, id := range [...]NodeID{edge.StartNode, edge.EndNode} {
		edges := tx.pendingAdjacency[id]
		if edges == nil {
			edges = make(map[EdgeID]*Edge)
			tx.pendingAdjacency[id] = edges
		}
		edges[edge.ID] = edge
	}
}

func (tx *BadgerTransaction) unindexPendingEdgeLocked(edge *Edge) {
	if edge == nil {
		return
	}
	for _, id := range [...]NodeID{edge.StartNode, edge.EndNode} {
		edges := tx.pendingAdjacency[id]
		delete(edges, edge.ID)
		if len(edges) == 0 {
			delete(tx.pendingAdjacency, id)
		}
	}
}

func (tx *BadgerTransaction) setPendingEdgeLocked(edge *Edge) {
	if tx.pendingAdjacency != nil {
		tx.unindexPendingEdgeLocked(tx.pendingEdges[edge.ID])
		tx.indexPendingEdgeLocked(edge)
	}
	tx.pendingEdges[edge.ID] = edge
}

func (tx *BadgerTransaction) removePendingEdgeLocked(id EdgeID) {
	if tx.pendingAdjacency != nil {
		tx.unindexPendingEdgeLocked(tx.pendingEdges[id])
	}
	delete(tx.pendingEdges, id)
}

// mergePendingAdjacentEdgesLocked resolves the transaction's own writes over
// the committed adjacency images before anything is delivered, then delivers
// the chosen result once: superseded committed edges are never copied
// (NOR-160).
func (tx *BadgerTransaction) mergePendingAdjacentEdgesLocked(committed []edgeImage, id NodeID, include func(*Edge) bool) []*Edge {
	if len(tx.pendingEdges) == 0 && len(tx.deletedEdges) == 0 {
		return deliverEdges(committed)
	}
	pending := tx.pendingAdjacencyLocked(id)
	// A rewrite shadows its former endpoints even when its new endpoints
	// aren't in this node's index.
	result := committed[:0]
	for _, edge := range committed {
		if edge.Edge == nil {
			continue
		}
		if _, gone := tx.deletedEdges[edge.ID]; gone {
			continue
		}
		if _, rewritten := tx.pendingEdges[edge.ID]; rewritten {
			continue
		}
		if include(edge.Edge) {
			result = append(result, edge)
		}
	}
	for _, edge := range pending {
		if _, gone := tx.deletedEdges[edge.ID]; !gone && include(edge) {
			// Retained by the transaction: delivered (copied) once.
			result = append(result, borrowedEdge(edge))
		}
	}
	return deliverEdges(result)
}
