package storage

import "context"

type pendingBetweenKey struct {
	start    NodeID
	end      NodeID
	typeName string
}

func pendingEdgeBetweenKey(edge *Edge) pendingBetweenKey {
	return pendingBetweenKey{edge.StartNode, edge.EndNode, edge.Type}
}

func indexPendingBetween(index map[pendingBetweenKey]map[EdgeID]*Edge, edge *Edge) {
	if edge == nil {
		return
	}
	key := pendingEdgeBetweenKey(edge)
	edges := index[key]
	if edges == nil {
		edges = make(map[EdgeID]*Edge)
		index[key] = edges
	}
	edges[edge.ID] = edge
}

func (tx *BadgerTransaction) unindexPendingBetweenLocked(edge *Edge) {
	if edge == nil {
		return
	}
	key := pendingEdgeBetweenKey(edge)
	edges := tx.pendingBetween[key]
	delete(edges, edge.ID)
	if len(edges) == 0 {
		delete(tx.pendingBetween, key)
	}
}

// pendingBetweenLocked narrows a transaction's staged relationship overlay to
// one directed endpoint pair and type. The index is built once and maintained
// by setPendingEdgeLocked/removePendingEdgeLocked; read-only lock images stay
// outside it and are composed only for the exceptional read that needs them.
func (tx *BadgerTransaction) pendingBetweenLocked(ctx context.Context, start, end NodeID, edgeType string, images map[EdgeID]*Edge) (map[EdgeID]*Edge, error) {
	key := pendingBetweenKey{start, end, edgeType}
	if len(images) != len(tx.pendingEdges) {
		matched := make(map[EdgeID]*Edge)
		for _, edge := range images {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if edge != nil && pendingEdgeBetweenKey(edge) == key {
				matched[edge.ID] = edge
			}
		}
		return matched, nil
	}
	if tx.pendingBetween == nil {
		index := make(map[pendingBetweenKey]map[EdgeID]*Edge)
		for _, edge := range tx.pendingEdges {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			indexPendingBetween(index, edge)
		}
		tx.pendingBetween = index
	}
	return tx.pendingBetween[key], ctx.Err()
}

// pendingAdjacencyLocked indexes the transaction's incident edges on first
// adjacency use. Writes maintain it thereafter; unrelated edges never enter a
// node's read/DELETE working set. All access is under tx.mu.
func (tx *BadgerTransaction) pendingAdjacencyLocked(ctx context.Context, id NodeID) (map[EdgeID]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	images, err := writeOwnedReadImagesLocked(ctx, tx.pendingEdges, tx.deletedEdges, &tx.entityLocks, edgeWriteLockKind, tx.engine.GetStoredEdge)
	if err != nil {
		return nil, err
	}
	if len(images) != len(tx.pendingEdges) {
		// Read-only lock refreshes must not enter the staged adjacency index.
		// Compose their incident population for this read, using the same
		// endpoint membership as the staged index below.
		index := make(map[NodeID]map[EdgeID]*Edge)
		for _, edge := range images {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			indexPendingEdge(index, edge)
		}
		return index[id], nil
	}
	if tx.pendingAdjacency == nil && len(tx.pendingEdges) == 0 {
		return nil, nil
	}
	if tx.pendingAdjacency == nil {
		index := make(map[NodeID]map[EdgeID]*Edge)
		for _, edge := range tx.pendingEdges {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			indexPendingEdge(index, edge)
		}
		tx.pendingAdjacency = index
	}
	return tx.pendingAdjacency[id], nil
}

func indexPendingEdge(index map[NodeID]map[EdgeID]*Edge, edge *Edge) {
	if edge == nil {
		return
	}
	for _, id := range [...]NodeID{edge.StartNode, edge.EndNode} {
		edges := index[id]
		if edges == nil {
			edges = make(map[EdgeID]*Edge)
			index[id] = edges
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
		indexPendingEdge(tx.pendingAdjacency, edge)
	}
	if tx.pendingBetween != nil {
		tx.unindexPendingBetweenLocked(tx.pendingEdges[edge.ID])
		indexPendingBetween(tx.pendingBetween, edge)
	}
	tx.pendingEdges[edge.ID] = edge
}

func (tx *BadgerTransaction) removePendingEdgeLocked(id EdgeID) {
	if tx.pendingAdjacency != nil {
		tx.unindexPendingEdgeLocked(tx.pendingEdges[id])
	}
	if tx.pendingBetween != nil {
		tx.unindexPendingBetweenLocked(tx.pendingEdges[id])
	}
	delete(tx.pendingEdges, id)
}

func (tx *BadgerTransaction) mergePendingAdjacentEdgesLocked(ctx context.Context, committed []*Edge, request AdjacentEdgeRead, include func(*Edge) bool) ([]*Edge, error) {
	if len(tx.pendingEdges) == 0 && len(tx.deletedEdges) == 0 && tx.entityLocks.latest == 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return committed, nil
	}
	pending, err := tx.pendingAdjacencyLocked(ctx, request.Node)
	if err != nil {
		return nil, err
	}
	// Committed adjacency is caller-owned. A rewrite shadows its former
	// endpoints even when its new endpoints aren't in this node's index.
	result := committed[:0]
	for _, edge := range committed {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if edge == nil {
			continue
		}
		if _, gone := tx.deletedEdges[edge.ID]; gone {
			continue
		}
		if tx.committedEdgeSupersededLocked(edge.ID) || tx.readsLatestLocked(bufferedEdgeEntity, string(edge.ID)) {
			continue
		}
		if include(edge) {
			result = append(result, edge)
		}
	}
	for _, edge := range pending {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, gone := tx.deletedEdges[edge.ID]; !gone && include(edge) {
			result = append(result, request.header(copyEdge(edge)))
		}
	}
	return result, nil
}
