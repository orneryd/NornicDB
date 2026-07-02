package storage

func (tx *BadgerTransaction) getAllCommittedNodesLocked() ([]*Node, error) {
	if tx.readTS.IsZero() {
		return tx.engine.AllNodes()
	}
	return tx.engine.GetNodesByLabelVisibleAt("", tx.readTS)
}

// getCommittedAdjacentEdgesLocked returns the committed edges adjacent to
// nodeID in the requested direction. Under an active snapshot (non-zero read
// timestamp) it resolves against the directional visible-at adjacency index so
// the cost is O(deg(nodeID)) rather than O(E): the previous implementation
// scanned every visible edge in the graph and filtered by endpoint in memory,
// which degraded linearly with the total edge count on large graphs. Pending
// transaction writes are merged by the caller.
func (tx *BadgerTransaction) getCommittedAdjacentEdgesLocked(nodeID NodeID, direction string) ([]*Edge, error) {
	if tx.readTS.IsZero() {
		switch direction {
		case "outgoing":
			return tx.engine.GetOutgoingEdges(nodeID)
		case "incoming":
			return tx.engine.GetIncomingEdges(nodeID)
		default:
			return nil, ErrInvalidData
		}
	}
	switch direction {
	case "outgoing":
		return tx.engine.GetOutgoingEdgesVisibleAt(nodeID, tx.readTS)
	case "incoming":
		return tx.engine.GetIncomingEdgesVisibleAt(nodeID, tx.readTS)
	default:
		return nil, ErrInvalidData
	}
}
