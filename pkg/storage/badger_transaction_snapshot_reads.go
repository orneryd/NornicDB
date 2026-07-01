package storage

func (tx *BadgerTransaction) getAllCommittedNodesLocked() ([]*Node, error) {
	if tx.readTS.IsZero() {
		return tx.engine.AllNodes()
	}
	return tx.engine.GetNodesByLabelVisibleAt("", tx.readTS)
}

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
	edges, err := tx.engine.GetEdgesByTypeVisibleAt("", tx.readTS)
	if err != nil {
		return nil, err
	}
	filtered := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		switch direction {
		case "outgoing":
			if edge.StartNode == nodeID {
				filtered = append(filtered, edge)
			}
		case "incoming":
			if edge.EndNode == nodeID {
				filtered = append(filtered, edge)
			}
		default:
			return nil, ErrInvalidData
		}
	}
	return filtered, nil
}
