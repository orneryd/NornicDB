package storage

// LabelNodeIDLookupEngine is an optional interface for engines that can return
// node IDs for a label without decoding full nodes.
//
// Labels match exactly: names are case-sensitive, as in Neo4j (#862).
// The visit function should return true to continue iteration, false to stop.
type LabelNodeIDLookupEngine interface {
	ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error
}

// FirstNodeIDByLabel returns the first node ID for a label without decoding nodes
// when possible. Returns ErrNotFound if no node matches.
func FirstNodeIDByLabel(engine Engine, label string) (NodeID, error) {
	if engine == nil {
		return "", ErrInvalidData
	}

	if lookup, ok := engine.(LabelNodeIDLookupEngine); ok {
		var found NodeID
		if err := lookup.ForEachNodeIDByLabel(label, func(id NodeID) bool {
			found = id
			return false
		}); err != nil {
			return "", err
		}
		if found == "" {
			return "", ErrNotFound
		}
		return found, nil
	}

	node, err := engine.GetFirstNodeByLabel(label)
	if err != nil {
		return "", err
	}
	if node == nil {
		return "", ErrNotFound
	}
	return node.ID, nil
}

// NodeIDsByLabel returns up to limit node IDs that have the label.
// If limit <= 0, all matches are returned (may be expensive).
func NodeIDsByLabel(engine Engine, label string, limit int) ([]NodeID, error) {
	if engine == nil {
		return nil, ErrInvalidData
	}

	if lookup, ok := engine.(LabelNodeIDLookupEngine); ok {
		ids := make([]NodeID, 0, 64)
		err := lookup.ForEachNodeIDByLabel(label, func(id NodeID) bool {
			ids = append(ids, id)
			if limit > 0 && len(ids) >= limit {
				return false
			}
			return true
		})
		return ids, err
	}

	nodes, err := engine.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		if node == nil {
			continue
		}
		ids = append(ids, node.ID)
		if limit > 0 && len(ids) >= limit {
			break
		}
	}
	return ids, nil
}
