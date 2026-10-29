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
	var found NodeID
	seen := false
	err := VisitNodeIDsByLabel(engine, label, func(id NodeID) bool { found = id; seen = true; return false })
	if err != nil {
		return "", err
	}
	if !seen {
		return "", ErrNotFound
	}
	return found, nil
}

// NodeIDsByLabel returns up to limit node IDs that have the label.
// If limit <= 0, all matches are returned (may be expensive).
func NodeIDsByLabel(engine Engine, label string, limit int) ([]NodeID, error) {
	if engine == nil {
		return nil, ErrInvalidData
	}
	ids := make([]NodeID, 0, 64)
	err := VisitNodeIDsByLabel(engine, label, func(id NodeID) bool {
		ids = append(ids, id)
		return limit <= 0 || len(ids) < limit
	})
	return ids, err
}
