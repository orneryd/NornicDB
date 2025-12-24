package storage

// LabelIndexEngine is an optional interface implemented by engines that can
// answer label-membership queries via an index, without decoding full nodes.
//
// This is used by performance-sensitive query executors that need to enforce
// `(n:Label)` semantics while avoiding per-node GetNode/BatchGetNodes overhead.
type LabelIndexEngine interface {
	// HasLabelBatch returns a map of node IDs that have the given label.
	// Implementations should treat labels case-insensitively (Neo4j compatible).
	// Missing nodes are treated as not having the label.
	HasLabelBatch(ids []NodeID, label string) (map[NodeID]bool, error)
}

