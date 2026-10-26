package storage

// StoredEntityChange is one entity's stored image before and after a logical
// commit. A nil image denotes absence. Exactly one entity kind is populated;
// repeated mutations of an ID produce one pair. Images include separately
// stored embeddings and metadata and belong to the commit, not its caller.
// Consumers must treat the images as immutable.
type StoredEntityChange struct {
	BeforeNode *Node
	AfterNode  *Node
	BeforeEdge *Edge
	AfterEdge  *Edge
}

// StoredEntityCountDelta folds physical stored presence in namespace. Hidden
// entities still consume capacity. Empty namespace selects all images.
// For example, an absent prior and present final node contribute one node.
func StoredEntityCountDelta(namespace string, changes []StoredEntityChange) (nodes, edges int64) {
	matches := func(id string) bool {
		if namespace == "" {
			return true
		}
		actual, _, _ := ParseDatabasePrefix(id)
		return actual == namespace
	}
	for _, change := range changes {
		if change.BeforeNode != nil || change.AfterNode != nil {
			if change.BeforeNode != nil && matches(string(change.BeforeNode.ID)) {
				nodes--
			}
			if change.AfterNode != nil && matches(string(change.AfterNode.ID)) {
				nodes++
			}
		} else {
			if change.BeforeEdge != nil && matches(string(change.BeforeEdge.ID)) {
				edges--
			}
			if change.AfterEdge != nil && matches(string(change.AfterEdge.ID)) {
				edges++
			}
		}
	}
	return nodes, edges
}
