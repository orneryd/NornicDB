package storage

// StoredNodeImage returns the logical full-node image produced by the storage
// codec. Commit admission uses this for both prior and staged entities: pending
// values (including an acknowledged write-behind value) have not necessarily
// made the same codec round trip as a stored reader's result. The caller owns
// the transaction until admission and commit finish.
func (tx *BadgerTransaction) StoredNodeImage(node *Node) (*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}
	canonical := copyNode(node)
	data, separate, err := tx.engine.encodeNodeInTxn(tx.badgerTx, namespaceForNodeID(node.ID), canonical)
	if err != nil {
		return nil, err
	}
	image, err := tx.engine.decodeNode(namespaceForNodeID(node.ID), data)
	if err != nil {
		return nil, err
	}
	if separate {
		// Full stored reads hydrate separately stored vectors and clear the
		// physical placement flag. Keep the codec's persisted metadata.
		image.ChunkEmbeddings = canonical.ChunkEmbeddings
		image.EmbeddingsStoredSeparately = false
	}
	return image, nil
}

// StoredEdgeImage is StoredNodeImage for a relationship. In particular, compact
// timestamps and property values must be sized as decoded by stored readers,
// rather than as the caller's original Go values.
func (tx *BadgerTransaction) StoredEdgeImage(edge *Edge) (*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}
	data, err := tx.engine.encodeEdgeInTxn(tx.staged(), namespaceForEdgeID(edge.ID), edge)
	if err != nil {
		return nil, err
	}
	return tx.engine.decodeEdgeBodyByID(data, edge.ID)
}
