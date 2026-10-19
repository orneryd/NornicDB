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
	return tx.storedNodeImageLocked(node)
}

func (tx *BadgerTransaction) storedNodeImageLocked(node *Node) (*Node, error) {
	namespace := NewNamespacedEngine(tx.engine, tx.namespace)
	canonical := copyNode(node)
	canonical.ID = namespace.prefixNodeID(canonical.ID)
	data, separate, err := tx.engine.encodeNodeInTxn(tx.badgerTx, tx.namespace, canonical)
	if err != nil {
		return nil, err
	}
	image, err := tx.engine.decodeNode(tx.namespace, data)
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
	return tx.storedEdgeImageLocked(edge)
}

func (tx *BadgerTransaction) storedEdgeImageLocked(edge *Edge) (*Edge, error) {
	namespace := NewNamespacedEngine(tx.engine, tx.namespace)
	canonical := copyEdge(edge)
	canonical.ID = namespace.prefixEdgeID(canonical.ID)
	canonical.StartNode = namespace.prefixNodeID(canonical.StartNode)
	canonical.EndNode = namespace.prefixNodeID(canonical.EndNode)
	data, err := tx.engine.encodeEdgeInTxn(tx.staged(), tx.namespace, canonical)
	if err != nil {
		return nil, err
	}
	return tx.engine.decodeEdgeBodyByID(data, canonical.ID)
}
