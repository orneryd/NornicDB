package storage

import "maps"

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
	canonical := *node
	canonical.ID = namespace.prefixNodeID(canonical.ID)
	// The codec only mutates embedding metadata when splitting a large body.
	// Properties, labels and inline vectors are read-only and decode into a
	// separate owned image; copying them before encoding serves no purpose.
	canonical.EmbedMeta = maps.Clone(node.EmbedMeta)
	data, separate, err := tx.engine.encodeNodeInTxn(tx.badgerTx, tx.namespace, &canonical)
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
		if len(canonical.ChunkEmbeddings) > 0 {
			image.ChunkEmbeddings = make([][]float32, len(canonical.ChunkEmbeddings))
			for i, chunk := range canonical.ChunkEmbeddings {
				image.ChunkEmbeddings[i] = make([]float32, len(chunk))
				copy(image.ChunkEmbeddings[i], chunk)
			}
		}
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
	canonical := *edge
	canonical.ID = namespace.prefixEdgeID(canonical.ID)
	canonical.StartNode = namespace.prefixNodeID(canonical.StartNode)
	canonical.EndNode = namespace.prefixNodeID(canonical.EndNode)
	data, err := tx.engine.encodeEdgeInTxn(tx.staged(), tx.namespace, &canonical)
	if err != nil {
		return nil, err
	}
	return tx.engine.decodeEdgeBodyByID(data, canonical.ID)
}
