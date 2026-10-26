package storage

import (
	"fmt"
	"time"
)

// StoredNodeImage returns the image the storage codec gives node, a body the
// transaction has staged: the body a stored read returns once it commits.
// Every acknowledged body already is such an image (writtenNodeImage), so
// commit admission images only what it stages. The caller owns the
// transaction until admission and commit finish.
func (tx *BadgerTransaction) StoredNodeImage(node *Node) (*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}
	namespace := NewNamespacedEngine(tx.engine, tx.namespace)
	if image, ok := writtenNodeSource(node); ok {
		image.ID = namespace.PrefixNodeID(image.ID)
		return image, nil
	}
	canonical := copyNode(node)
	canonical.ID = namespace.PrefixNodeID(canonical.ID)
	data, _, err := tx.engine.encodeNodeInTxn(tx.badgerTx, tx.namespace, canonical)
	if err != nil {
		return nil, err
	}
	return tx.engine.writtenNodeImage(data, canonical)
}

// StoredEdgeImage is StoredNodeImage for a relationship.
func (tx *BadgerTransaction) StoredEdgeImage(edge *Edge) (*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}
	namespace := NewNamespacedEngine(tx.engine, tx.namespace)
	canonical := copyEdge(edge)
	canonical.ID = namespace.PrefixEdgeID(canonical.ID)
	canonical.StartNode = namespace.PrefixNodeID(canonical.StartNode)
	canonical.EndNode = namespace.PrefixNodeID(canonical.EndNode)
	return tx.codecEdgeImageLocked(canonical)
}

// codecEdgeImageLocked returns edge, which carries physical IDs, as the
// storage codec would commit it (writtenEdgeImage): its property values take
// their stored representation (a homogeneous list becomes a typed slice,
// numbers keep their stored width, temporal values their stored instant), so
// it compares with a body read from storage on equal terms.
func (tx *BadgerTransaction) codecEdgeImageLocked(edge *Edge) (*Edge, error) {
	if image, ok := writtenEdgeSource(edge); ok {
		return image, nil
	}
	data, err := tx.engine.encodeEdgeInTxn(tx.staged(), tx.namespace, edge)
	if err != nil {
		return nil, err
	}
	return tx.engine.decodeWrittenEdge(data, edge)
}

// keepSourceEmbeddings completes a decoded node body with the embedding state
// a full stored read hydrates from outside the body: vectors the codec stored
// apart, or a managed embedding whose sidecar the body omits
// (reconcileManagedEmbeddingsLocked). It takes them from source, the node the
// body was encoded for; the codec's persisted embedding metadata stays.
func keepSourceEmbeddings(image, source *Node) {
	switch {
	case image.RuntimeMetadata().EmbeddingsStoredSeparately:
		image.ChunkEmbeddings = source.ChunkEmbeddings
		{
			metadata := image.RuntimeMetadata()
			metadata.EmbeddingsStoredSeparately = false
			image.SetRuntimeMetadata(metadata)
		}
	case len(image.ChunkEmbeddings) == 0 && image.EmbedMeta == nil:
		image.ChunkEmbeddings = source.ChunkEmbeddings
		image.EmbedMeta = source.EmbedMeta
	}
}

// writtenNodeImage is the single owner of an acknowledged node body: the
// image a stored read decodes for node once data, the body a write path
// encoded for it, lands. Every ingress publishes this image, never the
// writer's own values: transaction commits (storeWrittenBodies), direct and
// bulk engine writes and recovery. Property values take their stored
// representation and node keeps the embedding state a full stored read
// hydrates. node is the ingress's own body, the one it publishes: when its
// image is known without decoding (codecNodeProperties), node itself takes
// the stored values (setNodeImage), a property map it holds replaced rather
// than modified; otherwise the image is decoded.
func (b *BadgerEngine) writtenNodeImage(data []byte, node *Node) (*Node, error) {
	if properties, ok := codecNodeProperties(node); ok {
		setNodeImage(node, properties)
		return node, nil
	}
	image, err := b.decodeWrittenNode(node.ID, data)
	if err != nil {
		return nil, err
	}
	keepSourceEmbeddings(image, node)
	// The runtime embedding identity is never encoded; the image keeps the
	// writer's, as an undecoded body and storage's node copies do.
	image.CopyEmbeddingIdentityFrom(node)
	return image, nil
}

// writtenEdgeImage is writtenNodeImage for a relationship.
func (b *BadgerEngine) writtenEdgeImage(data []byte, edge *Edge) (*Edge, error) {
	if properties, ok := codecImageProperties(edge.Properties); ok {
		setEdgeImage(edge, properties)
		return edge, nil
	}
	return b.decodeWrittenEdge(data, edge)
}

// decodeWrittenNode decodes data, the body encoded for node id.
func (b *BadgerEngine) decodeWrittenNode(id NodeID, data []byte) (*Node, error) {
	node, err := b.decodeNode(namespaceForNodeID(id), data)
	if err != nil {
		return nil, fmt.Errorf("decode written node %s: %w", id, err)
	}
	return node, nil
}

// decodeWrittenEdge is decodeWrittenNode for a relationship, whose endpoint IDs
// come from source, the body data was encoded from.
func (b *BadgerEngine) decodeWrittenEdge(data []byte, source *Edge) (*Edge, error) {
	edge, _, _, err := b.decodeEdgeCompactV2(namespaceForEdgeID(source.ID), data)
	if err != nil {
		return nil, fmt.Errorf("decode written edge %s: %w", source.ID, err)
	}
	// Endpoint dictionary writes may belong to this same unflushed commit.
	// Their string IDs in the operation are already exact.
	edge.ID = source.ID
	edge.StartNode = source.StartNode
	edge.EndNode = source.EndNode
	return edge, nil
}

// writtenBodyKey is nodeKey or edgeKey appended to buf, for a lookup that
// keeps no key.
func writtenBodyKey(buf []byte, prefix byte, id string) []byte {
	return append(append(buf, prefix), id...)
}

// storeWrittenBodies gives the last operation on each entity in ops the image
// of the body writes holds for it (writtenNodeImage, writtenEdgeImage), so a
// commit publishes to the write-behind overlay, the entity caches and its
// callbacks what a read from storage returns. Earlier operations on the same
// entity keep their intermediate values for ordered publication.
func (b *BadgerEngine) storeWrittenBodies(writes map[string][]byte, ops []Operation) error {
	var keyBuf [64]byte
	var lastNodeWrite map[NodeID]int
	var lastEdgeWrite map[EdgeID]int
	if len(ops) > 1 {
		lastNodeWrite = make(map[NodeID]int)
		lastEdgeWrite = make(map[EdgeID]int)
		for i, op := range ops {
			if op.Node != nil {
				lastNodeWrite[op.Node.ID] = i
			}
			if op.Edge != nil {
				lastEdgeWrite[op.Edge.ID] = i
			}
		}
	}
	for i := range ops {
		op := &ops[i]
		if op.Node != nil && (lastNodeWrite == nil || lastNodeWrite[op.Node.ID] == i) {
			if data := writes[string(writtenBodyKey(keyBuf[:0], prefixNode, string(op.Node.ID)))]; len(data) != 0 {
				node, err := b.writtenNodeImage(data, op.Node)
				if err != nil {
					return err
				}
				op.Node = node
			}
		}
		if op.Edge != nil && (lastEdgeWrite == nil || lastEdgeWrite[op.Edge.ID] == i) {
			if data := writes[string(writtenBodyKey(keyBuf[:0], prefixEdge, string(op.Edge.ID)))]; len(data) != 0 {
				edge, err := b.writtenEdgeImage(data, op.Edge)
				if err != nil {
					return err
				}
				op.Edge = edge
			}
		}
	}
	return nil
}

// writtenNodeSource returns, as a copy, the image a stored read decodes for
// node's body without decoding it (codecNodeProperties), for a body a
// transaction has only staged.
func writtenNodeSource(node *Node) (*Node, bool) {
	properties, ok := codecNodeProperties(node)
	if !ok {
		return nil, false
	}
	image := *node
	setNodeImage(&image, properties)
	return &image, true
}

// codecNodeProperties returns node's properties as a stored read decodes
// them, when the codec's image of every value is known without decoding
// (codecImageProperties) and node carries no embedding state the body stores
// apart, types anew or hydrates from elsewhere.
func codecNodeProperties(node *Node) (map[string]any, bool) {
	if node.NamedEmbeddings != nil || node.ChunkEmbeddings != nil || node.EmbedMeta != nil ||
		node.RuntimeMetadata().EmbeddingsStoredSeparately {
		return nil, false
	}
	return codecImageProperties(node.Properties)
}

// setNodeImage gives node the image's properties and the timestamps the
// envelope stores.
func setNodeImage(node *Node, properties map[string]any) {
	node.Properties = properties
	node.CreatedAt = envelopeTime(node.CreatedAt)
	node.UpdatedAt = envelopeTime(node.UpdatedAt)
}

// writtenEdgeSource is writtenNodeSource for a relationship.
func writtenEdgeSource(edge *Edge) (*Edge, bool) {
	properties, ok := codecImageProperties(edge.Properties)
	if !ok {
		return nil, false
	}
	image := *edge
	setEdgeImage(&image, properties)
	return &image, true
}

// setEdgeImage is setNodeImage for a relationship, whose compact header
// stores its timestamps as UTC nanoseconds.
func setEdgeImage(edge *Edge, properties map[string]any) {
	edge.Properties = properties
	edge.CreatedAt = compactEdgeTime(edge.CreatedAt)
	edge.UpdatedAt = compactEdgeTime(edge.UpdatedAt)
}

// codecImageProperties adapts the shared owned-property representation for
// written images. The codec remains the owner of the envelope and wire format.
func codecImageProperties(properties map[string]any) (map[string]any, bool) {
	return ownPropertyMap(properties)
}

// envelopeTime is t as the node body envelope's msgpack time extension
// decodes it: the same instant in the local zone, the zero time in UTC.
func envelopeTime(t time.Time) time.Time {
	stored := time.Unix(t.Unix(), int64(t.Nanosecond()))
	if stored.IsZero() {
		return stored.UTC()
	}
	return stored
}
