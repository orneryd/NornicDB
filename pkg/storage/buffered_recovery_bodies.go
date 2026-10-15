package storage

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

// restoreBufferedOperationBodies repairs legacy JSON operation projections from
// their authoritative typed bodies before MVCC archiving and cache publication.
// It runs only during WAL recovery; ordinary in-memory flushes already own typed
// operations. Read prior bodies before applying raw writes/deletes: node deletion
// also needs the committed relationship bodies for adjacency/history.
func (b *BadgerEngine) restoreBufferedOperationBodies(txn *badger.Txn, commit *bufferedCommit) error {
	states := newCommitStates(commit.ops)
	read := func(key []byte) ([]byte, error) {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return item.ValueCopy(nil)
	}
	for id, old := range states.committedNodes {
		if old == nil {
			continue
		}
		data, err := read(nodeKey(id))
		if err != nil {
			return err
		}
		if data == nil {
			continue
		}
		node, err := b.decodeNodeWithEmbeddings(txn, data, id)
		if err != nil {
			return fmt.Errorf("recover prior node %s: %w", id, err)
		}
		states.committedNodes[id] = node
	}
	for id, old := range states.committedEdges {
		if old == nil {
			continue
		}
		data, err := read(edgeKey(id))
		if err != nil {
			return err
		}
		if data == nil {
			continue
		}
		edge, err := b.decodeEdgeBodyByID(data, id)
		if err != nil {
			return fmt.Errorf("recover prior edge %s: %w", id, err)
		}
		states.committedEdges[id] = edge
	}
	for id, projected := range states.finalNodes {
		if projected == nil {
			continue
		}
		data := commit.writes[string(nodeKey(id))]
		if len(data) == 0 {
			continue
		}
		node, err := b.decodeNode(namespaceForNodeID(id), data)
		if err != nil {
			return fmt.Errorf("recover final node %s: %w", id, err)
		}
		readFinal := embeddingRecordReader{txn: txn, commit: commit}
		if err := restoreBufferedNodeVectors(node, id, readFinal); err != nil {
			return err
		}
		states.finalNodes[id] = node
	}
	for id, projected := range states.finalEdges {
		if projected == nil {
			continue
		}
		data := commit.writes[string(edgeKey(id))]
		if len(data) == 0 {
			continue
		}
		edge, _, _, err := b.decodeEdgeCompactV2(commit.namespace, data)
		if err != nil {
			return fmt.Errorf("recover final edge %s: %w", id, err)
		}
		// Endpoint dictionary writes may belong to this same unflushed commit.
		// Their string IDs in the JSON operation are already exact.
		edge.ID = id
		edge.StartNode = projected.StartNode
		edge.EndNode = projected.EndNode
		states.finalEdges[id] = edge
	}
	for i := range commit.ops {
		op := &commit.ops[i]
		if op.Node != nil {
			if node := states.finalNodes[op.Node.ID]; node != nil {
				op.Node = node
			}
		}
		if op.OldNode != nil {
			op.OldNode = states.committedNodes[op.OldNode.ID]
		}
		if op.Type == OpDeleteNode {
			op.OldNode = states.committedNodes[op.NodeID]
		}
		if op.Edge != nil {
			if edge := states.finalEdges[op.Edge.ID]; edge != nil {
				op.Edge = edge
			}
		}
		if op.OldEdge != nil {
			op.OldEdge = states.committedEdges[op.OldEdge.ID]
		}
		if op.Type == OpDeleteEdge {
			op.OldEdge = states.committedEdges[op.EdgeID]
		}
	}
	return nil
}

// restoreBufferedNodeVectors reads the final sidecar view without mutating the
// transaction. Legacy operation JSON omits all vectors and embedding metadata.
func restoreBufferedNodeVectors(node *Node, id NodeID, read embeddingRecordReader) error {
	fresh := false
	data, err := read.read(embeddingMetaKey(id), nil)
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	if err == nil {
		var meta map[string]any
		if err := decodeValue(data, &meta); err != nil {
			return err
		}
		meta, fresh, err = validateEmbeddingSidecarMeta(meta, node)
		if err != nil {
			return err
		}
		if fresh {
			delete(meta, embeddingContentUpdatedAtKey)
			node.EmbedMeta = meta
			if len(meta) == 0 {
				node.EmbedMeta = nil
			}
		}
	}
	if fresh || node.EmbeddingsStoredSeparately {
		if err := loadEmbeddingChunksFrom(read, node, id, chunkCountFromMeta(node.EmbedMeta)); err != nil {
			return err
		}
		node.EmbeddingsStoredSeparately = false
	}
	return nil
}
