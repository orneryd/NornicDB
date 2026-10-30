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
func (b *BadgerEngine) restoreBufferedOperationBodies(txn *badger.Txn, commit *bufferedCommit, writes map[string][]byte, deletes map[string]bool) error {
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
		data := writes[string(nodeKey(id))]
		if len(data) == 0 {
			continue
		}
		node, err := b.decodeWrittenNode(id, data)
		if err != nil {
			return err
		}
		readFinal := embeddingRecordReader{txn: txn, stagedWrites: writes, stagedDeletes: deletes}
		if err := restoreBufferedNodeVectors(node, id, readFinal); err != nil {
			return err
		}
		states.finalNodes[id] = node
	}
	for id, projected := range states.finalEdges {
		if projected == nil {
			continue
		}
		data := writes[string(edgeKey(id))]
		if len(data) == 0 {
			continue
		}
		edge, err := b.decodeWrittenEdge(data, projected)
		if err != nil {
			return err
		}
		states.finalEdges[id] = edge
	}
	// Committed/final bodies describe MVCC endpoints. Cache/index publication
	// follows the ordered operations instead: an update followed by deletion
	// must unregister the intermediate value, not the initial committed one.
	currentNodes := make(map[NodeID]*Node, len(states.committedNodes))
	for id, node := range states.committedNodes {
		currentNodes[id] = node
	}
	currentEdges := make(map[EdgeID]*Edge, len(states.committedEdges))
	for id, edge := range states.committedEdges {
		currentEdges[id] = edge
	}
	lastNodeWrite := make(map[NodeID]int)
	lastEdgeWrite := make(map[EdgeID]int)
	for i, op := range commit.ops {
		if op.Node != nil {
			lastNodeWrite[op.Node.ID] = i
		}
		if op.Edge != nil {
			lastEdgeWrite[op.Edge.ID] = i
		}
	}
	for i := range commit.ops {
		op := &commit.ops[i]
		if op.OldNode != nil {
			op.OldNode = currentNodes[op.OldNode.ID]
		}
		if op.Node != nil {
			if node := states.finalNodes[op.Node.ID]; node != nil && lastNodeWrite[op.Node.ID] == i {
				op.Node = node
			}
			currentNodes[op.Node.ID] = op.Node
		}
		if op.Type == OpDeleteNode {
			op.OldNode = currentNodes[op.NodeID]
			delete(currentNodes, op.NodeID)
		}
		if op.OldEdge != nil {
			op.OldEdge = currentEdges[op.OldEdge.ID]
		}
		if op.Edge != nil {
			if edge := states.finalEdges[op.Edge.ID]; edge != nil && lastEdgeWrite[op.Edge.ID] == i {
				op.Edge = edge
			}
			currentEdges[op.Edge.ID] = op.Edge
		}
		if op.Type == OpDeleteEdge {
			op.OldEdge = currentEdges[op.EdgeID]
			delete(currentEdges, op.EdgeID)
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
			delete(meta, embeddingRangeManifestKey)
			node.EmbedMeta = meta
			if len(meta) == 0 {
				node.EmbedMeta = nil
			}
		}
	}
	if fresh || node.RuntimeMetadata().EmbeddingsStoredSeparately {
		if err := loadEmbeddingChunksFrom(read, node, id, chunkCountFromMeta(node.EmbedMeta)); err != nil {
			return err
		}
		{
			metadata := node.RuntimeMetadata()
			metadata.EmbeddingsStoredSeparately = false
			node.SetRuntimeMetadata(metadata)
		}
	}
	return nil
}
