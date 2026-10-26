package storage

import "strings"

// bufferNodeChunksLocked is the staging owner shared by native creates and
// updates whenever the destination codec chooses separate chunk storage.
func (tx *BadgerTransaction) bufferNodeChunksLocked(node *Node) error {
	for i, embedding := range node.ChunkEmbeddings {
		writes, err := buildEmbeddingChunkWriteKVs(node.ID, i, embedding)
		if err != nil {
			return err
		}
		for _, write := range writes {
			tx.bufferSet(write.key, write.val)
		}
	}
	return nil
}

// clearNodeEmbeddingRangeLocked replaces both the stored range and any writes
// staged earlier in this transaction. The existing scan includes manifests,
// metadata and chunk shards; no post-commit repair is needed.
func (tx *BadgerTransaction) clearNodeEmbeddingRangeLocked(id NodeID) error {
	if err := tx.scanCommittedEmbeddingKeysLocked(id, func(key []byte) error { tx.bufferDelete(key); return nil }); err != nil {
		return err
	}
	tx.clearPendingNodeEmbeddingRangeLocked(id)
	return nil
}

func (tx *BadgerTransaction) clearPendingNodeEmbeddingRangeLocked(id NodeID) {
	prefix := string(embeddingPrefix(id))
	for key := range tx.pendingWrites {
		if strings.HasPrefix(key, prefix) {
			tx.bufferDelete([]byte(key))
		}
	}
}
