package storage

import (
	"context"
	"strings"
)

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
func (tx *BadgerTransaction) clearNodeEmbeddingRangeLocked(ctx context.Context, id NodeID) error {
	if err := tx.scanCommittedEmbeddingKeysLocked(ctx, id, func(key []byte) error { tx.bufferDelete(key); return nil }); err != nil {
		return err
	}
	tx.clearPendingNodeEmbeddingRangeLocked(id)
	return nil
}

func (tx *BadgerTransaction) clearPendingNodeEmbeddingRangeLocked(id NodeID) {
	// Chunk/sidecar writes accompany the staged node image. Without such an
	// image there is no pending embedding range; scanning every other staged
	// key for each ordinary delete would make a batch quadratic.
	pending := tx.pendingNodes[id]
	if pending == nil || (len(pending.ChunkEmbeddings) == 0 && pending.EmbedMeta == nil && !pending.RuntimeMetadata().EmbeddingsStoredSeparately) {
		return
	}
	prefix := string(embeddingPrefix(id))
	for key := range tx.pendingWrites {
		if strings.HasPrefix(key, prefix) {
			tx.bufferDelete([]byte(key))
		}
	}
}
