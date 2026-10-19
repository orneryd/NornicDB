package storage

import "github.com/dgraph-io/badger/v4"

// EmbeddingBodyRevision is an opaque snapshot identity. A durable read uses
// Badger's body-key version. An acknowledged buffered read uses its existing
// MVCC commit sequence, which remains the same when replay materializes it.
// Neither representation depends on the caller-controlled UpdatedAt.
type EmbeddingBodyRevision struct {
	physical         uint64
	bufferedSequence uint64
}

func (revision EmbeddingBodyRevision) isZero() bool {
	return revision.physical == 0 && revision.bufferedSequence == 0
}

func (revision EmbeddingBodyRevision) matches(b *BadgerEngine, txn *badger.Txn, id NodeID, physical uint64) (bool, error) {
	if revision.bufferedSequence == 0 {
		return revision.physical == physical, nil
	}
	head, err := b.loadNodeMVCCHeadInTxn(txn, id)
	if err != nil {
		return false, err
	}
	return head.Version.CommitSequence == revision.bufferedSequence, nil
}

// SnapshotNodeForEmbedding binds a body and its revision in one read. Ordinary
// reads keep their existing write-cache fast path; only an embedding claim
// needs to acquire a revision when that cache came from a body write. Copies
// and namespace decorators preserve the private reader provenance.
func SnapshotNodeForEmbedding(node *Node) (*Node, error) {
	if node == nil || !node.EmbeddingRevision.isZero() || node.embeddingOwner == nil {
		return node, nil
	}
	b, id := node.embeddingOwner, node.embeddingOwnerID
	snapshot, err := b.getNodeSnapshot(id, true)
	if err == nil {
		snapshot.ID = node.ID
	}
	return snapshot, err
}
