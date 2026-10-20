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

// MatchesNode is the common body-change decision for claims, DLQ recovery and
// publication. A buffered ACK and its materialized body are the same revision;
// caller timestamps and notification delivery are not revision evidence.
func (revision EmbeddingBodyRevision) MatchesNode(node *Node) (bool, error) {
	if node == nil {
		return false, nil
	}
	current := node.embeddingRevision
	if revision == current {
		return true, nil
	}
	if revision.bufferedSequence == 0 || current.physical == 0 || node.embeddingOwner == nil {
		return false, nil
	}
	var same bool
	err := node.embeddingOwner.withReadView(nil, func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(node.embeddingOwnerID))
		if err != nil {
			return err
		}
		if item.Version() != current.physical {
			return nil
		}
		same, err = revision.matches(node.embeddingOwner, txn, node.embeddingOwnerID, item.Version())
		return err
	})
	return same, err
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

// Publication needs only the identity, not a decoded body or hydrated vectors.
// The ordered event drainer supplies the publication boundary; an acknowledged
// buffered body takes precedence over the physical body, as in snapshot reads.
func (b *BadgerEngine) embeddingRevisionIsCurrent(id NodeID, revision EmbeddingBodyRevision, proof ...embeddingPublicationProof) (bool, error) {
	var buffered *Node
	proven := false
	view, err := b.captureReadView(nil, readViewOptions{scope: namespaceForNodeID(id), cached: func(w *WriteBehindBuffer) (bool, error) {
		state := w.entityStateLocked(bufferedNodeEntity, string(id))
		if !state.found {
			if len(proof) != 0 && proof[0].matches(b, id, revision) {
				proven = true
				return true, nil
			}
			return false, nil
		}
		if state.deleted {
			return true, ErrNotFound
		}
		buffered = state.node
		return true, nil
	}})
	if err != nil {
		return false, err
	}
	if view == nil {
		if proven {
			return true, nil
		}
		return revision.MatchesNode(buffered)
	}
	var same bool
	err = view.readAndClose(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		same, err = revision.matches(b, txn, id, item.Version())
		return err
	})
	return same, err
}

// SnapshotNodeForEmbedding binds a body and its revision in one read. Ordinary
// reads keep their existing write-cache fast path; only an embedding claim
// needs to acquire a revision when that cache came from a body write. Copies
// and namespace decorators preserve the private reader provenance.
func SnapshotNodeForEmbedding(node *Node) (*Node, error) {
	if node == nil || !node.embeddingRevision.isZero() || node.embeddingOwner == nil {
		return node, nil
	}
	b, id := node.embeddingOwner, node.embeddingOwnerID
	snapshot, err := b.getNodeSnapshot(id, true)
	if err == nil {
		snapshot.ID = node.ID
	}
	return snapshot, err
}
