package storage

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// readCommittedNodeAtSnapshotLocked answers one committed node read at the
// transaction's read version.
//
// A transaction that holds an actually pinned physical snapshot owns the
// version-qualified body of every node that snapshot contains, so it reads the
// body from that snapshot through the existing body owner (readNodeItemInTxn)
// instead of resolving the node's logical MVCC head again per node. This is
// the same physical cut the pinned label scan
// (streamNodesByLabelFromPhysicalSnapshotAfterRead) already reads its
// candidates from, and it holds for point reads because:
//
//   - the physical snapshot is pinned before the namespace boundary that
//     produced readTS, so nothing the snapshot holds is newer than that
//     boundary (TestTransactionSnapshotPhysicalStateDoesNotAdvancePastLogicalBoundary);
//   - a commit writes a node's body, its history record and its head in one
//     Badger commit, so a snapshot never holds a live body next to a tombstone,
//     nor a body whose head resolves to a different version;
//   - the transaction's own lifetime snapshot-reader registration already
//     protects readTS — the very version this route reads — against a prune
//     floor for as long as the snapshot is readable, so no head-floor decision
//     can differ between the two owners and no history record this read never
//     touches can disappear underneath it.
//
// Every other read keeps the logical owner: write-behind (whose heads are
// backdated to their acknowledgement), a transaction without a pinned snapshot
// (legacy/manual construction, or a read version of zero), and a read version
// that is not this namespace's begin-time boundary — a late pin binds to
// sequence zero on purpose so that namespace's commits stay post-begin, and a
// read version rewound or replaced behind the boundary is not the snapshot's
// own version.
func (tx *BadgerTransaction) readCommittedNodeAtSnapshotLocked(ctx context.Context, nodeID NodeID, vectors bool, v readVisibility) (*Node, error) {
	if tx.readsPinnedSnapshotBodyLocked() {
		return tx.readPinnedSnapshotNodeLocked(ctx, nodeID, vectors, v)
	}
	return readEntityAtWithView(ctx, tx.engine, nodeID, tx.readTS, mvccEntityRead{vectors: vectors, visibility: v}, tx.withSnapshotViewLocked, (*BadgerEngine).getNodeAtInTxn)
}

// readsPinnedSnapshotBodyLocked reports whether the transaction's pinned
// physical snapshot is the authoritative owner of every committed node body it
// holds, as readCommittedNodeAtSnapshotLocked documents. Caller must hold
// tx.mu.
func (tx *BadgerTransaction) readsPinnedSnapshotBodyLocked() bool {
	if tx.engine == nil || tx.engine.writeBehind != nil || tx.snapshotTx == nil || tx.namespace == "" {
		return false
	}
	// The namespace's begin-time boundary is what bound readTS at pin time
	// (refreshReadVersionForNamespaceLocked). Only while readTS still is that
	// boundary does the snapshot own the read: a namespace the boundary did
	// not contain, a zero read version and a version rewound or replaced
	// behind it all keep the logical owner.
	boundary, pinned := tx.beginSnapshot[tx.namespace]
	if !pinned || tx.readTS.IsZero() {
		return false
	}
	return boundary.Compare(tx.readTS) == 0
}

// readPinnedSnapshotNodeLocked reads one node body from the transaction's
// pinned physical snapshot: the body this read's version resolves to, with the
// same hydration the logical owner applies (full embeddings for a caller that
// asked for vectors, settled query metadata otherwise), the same decay
// visibility decision, and the same body-cache and read-source provenance the
// engine's point and scan reads share. Caller must hold tx.mu and
// readsPinnedSnapshotBodyLocked must hold.
func (tx *BadgerTransaction) readPinnedSnapshotNodeLocked(ctx context.Context, nodeID NodeID, vectors bool, v readVisibility) (*Node, error) {
	hydration := nodeItemQueryMetadata
	var decodes *embeddingChunkDecodes
	if vectors {
		hydration = nodeItemFullEmbeddings
		// A write target's chunks may be read again by its commit's
		// stored-entity captures, which decode through the same memo
		// (embeddingChunkDecodes).
		if tx.readingWriteTarget {
			decodes = &tx.chunkDecodes
		}
	}
	var node *Node
	err := tx.withSnapshotViewLocked(func(snapshot *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		item, getErr := snapshot.Get(nodeKey(nodeID))
		if getErr != nil {
			return getErr
		}
		decoded, readErr := tx.engine.readNodeItemInTxn(snapshot, item, nodeID, hydration, false, decodes)
		if readErr != nil {
			return readErr
		}
		// The logical owner hides an image at read time; this route answers
		// the same question at the same point so both owners return the same
		// value/error pair and the read exit keeps its own completion.
		if tx.engine.filterEntityByDecay(v, decoded, DecayScoringTime()) {
			return ErrNotFound
		}
		node = decoded
		return nil
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return node, nil
}
