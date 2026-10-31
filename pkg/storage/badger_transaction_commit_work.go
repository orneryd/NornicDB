// Author: Claude, NOR-180 the observation of a transaction's commit work, 2026-10-31.
package storage

// CommitWorkMark observes what this transaction's COMMIT would write, wherever that
// work is staged: its published fields, its metadata, and the dictionary and pending
// state its collaborators hold under its native handle. Two equal marks mean no commit
// work was added or changed between them, apart from a hash collision (one in 2^64 for
// differing states). A mark is 0 until work is added beyond what opening the transaction
// set (MarkCommitWorkOpened).
//
// The lengths see any work added on any route, with no call site involved. They can't
// see a change in place, such as a replaced pending image; effects, which the mark
// also reads, sees those at the write-claim owner and the staged writer.
// TestCommitWorkMarkReadsEveryDurableInput holds the reads to the field and
// collaborator tables; it doesn't prove those tables are all COMMIT reads.
func (tx *BadgerTransaction) CommitWorkMark() uint64 {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	mark := tx.commitWorkHashLocked()
	if mark == tx.openedWork {
		return 0
	}
	return mark
}

// MarkCommitWorkOpened ends the opening of the transaction: work set so far, a client's
// metadata at BEGIN, belongs to no statement.
func (tx *BadgerTransaction) MarkCommitWorkOpened() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.openedWork = tx.commitWorkHashLocked()
}

// commitWorkHashLocked is 0 for a transaction with no commit work. Each input keeps its
// position, so work moving between inputs changes the hash.
func (tx *BadgerTransaction) commitWorkHashLocked() uint64 {
	var hash uint64
	mix := func(value uint64) { hash = (hash ^ value) * 0x100000001b3 }
	present := func(yes bool) {
		if yes {
			mix(1)
		} else {
			mix(0)
		}
	}
	mix(tx.effects.Load())
	mix(uint64(len(tx.operations)))
	mix(uint64(len(tx.connectedDeletes)))
	mix(uint64(len(tx.pendingWrites)))
	mix(uint64(len(tx.pendingDeletes)))
	mix(uint64(len(tx.pendingNodes)))
	mix(uint64(len(tx.pendingEdges)))
	mix(uint64(len(tx.deletedNodes)))
	mix(uint64(len(tx.deletedEdges)))
	mix(uint64(len(tx.deferredEdgeWrites)))
	mix(uint64(len(tx.cancelledEdgeWrites)))
	mix(uint64(len(tx.edgeCreateNoOps)))
	mix(uint64(len(tx.droppedEmbeddings)))
	mix(uint64(len(tx.pendingLabelCountDeltas)))
	mix(uint64(len(tx.pendingEdgeTypeCountDeltas)))
	mix(uint64(len(tx.pendingEdgeTypeLabelCountDeltas)))
	mix(uint64(len(tx.Metadata)))
	present(tx.knowledgeSchemaDefinition != nil)
	present(tx.knowledgeSchemaDirty)
	present(tx.schemaRuntime != nil)
	present(tx.preparedEntityCommit != nil)
	if tx.engine != nil && tx.badgerTx != nil {
		if tx.engine.idDict != nil {
			mix(tx.engine.idDict.txnWork(tx.badgerTx))
		}
		if tx.engine.propKeyDict != nil {
			mix(tx.engine.propKeyDict.txnWork(tx.badgerTx))
		}
		if journal, ok := tx.engine.pendingEmbeddingAdds.Load(tx.badgerTx); ok {
			mix(uint64(len(journal.(map[NodeID]struct{}))))
		}
	}
	return hash
}
