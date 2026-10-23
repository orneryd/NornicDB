package storage

import "github.com/dgraph-io/badger/v4"

// admitDurableWrite is the issuer's runtime admission. An undeclared writer is
// refused before its callback, tokens, intent, or entity data can be committed.
// Every physical transaction also passes this owner, including restore batches.
func admitDurableWrite(scope nodeCacheWriteScope) error {
	if !scope.declared {
		return ErrInvalidData
	}
	return nil
}

func commitDurableWrite(scope nodeCacheWriteScope, txn *badger.Txn, ts uint64) error {
	if err := admitDurableWrite(scope); err != nil {
		return err
	}
	return txn.CommitAt(ts, nil)
}

// issueNodeCacheCommit is shared by ordinary, large and maintenance commits.
// Eligibility follows durable order even when a writer has no warm callback.
func issueNodeCacheCommit(engine *BadgerEngine, ts uint64, token []uint64, scope nodeCacheWriteScope) {
	if engine == nil {
		return
	}
	generation := engine.cacheCommitNodeWrites(ts, scope)
	if len(token) == 1 {
		token[0] = generation
	}
}

// metadataCommitCheck is installed only by storage tests. It verifies the
// actual write set of the explicit dictionary/count/sequence-only exemption.
var metadataCommitCheck func(*badger.Txn) error

var nodeCacheScopeCheck func(*badger.Txn, nodeCacheWriteScope) error

// updateBadgerMetadata owns the dictionary's managed and offline raw paths.
// Known writers never select the arbitrary Update adapter themselves.
func updateBadgerMetadata(db badgerKV, fn func(*badger.Txn) error) error {
	if managed, ok := db.(*managedBadgerDB); ok {
		return managed.updateMetadata(fn)
	}
	return db.Update(func(txn *badger.Txn) error {
		if err := fn(txn); err != nil {
			return err
		}
		if metadataCommitCheck != nil {
			return metadataCommitCheck(txn)
		}
		return nil
	})
}

func (m *managedBadgerDB) commitMetadata(txn *badger.Txn) error {
	return m.commitWithNodeCache(txn, nil, nil, noNodeCacheWrites())
}

func (m *managedBadgerDB) updateMetadata(fn func(*badger.Txn) error) error {
	if m.DB.IsClosed() {
		return badger.ErrDBClosed
	}
	txn, readTs, err := m.beginTxn(true)
	if err != nil {
		return err
	}
	defer m.endRead(readTs)
	defer txn.Discard()
	if err := fn(txn); err != nil {
		return err
	}
	return m.commitMetadata(txn)
}

func (b *BadgerEngine) withMetadataUpdate(fn func(*badger.Txn) error) error {
	db, err := b.beginHelperTxn()
	if err != nil {
		return err
	}
	defer b.txnWG.Done()
	return recoverBadgerClosedPanic(func() error { return db.updateMetadata(fn) })
}

// metadataBatches persists property-key dictionary tokens before entity data.
type metadataBatches struct{ db *managedBadgerDB }

func (m metadataBatches) openBatch() *badger.Txn { return m.db.openBatch() }
func (m metadataBatches) commitBatch(txn *badger.Txn) error {
	return m.db.commitMetadata(txn)
}

// DropAll shadows Badger's physical restore/reset path, like DropPrefix.
func (m *managedBadgerDB) DropAll() error {
	m.commitGate.Lock()
	defer m.commitGate.Unlock()
	err := m.DB.DropAll()
	cacheErr := m.revokeMaintenancePublishers(unknownNodeCacheWrites(nodeCacheUnknownMaintenance), 0)
	if err != nil {
		return err
	}
	return cacheErr
}

// Prefix drops and backup loads hold commitGate exclusively. Give their
// physical mutations a fresh timestamp before publishing the maintenance tail.
func (m *managedBadgerDB) revokeMaintenancePublishers(scope nodeCacheWriteScope, after uint64) error {
	if m.cacheOwner == nil {
		m.oracle.advanceTo(after)
		return nil
	}
	m.oracle.advanceTo(after, func(ts uint64) {
		issueNodeCacheCommit(m.cacheOwner, ts, nil, scope)
	})
	return m.oracle.failure()
}
