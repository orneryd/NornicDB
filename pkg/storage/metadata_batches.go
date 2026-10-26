package storage

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

// metadataBatches declares the property dictionary's empty entity/cache scope.
// Preparation retains the commit context but clears the outer semantic decision:
// making tokens durable does not acknowledge the entity transaction.
type metadataBatches struct {
	db  *managedBadgerDB
	ctx context.Context
	p   *readerPublication
}

func (target metadataBatches) openBatch() *badger.Txn { return target.db.openBatch() }
func (target metadataBatches) commitBatch(txn *badger.Txn) error {
	ts, err := target.db.commitShared(target.ctx, txn, target.p, &commitPublication{metadata: true})
	if err != nil {
		return err
	}
	return target.db.oracle.waitPublished(target.ctx, ts)
}
