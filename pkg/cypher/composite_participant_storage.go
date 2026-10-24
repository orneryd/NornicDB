package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Composite routing chooses a physical database before admission. The existing
// Fabric transaction remains the sole writer/terminal owner; reads reuse its
// pending participant view once a source has been opened.
func (c *cypherFabricExecutor) compositeParticipantStorage(ctx context.Context, database string, engine storage.Engine, write bool) (storage.Engine, error) {
	tx := c.fabricTransaction()
	if !write {
		c.mu.Lock()
		participant := c.localShardTxExecutors()[database]
		c.mu.Unlock()
		if participant == nil {
			return engine, nil
		}
		physical := participant.txContext.tx.(*storage.BadgerTransaction)
		return participant.transactionStorageFor(physical), nil
	}
	if err := authorizeDatabase(ctx, database, "write"); err != nil {
		return nil, err
	}
	sub, err := tx.GetOrOpen(database, true)
	if err != nil {
		return nil, err
	}
	participant, err := c.ensureLocalShardTxExecutor(ctx, sub, database, engine)
	if err != nil {
		return nil, err
	}
	physical := participant.txContext.tx.(*storage.BadgerTransaction)
	return participant.transactionStorageFor(physical), nil
}
