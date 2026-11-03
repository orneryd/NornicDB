package storage

import "context"

// BeginSchemaTransaction admits an autocommit schema statement before opening
// its snapshot through begin. The caller supplies the live namespace manager
// and its existing transaction opener, preserving wrapper admission. The native
// transaction owns this admission until publication or rollback; callers must
// roll back any transaction they do not commit, including rejected commits.
// The opener must return a new active transaction, as BeginTransaction does.
// Explicit transactions must use their ordinary opener to retain their snapshot.
//
// For example, tx, err := schema.BeginSchemaTransaction(ctx, engine.BeginTransaction)
// opens a schema statement whose peer cannot snapshot an unpublished definition.
func (sm *SchemaManager) BeginSchemaTransaction(ctx context.Context, begin func(context.Context) (*BadgerTransaction, error)) (*BadgerTransaction, error) {
	if err := sm.schemaMutationMu.LockContext(ctx); err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			sm.schemaMutationMu.Unlock()
		}
	}()
	tx, err := begin(ctx)
	if err != nil {
		return nil, err
	}
	tx.mu.Lock()
	tx.schemaAdmission = sm
	tx.mu.Unlock()
	transferred = true
	return tx, nil
}
