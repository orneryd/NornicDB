package storage

import "context"

// CommitContextWithPublication commits atomically, then invokes afterPublication
// once on success after publication locks are released but before this commit's
// event becomes ready for the dispatcher. Wrappers finalize accounting and admission at
// this boundary. The callback must not panic. Failed commits never invoke it.
// Buffered commits invoke it at their acknowledged-publication boundary.
func (tx *BadgerTransaction) CommitContextWithPublication(ctx context.Context, afterPublication func()) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	tx.mu.Lock()
	tx.commitCtx = ctx
	tx.mu.Unlock()
	defer func() {
		tx.mu.Lock()
		tx.commitCtx = nil
		tx.mu.Unlock()
	}()
	return tx.commit(afterPublication)
}
