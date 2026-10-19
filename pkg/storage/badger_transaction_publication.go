package storage

import "context"

// CommitContextWithPublication commits atomically, then invokes afterPublication
// once on success after publication locks are released but before synchronous
// listeners run. Database wrappers finalize accounting and release admission at
// this boundary. The callback must not panic. Failed commits never invoke it.
// Buffered commits invoke it at their acknowledged-publication boundary.
func (tx *BadgerTransaction) CommitContextWithPublication(ctx context.Context, afterPublication func()) error {
	if ctx != nil {
		if after := CommandAfterPublicationFromContext(ctx); after != nil {
			tx.commandAfterPublication = after
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	tx.mu.Lock()
	tx.commitCtx = ctx
	if ctx != nil {
		if token := CommandVersionFromContext(ctx); token != ([16]byte{}) {
			tx.commandVersion = token
		}
	}
	if ctx != nil {
		if complete := CommandPublicationFromContext(ctx); complete != nil {
			previous := tx.commandPublication
			tx.commandPublication = func() {
				if previous != nil {
					previous()
				}
				complete()
			}
		}
	}
	tx.mu.Unlock()
	defer func() {
		tx.mu.Lock()
		tx.commitCtx = nil
		tx.mu.Unlock()
	}()
	return tx.commit(afterPublication)
}
