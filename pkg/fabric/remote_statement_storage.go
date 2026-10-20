package fabric

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// StatementStorage uses the exact remote participant that executes fragments.
// Its completion remains bound once by getOrCreateTxHandle; the borrowed view
// only supplies Engine operations with the current statement's deadline.
func (r *RemoteFragmentExecutor) StatementStorage(ctx context.Context, location *LocationRemote, token string) (storage.Engine, error) {
	engine, err := r.getOrCreateEngine(location, token)
	if err != nil {
		return nil, err
	}
	sub, ok := SubTransactionFromContext(ctx)
	if !ok || sub == nil {
		return nil, storage.ErrInvalidData
	}
	participant, err := r.getOrCreateTxHandle(ctx, sub, engine)
	if err != nil {
		return nil, err
	}
	return engine.WithCypherTransaction(ctx, participant.tx), nil
}
