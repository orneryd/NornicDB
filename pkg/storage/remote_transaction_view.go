package storage

import "context"

// WithCypherTransaction borrows an already admitted participant for one
// statement. It neither begins nor completes a transaction and never closes
// the underlying transport. Storage operations use the statement's context.
func (r *RemoteEngine) WithCypherTransaction(ctx context.Context, transaction RemoteCypherTx) *RemoteEngine {
	view := *r
	view.transport = &remoteTransactionTransport{ctx: ctx, transaction: transaction}
	return &view
}

type remoteTransactionTransport struct {
	ctx         context.Context
	transaction RemoteCypherTx
}

func (t *remoteTransactionTransport) query(_ context.Context, statement string, params map[string]interface{}) ([][]interface{}, error) {
	_, rows, err := t.transaction.QueryCypher(t.ctx, statement, params)
	return rows, err
}

func (t *remoteTransactionTransport) queryWithColumns(_ context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	return t.transaction.QueryCypher(t.ctx, statement, params)
}

func (t *remoteTransactionTransport) queryBatch(_ context.Context, statements []remoteStatement) error {
	for _, statement := range statements {
		if _, _, err := t.transaction.QueryCypher(t.ctx, statement.Statement, statement.Parameters); err != nil {
			return err
		}
	}
	return nil
}

func (*remoteTransactionTransport) beginCypherTx(context.Context) (RemoteCypherTx, error) {
	return nil, ErrNotImplemented
}

func (*remoteTransactionTransport) close() error { return nil }
