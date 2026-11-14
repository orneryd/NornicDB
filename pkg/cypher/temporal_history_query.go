package cypher

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var historicalQueryColumns = []ProcedureColumn{{Name: "row", Type: "MAP"}}

type historicalQueryScopeKey struct{}

// Historical executable text uses the same effect contract as ordinary
// statement admission. Nested dynamic text re-enters this boundary after its
// arguments are evaluated, before it can produce effects.
func admitHistoricalStatement(ctx context.Context, statement string) error {
	if ctx.Value(historicalQueryScopeKey{}) == nil {
		return nil
	}
	contract, err := readStatementExecutionContract(ctx, statement)
	if err != nil {
		return err
	}
	if contract.effect() == statementEffectWrite || contract.selectsDatabase || bareTransactionCommand(statement) != "" {
		return historicalReadOnlyError()
	}
	return nil
}

func historicalReadOnlyError() error {
	return newSemanticError("Neo.ClientError.Statement.AccessMode", "HistoricalReadOnly", "Historical queries must be read-only and stay in the selected database; select the database outside db.temporal.asOf")
}

func (e *StorageExecutor) callHistoricalQuery(ctx context.Context, args []interface{}) (result *ExecuteResult, retErr error) {
	at, ok := storage.CoerceTemporalTime(args[0])
	if !ok {
		return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "HistoricalTime", "db.temporal.asOf at must be a datetime")
	}
	query, err := requiredProcedureString("db.temporal.asOf", args, 1, "query")
	if err != nil {
		return nil, err
	}
	parameters, err := optionalProcedureMap("db.temporal.asOf", args, 2, "parameters")
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, historicalQueryScopeKey{}, true)
	if err = admitHistoricalStatement(ctx, query); err != nil {
		return nil, err
	}
	view, err := storage.OpenHistoricalRead(ctx, e.storage, at)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, view.Close()) }()
	queryStorage := view
	if scoped, ok := view.(interface{ Namespace() string }); ok && scoped.Namespace() != "" {
		// Reuse the normal database-ID projection; the native history owner holds
		// physical IDs, while Cypher selects and returns this database's local IDs.
		queryStorage = storage.NewNamespacedEngine(view, scoped.Namespace())
	}
	historical := e.cloneWithStorage(queryStorage, historicalReadStorage)
	historical.txContext = nil
	// The historical owner reads committed state at its own admission point,
	// without inheriting a caller's pending transaction, interval filter or params.
	ctx = context.WithValue(ctx, ctxKeyTxStorage, (*transactionStorageWrapper)(nil))
	ctx = context.WithValue(ctx, compositeStatementKey{}, (*compositeStatement)(nil))
	ctx = context.WithValue(ctx, temporalViewportKey, (*TemporalViewport)(nil))
	ctx = withQueryParams(ctx, parameters)
	inner, err := historical.executeUserStatement(ctx, query, parameters)
	if err != nil {
		return nil, fmt.Errorf("db.temporal.asOf query failed: %w", err)
	}
	return procedureRowsAsMaps(ctx, inner, historicalQueryColumns)
}
