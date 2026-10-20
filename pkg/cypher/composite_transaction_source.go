package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// A logical coordinator borrows physical participants. A caller's storage
// wrapper must not replace that logical view; all other request values and the
// Fabric transaction/periodic ownership marker remain on the context.
type compositeStatementContext struct{ context.Context }

func (ctx compositeStatementContext) Value(key interface{}) interface{} {
	if key == ctxKeyTxStorage {
		return nil
	}
	return ctx.Context.Value(key)
}

func (c *cypherFabricExecutor) logicalCompositeExecutor(ctx context.Context, database string, engine storage.Engine) (*StorageExecutor, error) {
	composite, ok := engine.(*storage.CompositeEngine)
	if !ok {
		return nil, localizedError(localization.CypherCommandRoutingFabricStorageTypeInvalid(database), nil)
	}
	ctx = compositeStatementContext{Context: ctx}
	view := composite.WithConstituentScope(func(alias string, source storage.Engine, write bool) (storage.Engine, error) {
		selected := database + "." + alias
		if err := c.base.authorizeDatabaseRequirements(ctx, selected, PermissionRequirements{Read: !write, Write: write}, false); err != nil {
			return nil, err
		}
		location, err := c.catalog.Resolve(selected)
		if err != nil {
			return nil, err
		}
		transaction := c.fabricTransaction()
		participant, err := transaction.GetOrOpenLocation(location, write)
		if err != nil {
			return nil, err
		}
		physicalCtx := fabric.WithSubTransaction(fabric.WithFabricTransaction(ctx, transaction), participant)
		if remote, ok := location.(*fabric.LocationRemote); ok {
			return c.remote.StatementStorage(physicalCtx, remote, c.authToken)
		}
		executor, err := c.ensureLocalShardTxExecutor(physicalCtx, participant, location.DatabaseName(), source)
		if err != nil {
			return nil, err
		}
		tx, ok := executor.txContext.tx.(*storage.BadgerTransaction)
		if !ok {
			return nil, localizedError(localization.CypherTransactionsUnknownType(), nil)
		}
		return executor.transactionStorageFor(tx), nil
	})
	logical := c.base.cloneForStorage(view)
	// Only the outer Fabric transaction owns completion. Retaining the parent's
	// active transaction here would recursively redispatch a logical statement.
	logical.txContext = nil
	return logical, nil
}
