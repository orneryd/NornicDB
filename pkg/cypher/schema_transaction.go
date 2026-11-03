package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// isOrdinarySchemaDDL reports whether query declares or removes a schema index
// or constraint: the ordinary schema DDL whose effects belong to the schema
// view of the statement's transaction (prepareSchemaTransaction). It is the one
// classifier both the statement contract (readStatementExecutionContract) and
// the executor's autocommit routing read, so a statement can never be a schema
// write for one and not for the other. Catalog commands (CREATE / DROP
// DATABASE, ALIAS, ...) and schema introspection keep their own owners and are
// not ordinary schema DDL.
func isOrdinarySchemaDDL(ctx context.Context, query string) bool {
	for _, command := range []string{"INDEX", "RANGE INDEX", "TEXT INDEX", "POINT INDEX", "VECTOR INDEX", "FULLTEXT INDEX", "LOOKUP INDEX", "CONSTRAINT"} {
		if startsWithKeywords(ctx, query, "CREATE", command) {
			return true
		}
	}
	return startsWithKeywords(ctx, query, "DROP", "INDEX") || startsWithKeywords(ctx, query, "DROP", "CONSTRAINT")
}

// prepareSchemaTransaction gives the statement its transaction's isolated
// schema view. Ordinary DDL applies and fills its change on that view; the
// statement's commit publishes the definition and the runtime values together
// (BadgerTransaction.StageSchemaChanges), so readers see the complete old or
// new schema and never a declared but unfilled index. Without a transaction
// (a storage engine that cannot stage schema changes) the live schema manager
// keeps its existing direct behavior.
func (e *StorageExecutor) prepareSchemaTransaction(ctx context.Context) error {
	wrapper, ok := e.getStorage(ctx).(*transactionStorageWrapper)
	if !ok {
		return nil
	}
	if err := wrapper.tx.SetNamespace(e.databaseName()); err != nil {
		return err
	}
	view, err := wrapper.tx.Schema(storage.OrdinarySchemaChanges)
	if err != nil {
		return err
	}
	wrapper.schema = view
	return nil
}

// afterCommit runs action under the lifetime that actually executes it.
// Without an explicit transaction it runs now, under the caller's context.
// Inside one it runs when the transaction commits, under that commit's admitted
// context, so an accepted metadata side effect never borrows the finished
// statement that queued it.
func (e *StorageExecutor) afterCommit(ctx context.Context, action func(context.Context) error) error {
	if wrapper := e.membership(ctx).storage; wrapper != nil {
		wrapper.commitActions = append(wrapper.commitActions, action)
		return nil
	}
	return action(ctx)
}

func (e *StorageExecutor) mutateSchema(ctx context.Context, mutation func(*storage.SchemaManager) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.prepareSchemaTransaction(ctx); err != nil {
		return err
	}
	schema := e.getStorage(ctx).GetSchema()
	release := schema.LockSchemaMutation()
	defer release()
	if err := mutation(schema); err != nil {
		return err
	}
	if wrapper, ok := e.getStorage(ctx).(*transactionStorageWrapper); ok {
		return wrapper.tx.StageSchemaChanges()
	}
	return nil
}
