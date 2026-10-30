package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func isOrdinarySchemaDDL(query braceView) bool {
	for _, command := range []string{"INDEX", "RANGE INDEX", "TEXT INDEX", "POINT INDEX", "VECTOR INDEX", "FULLTEXT INDEX", "LOOKUP INDEX", "CONSTRAINT"} {
		if startsWithKeywords(query, "CREATE", command) {
			return true
		}
	}
	return startsWithKeywords(query, "DROP", "INDEX") || startsWithKeywords(query, "DROP", "CONSTRAINT")
}

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

func (e *StorageExecutor) afterCommit(ctx context.Context, action func() error) error {
	if wrapper := e.membership(ctx).storage; wrapper != nil {
		wrapper.commitActions = append(wrapper.commitActions, action)
		return nil
	}
	return action()
}

func (e *StorageExecutor) mutateSchema(ctx context.Context, mutation func(*storage.SchemaManager) error) error {
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
