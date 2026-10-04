package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func isOrdinarySchemaDDL(query string) bool {
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
	if err := wrapper.tx.SetNamespace(e.currentDatabaseName()); err != nil {
		return err
	}
	view, err := wrapper.tx.Schema()
	if err != nil {
		return err
	}
	wrapper.schema = view
	return nil
}

func (e *StorageExecutor) afterSchemaCommit(ctx context.Context, action func()) {
	if wrapper, ok := e.getStorage(ctx).(*transactionStorageWrapper); ok && wrapper.schema != nil {
		wrapper.schemaCommitActions = append(wrapper.schemaCommitActions, action)
		return
	}
	action()
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
