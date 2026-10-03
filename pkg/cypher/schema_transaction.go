package cypher

func isOrdinarySchemaDDL(query string) bool {
	for _, command := range []string{"INDEX", "RANGE INDEX", "TEXT INDEX", "POINT INDEX", "VECTOR INDEX", "FULLTEXT INDEX", "LOOKUP INDEX", "CONSTRAINT"} {
		if startsWithKeywords(query, "CREATE", command) {
			return true
		}
	}
	return startsWithKeywords(query, "DROP", "INDEX") || startsWithKeywords(query, "DROP", "CONSTRAINT")
}

func (e *StorageExecutor) prepareSchemaTransaction() error {
	wrapper, ok := e.storage.(*transactionStorageWrapper)
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

func (e *StorageExecutor) afterSchemaCommit(action func()) {
	if wrapper, ok := e.storage.(*transactionStorageWrapper); ok && wrapper.schema != nil {
		wrapper.schemaCommitActions = append(wrapper.schemaCommitActions, action)
		return
	}
	action()
}
