package cypher

// relationshipEntityExecution is the storage-only capability for explicit
// entities, metadata and algorithms. It has no parsed traversal/path kernels.
// Admitted execution embeds its primitive reader without enlarging the frame.
type relationshipEntityExecution struct {
	executor *StorageExecutor
}

func (e *StorageExecutor) relationshipEntityExecution() relationshipEntityExecution {
	return relationshipEntityExecution{executor: e}
}
