package cypher

import "context"

// executeCreateIndex owns RANGE and VECTOR index creation: parsing the target,
// admitting the schema change and applying the kind-specific representation.
// Explicit RANGE syntax uses its original input instead of rewriting the query.
func (e *StorageExecutor) executeCreateIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	command, err := e.prepareCreateIndex(ctx, cypher)
	if err != nil {
		return nil, err
	}
	return command.run(ctx)
}
