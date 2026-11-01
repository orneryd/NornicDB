package cypher

import "context"

func (e *StorageExecutor) executeCreateTypedPropertyIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	command, err := e.prepareCreateTypedPropertyIndex(ctx, cypher)
	if err != nil {
		return nil, err
	}
	return command.run(ctx)
}
