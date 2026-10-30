package cypher

import "context"

// validateProjectionSemantics is the isolated-query compatibility entry. It
// consumes the canonical admission result; execution has no sibling seen-set,
// alias namespace, aggregation membership scan or declaration legality pass.
func validateProjectionSemantics(ctx context.Context, source string) error {
	_, err := (&StorageExecutor{}).admitBoundStatement(ctx, source)
	return err
}
