package cypher

import "context"

// pipelineCallSource composes the native CALL operator with the row pipeline.
// Scope/import validation and transaction semantics remain with that operator.
func (e *StorageExecutor) pipelineCallSource(ctx context.Context, input pipelineRowSource, clause string, scope map[string]struct{}, optional bool, stats *QueryStats) (pipelineRowSource, []string, error) {
	metadata := &pipelineCallMetadata{scope: scope}
	if _, _, _, err := e.pipelineApplyCallSubqueryWithMetadata(ctx, nil, clause, metadata); err != nil {
		return nil, nil, err
	}
	columns := append([]string(nil), metadata.columns...)
	return func(yield func(pipelineRow) bool) bool {
		retention := newMemoryRetention(ctx)
		defer retention.close()
		var cached []pipelineRow
		cachedRead, valid := false, true
		completed := input(func(outer pipelineRow) bool {
			more, emitted := true, false
			emit := func(row pipelineRow) bool {
				emitted = true
				more = yield(row)
				return more
			}
			if cachedRead {
				for _, row := range cached {
					if !emit(mergeCallPipelineRows(outer, row)) {
						break
					}
				}
			} else {
				metadata.consume = func(row pipelineRow) bool {
					if metadata.independentRead {
						inner := make(pipelineRow, len(columns))
						for _, name := range columns {
							inner[name] = row[name]
						}
						if err := retention.reserveMergedRow(nil, inner); err != nil {
							recordExpressionFailure(ctx, err)
							valid = false
							return false
						}
						cached = append(cached, inner)
					}
					return emit(row)
				}
				_, innerStats, handled, err := e.pipelineApplyCallSubqueryWithMetadata(ctx, []pipelineRow{outer}, clause, metadata)
				addQueryStats(stats, innerStats)
				if err != nil || !handled {
					if err == nil {
						err = newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "CALL source declined")
					}
					recordExpressionFailure(ctx, err)
					valid = false
					return false
				}
				cachedRead = metadata.independentRead
			}
			if optional && !emitted {
				row := mergeCallPipelineRows(outer, nil)
				for _, column := range columns {
					row[column] = nil
				}
				more = yield(row)
			}
			return more && valid
		})
		return completed && valid
	}, columns, nil
}
