package cypher

import "context"

func (e *StorageExecutor) pipelineCallCanStream(ctx context.Context, clause pipelineClause) bool {
	if !hasOutputStream(ctx) {
		return false
	}
	switch clause.kind {
	case pipelineClauseCall:
		return true
	case pipelineClauseCallSubquery:
		return true
	default:
		return false
	}
}

// pipelineCallSource composes the native CALL operator with the row pipeline.
// Scope/import validation and transaction semantics remain with that operator.
func (e *StorageExecutor) pipelineCallSource(ctx context.Context, input pipelineRowSource, clause string, scope map[string]struct{}, optional bool, stats *QueryStats) (pipelineRowSource, []string, error) {
	metadata := &pipelineCallMetadata{scope: scope}
	if _, _, _, err := e.pipelineApplyCallSubqueryWithMetadata(ctx, nil, clause, metadata); err != nil {
		return nil, nil, err
	}
	columns := append([]string(nil), metadata.columns...)
	if parsed := e.callSubqueryParseFor(ctx, clause); parsed.inTransactions {
		return e.pipelineBatchCallSource(ctx, input, clause, metadata, columns, optional, stats, parsed.batchSize), columns, nil
	}
	return func(yield func(pipelineRow) bool) bool {
		retention := newMemoryRetention(ctx)
		defer retention.release()
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

// pipelineBatchCallSource keeps only the native batch's input. The existing
// operator owns each transaction and commit; its rows feed the same consumer
// within that lifetime. Output exhaustion never skips remaining write effects.
func (e *StorageExecutor) pipelineBatchCallSource(ctx context.Context, input pipelineRowSource, clause string, metadata *pipelineCallMetadata, columns []string, optional bool, stats *QueryStats, batchSize int) pipelineRowSource {
	if batchSize <= 0 {
		batchSize = 1000
	}
	// The native optional operator invokes the call once per input row.
	if optional {
		batchSize = 1
	}
	return func(yield func(pipelineRow) bool) bool {
		retention := newMemoryRetention(ctx)
		defer func() { retention.release() }()
		var batch []pipelineRow
		more, valid, emitted := true, true, false
		metadata.consume = func(row pipelineRow) bool { emitted = true; more = yield(row); return more }
		flush := func() bool {
			if len(batch) == 0 {
				return more
			}
			emitted = false
			_, innerStats, handled, err := e.pipelineApplyCallSubqueryWithMetadata(ctx, batch, clause, metadata)
			addQueryStats(stats, innerStats)
			if err != nil || !handled {
				if err == nil {
					err = newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "CALL batch source declined")
				}
				recordExpressionFailure(ctx, err)
				valid = false
				return false
			}
			if optional && !emitted {
				row := mergeCallPipelineRows(batch[0], nil)
				for _, column := range columns {
					row[column] = nil
				}
				more = yield(row)
			}
			batch = nil
			retention.release()
			retention = newMemoryRetention(ctx)
			return more
		}
		completed := input(func(row pipelineRow) bool {
			if err := retention.reserveMergedRow(nil, row); err != nil {
				recordExpressionFailure(ctx, err)
				valid = false
				return false
			}
			batch = append(batch, row)
			return len(batch) < batchSize || flush()
		})
		if completed && valid && more {
			flush()
		}
		return completed && valid
	}
}
