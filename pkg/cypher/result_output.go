package cypher

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/orneryd/nornicdb/pkg/resultstream"
)

// WithResultBudget makes output retention a property of execution. Byte cost
// includes a JSON row and its quoted JSON copy; transports reserve envelope
// space separately and retain responsibility for final wire encoding.
func WithResultBudget(ctx context.Context, rows, bytes int) context.Context {
	budget := resultstream.NewBudget(rows, bytes, func(row []any, remaining int) ([]any, int, error) {
		converted, err := JSONTransportValue(row, remaining)
		if errors.Is(err, ErrJSONValueTooLarge) {
			return nil, 0, resultstream.ErrValueTooLarge
		}
		if err != nil {
			return nil, 0, err
		}
		encoded, err := json.Marshal(converted)
		if err != nil {
			return nil, 0, err
		}
		quoted, err := json.Marshal(string(encoded))
		return converted.([]any), len(encoded) + len(quoted), err
	})
	return resultstream.WithConsumer(resultstream.WithBudget(ctx, budget), budget.Consume)
}

// consumeResultRows is the common source handoff for statements, procedures
// and fragment adapters. A source is consumed while its transaction is active.
func consumeResultRows(ctx context.Context, result *ExecuteResult, consumer resultstream.Consumer) error {
	if result == nil {
		return nil
	}
	if result.Source != nil {
		produce := result.Source
		result.Source = nil
		return produce(ctx, consumer)
	}
	for _, row := range result.Rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		more, err := consumer(result.Columns, row)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

func materializeResultSource(ctx context.Context, result *ExecuteResult) error {
	if result == nil || result.Source == nil {
		return nil
	}
	retention := newMemoryRetention(ctx)
	defer retention.close()
	return consumeResultRows(ctx, result, func(columns []string, row []any) (bool, error) {
		result.Columns = columns
		if err := retention.reserveResultRow(row); err != nil {
			return false, err
		}
		result.Rows = append(result.Rows, row)
		return true, nil
	})
}

func (s *ResultStream) consumeResult(ctx context.Context, result *ExecuteResult) error {
	if result == nil || s.streamed() {
		return nil
	}
	s.begin(result.Columns)
	err := consumeResultRows(ctx, result, func(columns []string, row []any) (bool, error) {
		s.columns = columns
		more := s.emit(ctx, row)
		return more, s.consumeErr
	})
	result.Rows = nil
	return err
}

func addOutputRestore(stream *ResultStream, restore func(*ExecuteResult) *ExecuteResult) {
	if !stream.bounded() {
		return
	}
	previous := stream.restoreRow
	stream.restoreRow = func(result *ExecuteResult) *ExecuteResult {
		result = restore(result)
		if previous != nil {
			result = previous(result)
		}
		return result
	}
}

func prepareOutputStream(ctx context.Context, stream *ResultStream, statement string, maxResults int64) context.Context {
	if !stream.bounded() {
		return ctx
	}
	permissions := QueryPermissionRequirements(statement)
	stream.drain = permissions.Write || permissions.Schema || permissions.Admin
	// A dynamic CALL may perform effects in later input rows. Its own inner
	// read frame stops when the shared final budget fills; write frames drain.
	stream.drain = stream.drain || !cacheableDynamicStatements(statement, 0)
	stream.rowLimit = maxResults
	return armResultStream(ctx, stream, statement, nil, nil)
}

func resultRowsSource(ctx context.Context, result *ExecuteResult) pipelineRowSource {
	return func(yield func(pipelineRow) bool) bool {
		err := consumeResultRows(ctx, result, func(columns []string, values []any) (bool, error) {
			row := make(pipelineRow, len(columns))
			for i, column := range columns {
				if i < len(values) {
					row[column] = values[i]
				}
			}
			return yield(row), nil
		})
		if err != nil {
			recordExpressionFailure(ctx, err)
			return false
		}
		return true
	}
}

func (e *StorageExecutor) executeSourceTail(ctx context.Context, seed *ExecuteResult, tail string) (*ExecuteResult, error) {
	clauses, ok := pipelineClausesFor(tail)
	if !ok {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", "unsupported procedure result tail")
	}
	if parent, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream); parent.bounded() {
		child := NewBoundedResultStream(parent.consume)
		child.restoreRow = parent.restoreRow
		ctx = prepareOutputStream(ctx, child, tail, parent.rowLimit)
		bindResultStream(ctx, tail, clauses)
	}
	scope := make(map[string]struct{}, len(seed.Columns))
	for _, column := range seed.Columns {
		scope[column] = struct{}{}
	}
	result, handled, err := e.runPipelineClauseRows(ctx, nil, scope, clauses, clauses, nil, resultRowsSource(ctx, seed))
	if !handled && err == nil {
		err = newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", "procedure result pipeline declined")
	}
	if result != nil {
		result.Stats = seed.Stats
		if result.Metadata == nil {
			result.Metadata = seed.Metadata
		}
	}
	return result, err
}

func finishProcedureSource(ctx context.Context, result *ExecuteResult) (*ExecuteResult, error) {
	consumer := resultstream.ConsumerFromContext(ctx)
	if consumer == nil {
		if stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream); stream.bounded() && stream.frame == nil {
			stream.begin(result.Columns)
			consumer = func(columns []string, row []any) (bool, error) {
				stream.columns = columns
				return stream.emit(ctx, row), stream.consumeErr
			}
		}
	}
	if consumer != nil {
		err := consumeResultRows(ctx, result, consumer)
		result.Rows = nil
		return result, err
	}
	return result, materializeResultSource(ctx, result)
}

// indexedOutput keeps procedure row formatting lazy. Algorithm/retrieval
// working state remains with its native owner; this avoids a second output
// table before YIELD/filter/projection and final admission.
func indexedOutput(ctx context.Context, columns []string, count int, row func(int) []any) (*ExecuteResult, error) {
	result := &ExecuteResult{Columns: columns}
	result.Source = func(ctx context.Context, consume resultstream.Consumer) error {
		for i := 0; i < count; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			values := row(i)
			if values == nil {
				continue
			}
			more, err := consume(columns, values)
			if err != nil {
				return err
			}
			if !more {
				return nil
			}
		}
		return nil
	}
	if resultstream.BudgetFromContext(ctx) == nil && resultstream.ConsumerFromContext(ctx) == nil {
		if err := materializeResultSource(ctx, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}
