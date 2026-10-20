package fabric

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/resultstream"
)

// Produce drives the distributed row pipeline without constructing constituent
// result tables. Consumers compose at APPLY/UNION boundaries; only the final
// execution consumer admits output into the request budget.
func (e *FabricExecutor) Produce(ctx context.Context, tx *FabricTransaction, fragment Fragment, params map[string]interface{}, token string, consume resultstream.Consumer) error {
	switch f := fragment.(type) {
	case *FragmentInit:
		_, err := consume(f.Columns, []any{})
		return err
	case *FragmentExec:
		loc, values, dispatch, err := e.prepareExecDispatch(ctx, tx, f, params)
		if err != nil {
			return err
		}
		dispatch = resultstream.WithConsumer(resultstream.WithDrain(dispatch, f.IsWrite || resultstream.Drain(ctx)), consume)
		switch location := loc.(type) {
		case *LocationLocal:
			_, err = e.local.ExecuteWithRecord(dispatch, location, f.Query, values, recordBindings(dispatch))
		case *LocationRemote:
			_, err = e.remote.Execute(dispatch, location, f.Query, values, token)
		default:
			err = fmt.Errorf("unsupported fragment location %T", loc)
		}
		return err
	case *FragmentUnion:
		// DISTINCT is blocking and uses the native input pipeline, whose
		// retained keys/rows are charged by the execution memory owner.
		if f.Distinct {
			input := func(ctx context.Context, consumer resultstream.Consumer) error {
				if err := e.Produce(ctx, tx, f.LHS, params, token, consumer); err != nil {
					return err
				}
				return e.Produce(ctx, tx, f.RHS, params, token, consumer)
			}
			return e.local.ProduceInput(ctx, "RETURN DISTINCT *", params, f.Columns, input, consume)
		}
		more := true
		next := func(columns []string, row []any) (bool, error) {
			var err error
			more, err = consume(columns, row)
			return more, err
		}
		if err := e.Produce(ctx, tx, f.LHS, params, token, next); err != nil {
			return err
		}
		if !more && !resultstream.Drain(ctx) {
			return nil
		}
		return e.Produce(ctx, tx, f.RHS, params, token, next)
	case *FragmentApply:
		if continuation, ok := f.Inner.(*FragmentExec); ok && f.Projection && continuation.Graph == nil {
			if _, _, _, err := e.prepareExecDispatch(ctx, tx, continuation, params); err != nil {
				return err
			}
			input := func(ctx context.Context, next resultstream.Consumer) error {
				return e.Produce(ctx, tx, f.Input, params, token, next)
			}
			return e.local.ProduceInput(ctx, continuation.Query, params, f.Input.OutputColumns(), input, consume)
		}
		parent, _ := RecordBindingsFromContext(ctx)
		return e.Produce(ctx, tx, f.Input, params, token, func(columns []string, row []any) (bool, error) {
			bindings := bindingsFromParentAndRow(parent, columns, row)
			innerCtx := WithRecordBindings(ctx, bindings)
			more := true
			err := e.Produce(innerCtx, tx, f.Inner, params, token, func(innerColumns []string, inner []any) (bool, error) {
				outColumns := f.Columns
				if len(outColumns) == 0 {
					outColumns = combineColumns(columns, innerColumns)
				}
				if f.Projection {
					outColumns = innerColumns
				}
				combiner := newCompiledRowCombiner(outColumns, buildColumnIndex(columns), buildColumnIndex(innerColumns))
				var err error
				more, err = consume(outColumns, combiner.combine(row, inner))
				return more, err
			})
			return more, err
		})
	default:
		return fmt.Errorf("unbound row fragment %T", fragment)
	}
}

func recordBindings(ctx context.Context) map[string]interface{} {
	bindings, _ := RecordBindingsFromContext(ctx)
	return bindings
}

// FragmentContainsWrite is the planner's effect classification, shared by
// statement output and distributed transaction completion.
func FragmentContainsWrite(fragment Fragment) bool { return fragmentContainsWrite(fragment) }

type inputQueryExecutor interface {
	ProduceInput(context.Context, string, map[string]interface{}, []string, resultstream.Producer, resultstream.Consumer) error
}

func (l *LocalFragmentExecutor) ProduceInput(ctx context.Context, query string, params map[string]interface{}, columns []string, input resultstream.Producer, consume resultstream.Consumer) error {
	executor, ok := l.cypherExec.(inputQueryExecutor)
	if !ok {
		return fmt.Errorf("local executor does not implement row input")
	}
	return executor.ProduceInput(ctx, query, params, columns, input, consume)
}
