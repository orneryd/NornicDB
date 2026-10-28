package fabric

import (
	"context"
	"fmt"
	"sync"
)

// executeUnion executes both branches and merges results.
func (e *FabricExecutor) executeUnion(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
	cols, it, err := e.executeUnionRows(ctx, tx, f, params, authToken)
	if err != nil {
		return nil, err
	}
	return materializeIterator(cols, it)
}

func (e *FabricExecutor) executeUnionRows(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) ([]string, RowIterator, error) {
	// Keep write branch execution sequential so shard write-routing remains deterministic.
	// Read-only UNION branches execute concurrently and are merged in LHS/RHS order.
	if e.serializeExec || fragmentContainsWrite(f.LHS) || fragmentContainsWrite(f.RHS) {
		lhsCols, lhsIt, err := e.executeRows(ctx, tx, f.LHS, params, authToken)
		if err != nil {
			return nil, nil, fmt.Errorf("union LHS failed: %w", err)
		}
		rhsCols, rhsIt, err := e.executeRows(ctx, tx, f.RHS, params, authToken)
		if err != nil {
			_ = lhsIt.Close()
			return nil, nil, fmt.Errorf("union RHS failed: %w", err)
		}
		return e.mergeUnion(f, lhsCols, lhsIt, rhsCols, rhsIt)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		lhsCols []string
		lhsIt   RowIterator
		rhsCols []string
		rhsIt   RowIterator
		err     error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cols, it, runErr := e.executeRows(ctx, tx, f.LHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			err = fmt.Errorf("union LHS failed: %w", runErr)
			return
		}
		lhsCols = cols
		lhsIt = it
	}()
	go func() {
		defer wg.Done()
		cols, it, runErr := e.executeRows(ctx, tx, f.RHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			if err == nil {
				err = fmt.Errorf("union RHS failed: %w", runErr)
			}
			return
		}
		rhsCols = cols
		rhsIt = it
	}()
	wg.Wait()
	if err != nil {
		if lhsIt != nil {
			_ = lhsIt.Close()
		}
		if rhsIt != nil {
			_ = rhsIt.Close()
		}
		return nil, nil, err
	}
	return e.mergeUnion(f, lhsCols, lhsIt, rhsCols, rhsIt)
}

// mergeUnion combines a UNION's branch streams; every UNION route ends here.
// A branch without column metadata returned no rows and has nothing to match,
// and the pipeline's row wrapper column is resolved after execution.
func (e *FabricExecutor) mergeUnion(f *FragmentUnion, lhsCols []string, lhs RowIterator, rhsCols []string, rhs RowIterator) ([]string, RowIterator, error) {
	lhsWrapper, rhsWrapper := isRowWrapperRows(lhs), isRowWrapperRows(rhs)
	if lhsCols == nil && f.LHS != nil && !lhsWrapper {
		lhsCols = f.LHS.OutputColumns()
	}
	if rhsCols == nil && f.RHS != nil && !rhsWrapper {
		rhsCols = f.RHS.OutputColumns()
	}
	cols, wrapper := f.Columns, false
	if len(cols) == 0 {
		cols, wrapper = lhsCols, lhsWrapper
		if len(cols) == 0 {
			cols, wrapper = rhsCols, rhsWrapper
		}
	}
	align := func(branch []string, rows RowIterator) (RowIterator, error) {
		if e.union.ColumnOrder == nil || branch == nil || lhsWrapper || rhsWrapper {
			return rows, nil
		}
		order, same := e.union.ColumnOrder(cols, branch)
		if !same {
			return nil, e.union.ColumnsError(cols, branch)
		}
		if order == nil {
			return rows, nil
		}
		return NewConvertingRowIterator(rows, func(row []interface{}) []interface{} {
			values := make([]interface{}, len(order))
			for column, position := range order {
				if position < len(row) {
					values[column] = row[position]
				}
			}
			return values
		}), nil
	}
	left, err := align(lhsCols, lhs)
	var right RowIterator
	if err == nil {
		right, err = align(rhsCols, rhs)
	}
	if err != nil {
		if lhs != nil {
			_ = lhs.Close()
		}
		if rhs != nil {
			_ = rhs.Close()
		}
		return nil, nil, err
	}
	lhs, rhs = left, right
	out := NewConcatRowIterator(lhs, rhs)
	if f.Distinct {
		distinct := NewDistinctRowIterator(out)
		if e.union.NewDistinct != nil {
			distinct.(*distinctRowIterator).added = e.union.NewDistinct()
		}
		out = distinct
	}
	if wrapper {
		out = rowWrapperRows{out}
	}
	return cols, out, nil
}

// executeUnionSequential preserves direct test-call compatibility while using
// the iterator-based UNION execution path.
func (e *FabricExecutor) executeUnionSequential(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
	lhsCols, lhsIt, err := e.executeRows(ctx, tx, f.LHS, params, authToken)
	if err != nil {
		return nil, fmt.Errorf("union LHS failed: %w", err)
	}
	rhsCols, rhsIt, err := e.executeRows(ctx, tx, f.RHS, params, authToken)
	if err != nil {
		_ = lhsIt.Close()
		return nil, fmt.Errorf("union RHS failed: %w", err)
	}
	cols, out, err := e.mergeUnion(f, lhsCols, lhsIt, rhsCols, rhsIt)
	if err != nil {
		return nil, err
	}
	return materializeIterator(cols, out)
}

// executeUnionParallel preserves direct test-call compatibility while using
// the iterator-based UNION execution path.
func (e *FabricExecutor) executeUnionParallel(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		lhs []string
		lit RowIterator
		rhs []string
		rit RowIterator
		err error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cols, it, runErr := e.executeRows(ctx, tx, f.LHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			err = fmt.Errorf("union LHS failed: %w", runErr)
			return
		}
		lhs, lit = cols, it
	}()
	go func() {
		defer wg.Done()
		cols, it, runErr := e.executeRows(ctx, tx, f.RHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			if err == nil {
				err = fmt.Errorf("union RHS failed: %w", runErr)
			}
			return
		}
		rhs, rit = cols, it
	}()
	wg.Wait()
	if err != nil {
		if lit != nil {
			_ = lit.Close()
		}
		if rit != nil {
			_ = rit.Close()
		}
		return nil, err
	}
	cols, out, err := e.mergeUnion(f, lhs, lit, rhs, rit)
	if err != nil {
		return nil, err
	}
	return materializeIterator(cols, out)
}

func fragmentContainsWrite(fragment Fragment) bool {
	switch f := fragment.(type) {
	case *FragmentExec:
		return f.IsWrite
	case *FragmentApply:
		return fragmentContainsWrite(f.Input) || fragmentContainsWrite(f.Inner)
	case *FragmentUnion:
		return fragmentContainsWrite(f.LHS) || fragmentContainsWrite(f.RHS)
	case *FragmentLeaf:
		return fragmentContainsWrite(f.Input)
	case *FragmentInit:
		return false
	default:
		return false
	}
}

// combineColumns merges two column lists, deduplicating names. Every column
// is a query's own, whatever its name.
