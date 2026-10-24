package fabric

import "fmt"

// All UNION routes reconcile the declared/runtime schema before concatenation
// or DISTINCT. Conversion is lazy, preserving the row-stream close contract.
func mergeUnionRows(contract UnionContract, f *FragmentUnion, lhs []string, left RowIterator, rhs []string, right RowIterator) ([]string, RowIterator, error) {
	columns := f.Columns
	if len(columns) == 0 {
		columns = lhs
		if len(columns) == 0 {
			columns = rhs
		}
	}
	alignBranches := !isFabricRowWrapper(lhs) && !isFabricRowWrapper(rhs)
	align := func(branch []string, rows RowIterator) (RowIterator, error) {
		// A nil result has neither rows nor schema; a parsed RETURN is validated by
		// the planner even when the execution participant produces no row metadata.
		if branch == nil || !alignBranches {
			return rows, nil
		}
		order, same := UnionColumnOrder(columns, branch)
		if contract.ColumnOrder != nil {
			order, same = contract.ColumnOrder(columns, branch)
		}
		if !same {
			err := fmt.Errorf("UNION queries must return the same columns (got %v and %v)", columns, branch)
			if contract.ColumnsError != nil {
				err = contract.ColumnsError(columns, branch)
			}
			return nil, err
		}
		if order == nil {
			return rows, nil
		}
		return NewConvertingRowIterator(rows, func(row []interface{}) []interface{} {
			values := make([]interface{}, len(order))
			for i, index := range order {
				if index < len(row) {
					values[i] = row[index]
				}
			}
			return values
		}), nil
	}
	l, err := align(lhs, left)
	var r RowIterator
	if err == nil {
		r, err = align(rhs, right)
	}
	if err != nil {
		if left != nil {
			_ = left.Close()
		}
		if right != nil {
			_ = right.Close()
		}
		return nil, nil, err
	}
	out := NewConcatRowIterator(l, r)
	if f.Distinct {
		distinct := NewDistinctRowIterator(out)
		if contract.NewDistinct != nil {
			distinct.(*distinctRowIterator).added = contract.NewDistinct()
		}
		out = distinct
	}
	return columns, out, nil
}
