package fabric

// All UNION routes reconcile the declared/runtime schema before concatenation
// or DISTINCT. Conversion is lazy, preserving the row-stream close contract.
func (e *FabricExecutor) mergeUnionRows(f *FragmentUnion, lhs []string, left RowIterator, rhs []string, right RowIterator) ([]string, RowIterator, error) {
	// Missing participant metadata uses the branch's already compiled result
	// columns. This reconciliation runs only for UNION, including empty rows.
	if lhs == nil && f.LHS != nil {
		lhs = f.LHS.OutputColumns()
	}
	if rhs == nil && f.RHS != nil {
		rhs = f.RHS.OutputColumns()
	}
	columns := f.Columns
	if len(columns) == 0 {
		columns = lhs
		if len(columns) == 0 {
			columns = rhs
		}
	}
	align := func(branch []string, rows RowIterator) (RowIterator, error) {
		// A nil result has neither rows nor schema; a parsed RETURN is validated by
		// the planner even when the execution participant produces no row metadata.
		if branch == nil {
			return rows, nil
		}
		order, same := UnionColumnOrder(columns, branch)
		if !same {
			return nil, &UnionColumnsError{Message: UnionColumnMismatchMessage(columns, branch)}
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
		out = NewDistinctRowIterator(out, e.rowEquivalence())
	}
	return columns, out, nil
}
