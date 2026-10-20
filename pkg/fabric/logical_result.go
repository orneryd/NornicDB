package fabric

import (
	"context"
	"slices"
)

// executePhysicalQuery runs a generated operator under the original admitted
// descriptor. The operator owns its temporary columns and consumes them before
// merging its output into the logical stream; the descriptor remains immutable.
func (e *FabricExecutor) executePhysicalQuery(ctx context.Context, tx *FabricTransaction, logical *FragmentExec, query string, params map[string]interface{}, authToken string) (*ResultStream, error) {
	physical := *logical
	physical.Query = query
	result, _, err := e.dispatchPhysicalExec(ctx, tx, &physical, params, authToken)
	return result, err
}

// projectLogicalResult is the logical export boundary after physical operators.
// Retained invocation bindings are transport state, not additional CALL exports.
// Unit bodies have no RETURN projection and keep the CALL cardinality contract.
func projectLogicalResult(frame *LogicalFrame, result *ResultStream) *ResultStream {
	if frame == nil || !frame.ReturnProjection || result == nil || slices.Equal(frame.Columns, result.Columns) {
		return result
	}
	indexes := buildColumnIndex(result.Columns)
	projected := &ResultStream{Columns: slices.Clone(frame.Columns), Rows: make([][]interface{}, len(result.Rows))}
	for rowIndex, row := range result.Rows {
		values := make([]interface{}, len(frame.Columns))
		for columnIndex, name := range frame.Columns {
			if source, exists := indexes[name]; exists && source < len(row) {
				values[columnIndex] = row[source]
			}
		}
		projected.Rows[rowIndex] = values
	}
	return projected
}
