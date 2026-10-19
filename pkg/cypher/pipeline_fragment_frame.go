package cypher

import (
	"context"
	"slices"
	"strings"
)

// pipelineProjectFragmentResult uses the native projection/frame owner to carry
// CALL globals alongside a physical result. The globals never become DISTINCT
// or aggregate keys. The final public RETURN still projects only its own items.
func (e *StorageExecutor) pipelineProjectFragmentResult(ctx context.Context, rows []pipelineRow, source pipelineRowSource, scope map[string]struct{}, clause, original string, rowsValidated bool) (*ExecuteResult, bool) {
	plan := returnProjectionPlanFor(clause)
	if plan.star {
		plan = plan.withStarExpanded(pipelineScopeColumns(scope))
	}
	items := make([]string, len(plan.projections))
	for i, item := range plan.projections {
		items[i] = item.expr + " AS " + projectionVariableText(item.alias)
	}
	text := "WITH "
	if plan.distinct {
		text += "DISTINCT "
	}
	text += strings.Join(items, ", ") + " " + plan.modifiers
	if !plan.hasAggregate {
		var complete bool
		rows, complete = materializePipelineSource(source)
		if !complete {
			return nil, false
		}
		source = pipelineRowsSource(rows)
	}
	projected, ok := e.pipelineApplyWithSource(ctx, rows, text, source, rowsValidated)
	if !ok {
		return nil, false
	}
	columns := slices.Clone(plan.columns)
	imports := pipelineImports(ctx)
	importScope := make(map[string]struct{}, len(imports))
	for name := range imports {
		importScope[name] = struct{}{}
	}
	for _, name := range pipelineScopeColumns(importScope) {
		if !slices.Contains(columns, name) {
			columns = append(columns, name)
		}
	}
	result := callPipelineResultFromRows(projected, nil, nil, columns)
	if original != "" {
		if names := pipelineReturnSourceColumns(original); len(names) == len(plan.columns) {
			copy(result.Columns, names)
		}
	}
	return result, true
}

// A frame with retained globals enters the native pipeline explicitly. A
// specialized statement path cannot silently discard that frame contract.
func (e *StorageExecutor) executeFragmentStatement(ctx context.Context, query string, params map[string]interface{}) (*ExecuteResult, error) {
	if len(pipelineImports(ctx)) > 0 {
		return e.executeRequiredPipeline(withQueryParams(ctx, params), query)
	}
	return e.executeInternal(ctx, query, params)
}
