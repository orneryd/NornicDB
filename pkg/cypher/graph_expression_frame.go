package cypher

import (
	"context"
	"strings"
)

// graphExpressionFrame is the runtime value side of a compiled body scope.
// Static import/local selection belongs to semanticScopeWalk. Presence is
// retained in the map independently of value, including untyped/typed null.
// The same resolved values feed every physical strategy and the body pipeline.
type graphExpressionFrame struct {
	values pipelineRow
}

// graphScopeValues is one body's persistent import set. An explicitly empty
// set masks an enclosing body's imports at a real CALL scope boundary.
type graphScopeValues struct{ imports pipelineRow }
type graphScopeValuesKey struct{}

func graphScopeImports(ctx context.Context, initial pipelineRow) pipelineRow {
	if scope, present := ctx.Value(graphScopeValuesKey{}).(graphScopeValues); present {
		return scope.imports
	}
	return initial
}

func withGraphScopeImports(ctx context.Context, imports pipelineRow) context.Context {
	return context.WithValue(ctx, graphScopeValuesKey{}, graphScopeValues{imports: imports})
}

func (e *StorageExecutor) resolveGraphExpressionFrame(ctx context.Context, scope compiledRowScope) graphExpressionFrame {
	values := scope.materialize()
	owned := scope.nodes != nil || scope.rels != nil || scope.parameters != nil
	bindMissing := func(name string, value interface{}) {
		if _, present := values[name]; present {
			return
		}
		if !owned {
			row := make(pipelineRow, len(values)+4)
			for key, current := range values { row[key] = current }
			values, owned = row, true
		}
		values[name] = value
	}
	// Context values supply iterator/scalar adapters; current row values win.
	for name, value := range valueBindingsFromContext(ctx) { bindMissing(name, value) }
	for name, value := range graphScopeImports(ctx, e.fabricRecordBindings) { bindMissing(name, value) }
	for name, value := range parameterRowValues(ctx) { bindMissing(name, value) }
	return graphExpressionFrame{values: values}
}

func (frame graphExpressionFrame) lookup(name string) (interface{}, bool) {
	value, present := frame.values[name]
	return value, present
}

func (frame graphExpressionFrame) imports() pipelineRow {
	imports := make(pipelineRow, len(frame.values))
	for name, value := range frame.values {
		if !strings.HasPrefix(name, "\x00") && !strings.HasPrefix(name, "$") {
			imports[name] = value
		}
	}
	return imports
}

func (frame graphExpressionFrame) expressionValues(ctx context.Context) pipelineRow {
	if current, _ := frame.values[rowStatementContextKey].(context.Context); current == ctx {
		return frame.values
	}
	return rowValuesInExpressionContext(ctx, frame.values)
}

// enter installs exactly this body's imports. WITH's existing carry operation
// consumes them; scoped/legacy CALL replaces them through withPipelineImports.
func (frame graphExpressionFrame) enter(ctx context.Context) context.Context {
	return withGraphScopeImports(ctx, frame.imports())
}

// resolveFrame consumes the static owner's answer. Values are resolved before
// choosing a matcher, so a name retained across WITH has identical presence and
// value in the compiled hop and in the full correlated body.
func (plan *graphExpressionPlan) resolveFrame(e *StorageExecutor, ctx context.Context, available graphExpressionFrame) (graphExpressionFrame, error) {
	visible := make([]string, 0, len(available.values))
	for name := range available.values {
		if !strings.HasPrefix(name, "\x00") && !strings.HasPrefix(name, "$") { visible = append(visible, name) }
	}
	metadata, err := e.bodyScopeOf(plan.scopeBody(), visible, cypherVersionFromContext(ctx) == "25")
	if err != nil { return graphExpressionFrame{}, err }
	values := make(pipelineRow, len(metadata.Reads)+2)
	for _, name := range metadata.Reads {
		value, present := available.lookup(name)
		if !present { return graphExpressionFrame{}, createUndefinedVariableError(name) }
		values[name] = value
	}
	for name, value := range available.values {
		if strings.HasPrefix(name, "\x00") || strings.HasPrefix(name, "$") { values[name] = value }
	}
	return graphExpressionFrame{values: values}, nil
}
